/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.spez.core;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.Timestamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.spannerclient.*;
import com.google.spez.common.UsefulExecutors;
import com.google.spez.core.internal.BothanRow;
import io.grpc.stub.StreamObserver;
import io.opencensus.common.Scope;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates and schedules the spanner event tailer
 *
 * <p>The {@code SpannerTailer} class creates your tailer and schedules it to tail the configured
 * spanner table based on your configuration options. For each row the tailer receives, it will
 * allow you to create a corresponding avro record and publish that record to the configured pub /
 * sub topic. This class assumes you have your Google Cloud credentials set up as described in the
 * {@code README.md} for this git repo.
 *
 * <p>As the tailer tails on a fixed interval, there is no mechanism for retry as the poller will
 * just process the records on its next poll attempt based on the last processed timestamp.
 *
 * <p>For the first run, the poller will try to configure the lastTimestamp based on the last record
 * published to the configured pub / sub topic and updated to the metadata Cloud Spanner table via
 * the lastProcessedTimestamp Cloud Function (if deployed).
 */
public class SpannerTailer {
  private static final Logger log = LoggerFactory.getLogger(SpannerTailer.class);
  public static final int THREAD_POOL = 12; // TODO(pdex): move to config

  private final SpezMetrics metrics = new SpezMetrics();
  private final SpezTagging tagging = new SpezTagging();
  private final SpezTracing tracing = new SpezTracing();
  private final AtomicLong running = new AtomicLong(0);
  private final SpezConfig config;
  private final SpezConfig.SinkConfig sinkConfig;
  private final SpezConfig.LptsConfig lptsConfig;
  private final Database database;
  private final WorkStealingHandler handler;
  private final ListeningExecutorService service;
  private final ListeningScheduledExecutorService scheduler;
  private final HashFunction hasher;
  private String lastProcessedTimestamp;

  public SpannerTailer(
      SpezConfig config,
      Database database,
      WorkStealingHandler handler,
      String lastProcessedTimestamp) {
    this.config = config;
    this.sinkConfig = config.getSink();
    this.lptsConfig = config.getLpts();
    this.database = database;
    this.handler = handler;
    this.lastProcessedTimestamp = lastProcessedTimestamp;
    this.scheduler = UsefulExecutors.listeningScheduler();
    this.service =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                THREAD_POOL,
                new ThreadFactoryBuilder().setNameFormat("SpannerTailer Acceptor").build()));
    this.hasher = Hashing.murmur3_128();
  }

  /**
   * Starts the tailing process by scheduling a query at a configurable fixed delay with a read only
   * query. To maintain state to allow only new events to be processed, a {@code Timestamp} field is
   * required in the table and will be maintained here. As of now, that field is hardcoded as
   * `Timestamp`.
   *
   * @param tsColName the name of the column holding the true time commit timestamp for processing
   * @param bucketSize Size of the consistent hashing algorythm for thread poll managment
   * @param threadCount xx
   * @param pollRate Time delay in Milliseconds between polls
   * @param projectId GCP Project ID
   * @param instanceName Cloud Spanner Instance Name
   * @param dbName Cloud Spanner Database Name
   * @param tableName Cloud Spanner table Name
   * @param lptsTableName Cloud Spanner table name for lastProcessedTimestamp (as populated by the
   *     lastProcessedTimestamp Cloud Function
   * @param recordLimit The limit for the max number of records returned for a given query
   * @param vacuumRate xxx
   * @param eventCacheTTL xxx
   */
  public void start() {
    var schedulerFuture =
        scheduler.scheduleAtFixedRate(
            this::poll,
            0,
            30, // TODO(pdex): move to config // TODO(pdex): I should be a runtime option
            TimeUnit.SECONDS);

    Futures.addCallback(
        schedulerFuture,
        new FutureCallback() {
          @Override
          public void onFailure(Throwable t) {
            log.error("SpannerTailer::poll scheduled task error", t);
          }

          @Override
          public void onSuccess(Object result) {}
        },
        scheduler);
  }

  @VisibleForTesting
  static String buildLptsTableQuery(
      String tableName, String timestampColumn, String lastProcessedTimestamp) {
    return new StringBuilder()
        .append("SELECT * FROM ")
        .append(tableName)
        .append(" ")
        .append("WHERE ")
        .append(timestampColumn)
        .append(" > '")
        .append(lastProcessedTimestamp)
        .append("'")
        .append(" ORDER BY ")
        .append(timestampColumn)
        .append(" ASC")
        .toString();
  }

  private void poll() {
    long num = running.incrementAndGet();
    if (num > 1) {
      log.debug("Already {} polling processes in flight", num - 1);
      running.decrementAndGet();
      return;
    }
    log.debug("POLLER ACTIVE");
    try {
      log.info("Polling for records newer than {}", lastProcessedTimestamp);
      Instant then = Instant.now();
      AtomicLong records = new AtomicLong(0);
      Spanner.executeStreaming(
          QueryOptions.newBuilder()
              .setReadOnly(true)
              .setStale(true)
              .setMaxStaleness(500)
              .build(), // TODO(pdex): move to sinkConfig
          database,
          new RowStreamObserver(records, then),
          Query.create(
              buildLptsTableQuery(
                  sinkConfig.getTable(), sinkConfig.getTimestampColumn(), lastProcessedTimestamp)));
    } catch (Exception e) {
      log.error("Caught error while polling", e);
      running.decrementAndGet();
      throw e;
    }
  }

  public void close() {
    try {
      if (database != null) {
        database.close();
      }
    } catch (IOException e) {
      log.error("Error Closing Managed Channel", e);
    }
  }

  private Boolean processRow(com.google.spannerclient.Row row, SpezConfig.SinkConfig config) {
    final String uuid = Long.toString(row.getLong(config.getUuidColumn()));
    final Timestamp ts = row.getTimestamp(config.getTimestampColumn());
    final HashCode sortingKeyHashCode = hasher.newHasher().putBytes(uuid.getBytes(UTF_8)).hash();
    final int bucket = Hashing.consistentHash(sortingKeyHashCode, 12);

    //    try (Scope ss = tracer.spanBuilder("SpannerTailer.processRow").startScopedSpan()) {
    try (Scope ss = tracing.processRowScope()) {
      Span span = tracing.currentSpan();
      ListenableFuture<Boolean> result =
          handler.process(bucket, new BothanRow(row), ts.toString(), span);
      lastProcessedTimestamp = ts.toString();
      Futures.addCallback(
          result,
          new FutureCallback<Boolean>() {

            @Override
            public void onSuccess(Boolean result) {
              ss.close();
            }

            @Override
            public void onFailure(Throwable t) {
              span.setStatus(Status.INTERNAL.withDescription(t.toString()));
              ss.close();
            }
          },
          MoreExecutors.directExecutor());

      return true;
    }
  }

  public void logStats() {
    metrics.logStats();
  }

  private void convertAndPublish(com.google.spannerclient.Row row) {
    try (Scope scopedTags = tagging.tagFor(sinkConfig.getTable())) {
      //    try (Scope ss = tracer.spanBuilder("SpannerTailer.processRow").startScopedSpan()) {
      try (Scope ss = tracing.processRowScope()) {
        Span span = tracing.currentSpan();

        final String uuid = Long.toString(row.getLong(sinkConfig.getUuidColumn()));
        final Timestamp timestamp = row.getTimestamp(sinkConfig.getTimestampColumn());

        ListenableFuture<Boolean> result = handler.process(0, new BothanRow(row), "", span);
        lastProcessedTimestamp = timestamp.toString();
        Futures.addCallback(
            result,
            new FutureCallback<Boolean>() {

              @Override
              public void onSuccess(Boolean result) {
                ss.close();
              }

              @Override
              public void onFailure(Throwable t) {
                span.setStatus(Status.INTERNAL.withDescription(t.toString()));
                ss.close();
              }
            },
            MoreExecutors.directExecutor());
      }
      // processRow(row, sinkConfig);
      lastProcessedTimestamp = row.getTimestamp(sinkConfig.getTimestampColumn()).toString();
      metrics.addMessageSize(row.getSize());
    }
  }

  private class RowStreamObserver implements StreamObserver<Row> {
    private final AtomicLong records;
    private final Instant then;

    public RowStreamObserver(AtomicLong records, Instant then) {
      this.records = records;
      this.then = then;
    }

    @Override
    public void onNext(Row row) {
      long count = records.incrementAndGet();
      log.debug("onNext count = {}", count);
      convertAndPublish(row);
    }

    @Override
    public void onError(Throwable t) {
      log.error("StreamObserver", t);
      running.decrementAndGet();
    }

    @Override
    public void onCompleted() {
      Instant now = Instant.now();
      Duration duration = Duration.between(then, now);
      log.debug("SpannerTailer completed!");
      log.warn(
          "Processed {} records in {} seconds for last processed timestamp {}",
          records.get(),
          duration.toNanos() / 1000000000.0,
          lastProcessedTimestamp);
      running.decrementAndGet();
    }
  }
}
