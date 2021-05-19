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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.spannerclient.Query;
import com.google.spannerclient.QueryOptions;
import com.google.spez.common.ListenableFutureErrorHandler;
import com.google.spez.common.UsefulExecutors;
import com.google.spez.core.internal.Database;
import com.google.spez.core.internal.Row;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
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
@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class SpannerTailer {
  private static final Logger log = LoggerFactory.getLogger(SpannerTailer.class);

  private final SpezMetrics metrics = new SpezMetrics();
  private final AtomicLong running = new AtomicLong(0);
  private final SpezConfig.SinkConfig sinkConfig;
  private final Database database;
  private final RowProcessor handler;
  private final ListeningScheduledExecutorService scheduler;
  private String lastProcessedTimestamp;

  /**
   * SpannerTailer constructor.
   *
   * @param config config object
   * @param database database object
   * @param handler row handler
   * @param lastProcessedTimestamp initial last processed timestamp
   */
  public SpannerTailer(
      SpezConfig config, Database database, RowProcessor handler, String lastProcessedTimestamp) {
    this.sinkConfig = config.getSink();
    this.database = database;
    this.handler = handler;
    this.lastProcessedTimestamp = lastProcessedTimestamp;
    this.scheduler = UsefulExecutors.listeningScheduler();
  }

  /**
   * Starts the tailing process by scheduling a query at a configurable fixed delay with a read only
   * query. To maintain state to allow only new events to be processed, a {@code Timestamp} field is
   * required in the table and will be maintained here. As of now, that field is hardcoded as
   * `Timestamp`.
   */
  // TODO(pdex): move this out to SpezApp
  public void start() {
    var schedulerFuture =
        scheduler.scheduleAtFixedRate(this::poll, 0, sinkConfig.getPollRate(), TimeUnit.SECONDS);

    ListenableFutureErrorHandler.create(
        scheduler,
        schedulerFuture,
        (throwable) -> {
          log.error("SpannerTailer::poll scheduled task error", throwable);
        });
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
    if (num > 1) { // NOPMD
      log.debug("Already {} polling processes in flight", num - 1);
      running.decrementAndGet();
      return;
    }
    log.debug("POLLER ACTIVE");
    try {
      log.info("Polling for records newer than {}", lastProcessedTimestamp);
      Instant then = Instant.now();
      AtomicLong records = new AtomicLong(0);
      var options =
          QueryOptions.newBuilder()
              .setReadOnly(true)
              .setStale(true)
              .setMaxStaleness(500)
              .build(); // TODO(pdex): move to sinkConfig
      var observer = new RowStreamObserver(records, then);
      var query =
          Query.create(
              buildLptsTableQuery(
                  sinkConfig.getTable(), sinkConfig.getTimestampColumn(), lastProcessedTimestamp));

      database.executeStreaming(options, observer, query);
    } catch (Exception e) {
      log.error("Caught error while polling", e);
      running.decrementAndGet();
      throw e;
    }
  }

  /** closes the underlying resources. */
  public void close() {
    try {
      if (database != null) {
        database.close();
      }
    } catch (IOException e) {
      log.error("Error Closing Managed Channel", e);
    }
  }

  // TODO(pdex): move this out to SpezApp
  public void logStats() {
    metrics.logStats();
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
      handler.convertAndPublish(row);
      lastProcessedTimestamp = row.getTimestamp(sinkConfig.getTimestampColumn()).toString();
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
