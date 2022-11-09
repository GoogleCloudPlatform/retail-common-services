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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.spez.spanner.Row;
import io.opencensus.trace.Span;
import java.text.NumberFormat;
import java.util.concurrent.ForkJoinPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class RowProcessor {
  private static final Logger log = LoggerFactory.getLogger(RowProcessor.class);

  private ListeningExecutorService buildThreadPool(boolean useDirectExecutor) {
    ListeningExecutorService listeningPool;
    if (useDirectExecutor) {
      listeningPool = MoreExecutors.newDirectExecutorService();
    } else {
      listeningPool = MoreExecutors.listeningDecorator(threadPool);
    }
    return listeningPool;
  }

  private final ListeningExecutorService listeningPool;
  private final ForkJoinPool threadPool = new ForkJoinPool();
  private final RowProcessorStats stats = new RowProcessorStats();

  private final SpezConfig.SinkConfig sinkConfig;

  private final EventPublisher publisher;
  private final MetadataExtractor extractor;

  /**
   * Constructor.
   *
   * @param sinkConfig description
   * @param publisher description
   * @param extractor description
   */
  public RowProcessor(
      SpezConfig.SinkConfig sinkConfig, EventPublisher publisher, MetadataExtractor extractor) {
    this.listeningPool = buildThreadPool(false);
    this.sinkConfig = sinkConfig;
    this.publisher = publisher;
    this.extractor = extractor;
    log.info("availableProcessors(): {}", Runtime.getRuntime().availableProcessors());
  }

  /**
   * Convert row to avro record and publish to pubsub topic. ONLY VISIBLE FOR TESTING!
   *
   * @param eventState to publish
   * @return timestamp of the row published
   */
  @VisibleForTesting
  public ListenableFuture<String> convertAndPublishTask(EventState eventState) {
    Row row = eventState.getRow();

    var tableName = sinkConfig.getTable();
    var avroNamespace = "avroNamespace"; // TODO(pdex): move to config
    var schema = SpannerToAvroSchema.buildSchema(tableName, avroNamespace, row);
    var maybeAvroRecord = SpannerToAvroRecord.makeRecord(schema, row);
    if (maybeAvroRecord.isEmpty()) {
      // TODO(pdex): this should be an error set on the future inside of the eventState
      throw new RuntimeException("Empty avro record");
    }
    var avroRecord = maybeAvroRecord.get();
    eventState.convertedToMessage(avroRecord);

    var metadata = extractor.extract(row);
    var publishFuture = publisher.publish(avroRecord, metadata, eventState);
    stats.incRecords();
    Futures.addCallback(
        publishFuture,
        new FutureCallback<String>() {

          @Override
          public void onSuccess(String publishId) {
            // Once published, returns server-assigned message ids
            // (unique within the topic)
            eventState.messagePublished(publishId);
            stats.incPublished();
            if (log.isDebugEnabled()) {
              log.debug(
                  "Published message for timestamp '{}' with message id '{}'",
                  metadata.get(SpezConfig.SINK_TIMESTAMP_KEY),
                  publishId);
            }
          }

          @Override
          public void onFailure(Throwable ex) {
            stats.incErrors();
            eventState.error(ex);
          }
        },
        listeningPool);
    return publishFuture;
  }

  private void processRow(Row row, Span pollingSpan) {
    EventState eventState = null;
    try {
      eventState =
          new EventState(
              pollingSpan,
              StatsCollector.newForTable(sinkConfig.getTable()).attachSpan(pollingSpan));
      eventState.rowRead(row);
      // TODO(pdex): remove this call and fix the state machine.
      eventState.queued();
      // TODO(pdex): address this unchecked future
      convertAndPublishTask(eventState);
    } catch (Exception ex) {
      log.error("processRow", ex);
      stats.incErrors();
      if (eventState != null) {
        eventState.error(ex);
      }
    }
  }

  private final NumberFormat formatter = NumberFormat.getInstance();

  /**
   * Convert row to avro record and publish to pubsub topic.
   *
   * @param row to publish
   * @param pollingSpan covers the duration of this polling iteration
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void queueRow(Row row, Span pollingSpan) {
    var nanosThen = System.nanoTime();
    var forkJoinStats = threadPool.toString();
    threadPool.submit(() -> processRow(row, pollingSpan)); // ignore future value
    if (log.isTraceEnabled()) {
      var nanosNow = System.nanoTime();
      long duration = nanosNow - nanosThen;
      if (duration > 10_000_000) {
        log.trace(
            "row took {} ns to return from threadPool.submit() stats: {}",
            formatter.format(duration),
            forkJoinStats);
      }
    }
  }

  /** log stats. */
  // TODO(pdex): move this out to SpezApp
  public void logStats() {
    stats.logStats();
  }
}
