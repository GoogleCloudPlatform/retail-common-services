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
import com.google.spez.core.internal.Row;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class RowProcessor {
  private static final Logger log = LoggerFactory.getLogger(RowProcessor.class);

  private final SpezMetrics metrics = new SpezMetrics();
  private final SpezTagging tagging = new SpezTagging();
  private final SpezTracing tracing = new SpezTracing();

  private ListeningExecutorService buildThreadPool(boolean useDirectExecutor) {
    ListeningExecutorService listeningPool;
    if (useDirectExecutor) {
      listeningPool = MoreExecutors.newDirectExecutorService();
    } else {
      ExecutorService threadPool =
          Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
      listeningPool = MoreExecutors.listeningDecorator(threadPool);
    }
    return listeningPool;
  }

  private final ListeningExecutorService listeningPool;
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
    this.listeningPool = buildThreadPool(true);
    this.sinkConfig = sinkConfig;
    this.publisher = publisher;
    this.extractor = extractor;
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
    stats.incRecords();

    var tableName = sinkConfig.getTable();
    var avroNamespace = "avroNamespace"; // TODO(pdex): move to config
    var schema = SpannerToAvroSchema.buildSchema(tableName, avroNamespace, row);
    var maybeAvroRecord = SpannerToAvroRecord.makeRecord(schema, row);
    if (maybeAvroRecord.isEmpty()) {
      throw new RuntimeException("Empty avro record");
    }
    var avroRecord = maybeAvroRecord.get();
    eventState.convertedToMessage(avroRecord);

    var metadata = extractor.extract(row);
    var publishFuture = publisher.publish(avroRecord, metadata, eventState);
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

            metrics.addMessageSize(row.getSize(), tableName);
          }

          @Override
          public void onFailure(Throwable ex) {
            stats.incErrors();
            log.error("Error Publishing Record: ", ex);
          }
        },
        listeningPool);
    return publishFuture;
  }

  /**
   * Convert row to avro record and publish to pubsub topic.
   *
   * @param eventState to publish
   * @return timestamp of the row published
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public ListenableFuture<String> convertAndPublish(EventState eventState) {
    eventState.queued();
    ListenableFuture<ListenableFuture<String>> future =
        listeningPool.submit(() -> convertAndPublishTask(eventState));

    return Futures.transformAsync(future, (ListenableFuture<String> f) -> f, listeningPool);
  }

  /** log stats. */
  // TODO(pdex): move this out to SpezApp
  public void logStats() {
    stats.logStats();
  }
}
