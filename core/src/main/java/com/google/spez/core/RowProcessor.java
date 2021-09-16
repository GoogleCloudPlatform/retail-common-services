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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.spez.core.internal.Row;
import io.opencensus.common.Scope;
import io.opencensus.trace.Span;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO(pdex): rename to RowProcessor
@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class RowProcessor {
  private static final Logger log = LoggerFactory.getLogger(RowProcessor.class);

  private final SpezMetrics metrics = new SpezMetrics();
  private final SpezTagging tagging = new SpezTagging();
  private final SpezTracing tracing = new SpezTracing();

  private final ExecutorService workStealingPool = Executors.newWorkStealingPool();
  private final ListeningExecutorService forkJoinPool =
      MoreExecutors.listeningDecorator(workStealingPool);
  // private final AtomicReference<String> lastProcessedTimestamp = new AtomicReference<String>("");
  private final RowProcessorStats stats = new RowProcessorStats();

  private final SpezConfig.SinkConfig sinkConfig;

  private final EventPublisher publisher;
  private final Reconciler reconciler;
  private final MetadataExtractor extractor;

  /**
   * Constructor.
   *
   * @param schemaSet description
   * @param publisher description
   * @param extractor description
   */
  public RowProcessor(
      SpezConfig.SinkConfig sinkConfig, EventPublisher publisher, Reconciler reconciler, MetadataExtractor extractor) {
    this.sinkConfig = sinkConfig;
    this.publisher = publisher;
    this.reconciler = reconciler;
    this.extractor = extractor;
  }

  public String convertAndPublishTask(Row row) {
    try (Scope scopedTags = tagging.tagFor(sinkConfig.getTable())) {
      try (Scope ss = tracing.processRowScope()) {
        stats.incRecords();

        Span span = tracing.currentSpan();

        var metadata = extractor.extract(row);

        var tableName = sinkConfig.getTable();
        var avroNamespace = "avroNamespace"; // TODO(pdex): move to config
        var schema = SpannerToAvroSchema.buildSchema(tableName, avroNamespace, row);
        var avroRecord = SpannerToAvroRecord.makeRecord(schema, row);

        var publishFuture = publisher.publish(avroRecord.get(), metadata, span);
        try {
          // Once published, returns server-assigned message ids
          // (unique within the topic)
          var messageId = publishFuture.get();
          stats.incPublished();
          if (reconciler != null) {
            var commitTimestamp = metadata.get(SpezConfig.SINK_TIMESTAMP_KEY);
            var uuid = metadata.get(SpezConfig.SINK_UUID_KEY);
            reconciler.onPublish(messageId, commitTimestamp, uuid);
          }
          if (log.isDebugEnabled()) {
            log.debug(
                "Published message for timestamp '{}' with message id '{}'",
                metadata.get(SpezConfig.SINK_TIMESTAMP_KEY),
                messageId);
          }

          metrics.addMessageSize(row.getSize());
          return messageId;
        } catch (Exception ex) {
          stats.incErrors();
          log.error("Error Publishing Record: ", ex);
          throw new RuntimeException(ex);
        }
      }
    }
  }

  /**
   * Convert row to avro record and publish to pubsub topic.
   *
   * @param row to publish
   * @return timestamp of the row published
   */
  public ListenableFuture<String> convertAndPublish(Row row) {
    ListenableFuture<String> future = forkJoinPool.submit(() -> convertAndPublishTask(row));

    return future;
  }

  /** log stats. */
  // TODO(pdex): move this out to SpezApp
  public void logStats() {
    stats.logStats();
  }
}
