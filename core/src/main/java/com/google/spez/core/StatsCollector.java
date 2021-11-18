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

import io.opencensus.contrib.exemplar.util.ExemplarUtils;
import io.opencensus.metrics.data.AttachmentValue;
import io.opencensus.stats.MeasureMap;
import io.opencensus.stats.Stats;
import io.opencensus.trace.Span;

public class StatsCollector {
  private final String tableName;
  private final MeasureMap measureMap;

  private StatsCollector(String tableName) {
    this.tableName = tableName;
    measureMap = Stats.getStatsRecorder().newMeasureMap();
  }

  public static StatsCollector newForTable(String tableName) {
    return new StatsCollector(tableName);
  }

  public StatsCollector attachPublishId(String publishId) {
    measureMap.putAttachment("publishId", AttachmentValue.AttachmentValueString.create(publishId));
    return this;
  }

  public StatsCollector attachSpan(Span span) {
    // TODO(pdex): maybe turn this back on if it's not causing "duplicate attachment" errors
    if (false) {
      ExemplarUtils.putSpanContextAttachments(measureMap, span.getContext());
    }
    return this;
  }

  public StatsCollector attachUuid(String uuid) {
    measureMap.putAttachment("uuid", AttachmentValue.AttachmentValueString.create(uuid));
    return this;
  }

  public StatsCollector incrementPublished() {
    measureMap.put(SpezMetrics.MessagePublished.MEASURE, 1);
    return this;
  }

  public StatsCollector incrementReceived() {
    measureMap.put(SpezMetrics.MessageReceived.MEASURE, 1);
    return this;
  }

  public StatsCollector incrementTablePolled() {
    measureMap.put(SpezMetrics.TablePolled.MEASURE, 1);
    return this;
  }

  public StatsCollector addBufferSizeWhenQueued(long bufferSize) {
    measureMap.put(SpezMetrics.BufferSizeWhenQueued.MEASURE, bufferSize);
    return this;
  }

  public StatsCollector addConvertedToMessageDuration(long nanos) {
    measureMap.put(SpezMetrics.ConvertedToMessageDuration.MEASURE, nanos);
    return this;
  }

  public StatsCollector addMessagePublishedDuration(long nanos) {
    measureMap.put(SpezMetrics.MessagePublishedDuration.MEASURE, nanos);
    return this;
  }

  public StatsCollector addMessagePublishRequestedDuration(long nanos) {
    measureMap.put(SpezMetrics.MessagePublishRequestedDuration.MEASURE, nanos);
    return this;
  }

  public StatsCollector addMessageSize(long messageSize) {
    measureMap.put(SpezMetrics.MessageSize.MEASURE, messageSize);
    return this;
  }

  public StatsCollector addQueuedForConversionDuration(long nanos) {
    measureMap.put(SpezMetrics.QueuedForConversionDuration.MEASURE, nanos);
    return this;
  }

  public StatsCollector addQueuedForPublishingDuration(long nanos) {
    measureMap.put(SpezMetrics.QueuedForPublishingDuration.MEASURE, nanos);
    return this;
  }

  public StatsCollector addRowReadDuration(long nanos) {
    measureMap.put(SpezMetrics.RowReadDuration.MEASURE, nanos);
    return this;
  }

  public StatsCollector addRowSize(long rowSize) {
    measureMap.put(SpezMetrics.RowSize.MEASURE, rowSize);
    return this;
  }

  public void collect() {
    measureMap.record(SpezTagging.tagForTable(tableName));
  }
}
