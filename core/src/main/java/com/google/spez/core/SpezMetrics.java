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

import io.opencensus.stats.Aggregation;
import io.opencensus.stats.BucketBoundaries;
import io.opencensus.stats.Measure;
import io.opencensus.stats.Stats;
import io.opencensus.stats.View;
import io.opencensus.stats.ViewManager;
import io.opencensus.tags.TagKey;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpezMetrics {
  private static final Logger log = LoggerFactory.getLogger(SpezMetrics.class);
  private static final ViewManager viewManager = Stats.getViewManager();

  private static final long KiB = 1 << 10;
  private static final long MiB = 1 << 20;

  private static final List<View> VIEWS =
      List.of(
          BufferSizeWhenQueued.VIEW,
          ConvertedToMessageDuration.VIEW,
          MessagePublishedDuration.VIEW,
          MessagePublishRequestedDuration.VIEW,
          MessageSize.VIEW,
          QueuedForConversionDuration.VIEW,
          QueuedForPublishingDuration.VIEW,
          RowReadDuration.VIEW,
          RowSize.VIEW);

  public static void setupViews() {
    for (var view : VIEWS) {
      viewManager.registerView(view);
    }
  }

  /** log the recorded to stats. */
  public void logStats() {
    for (var view : VIEWS) {
      var viewName = view.getName();
      log.info(
          String.format(
              "Recorded stats for %s:\n %s", viewName.asString(), viewManager.getView(viewName)));
    }
  }

  public static BucketBoundaries byteBuckets() {
    return BucketBoundaries.create(
        List.of(0.0, 1.0 * KiB, 16.0 * KiB, 256.0 * KiB, 1.0 * MiB, 16.0 * MiB, 256.0 * MiB));
  }

  public static BucketBoundaries nanosTimeBuckets() {
    return BucketBoundaries.create(
        List.of(
            0.0,
            1_000.0, // 1 microsecond
            1_000_000.0, // 1 millisecond
            1_000_000_000.0, // 1 second
            60_000_000_000.0)); // 1 minute
  }

  public static BucketBoundaries countBuckets() {
    return BucketBoundaries.create(List.of(0.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0));
  }

  /**
   * This class groups together all of the parts needed to track metrics around publish buffer size
   * when queued.
   */
  public static class BufferSizeWhenQueued {
    public static final View.Name NAME = View.Name.create("spez_buffer_size_when_queued");
    public static final String DESCRIPTION = "Publish buffer size when message queued";
    public static final Measure.MeasureLong MEASURE =
        Measure.MeasureLong.create("buffer-size-when-queued", DESCRIPTION, "");
    public static final BucketBoundaries BUCKETS = SpezMetrics.countBuckets();
    public static final Aggregation AGGREGATION = Aggregation.Distribution.create(BUCKETS);
    public static final List<TagKey> TAGS = List.of(SpezTagging.TAILER_TABLE_KEY);
    public static final View VIEW = View.create(NAME, DESCRIPTION, MEASURE, AGGREGATION, TAGS);
  }

  /**
   * This class groups together all of the parts needed to track metrics around row converted to
   * message duration.
   */
  public static class ConvertedToMessageDuration {
    public static final View.Name NAME = View.Name.create("spez_converted_to_message_duration");
    public static final String DESCRIPTION =
        "Row converted to avro message duration in nanoseconds";
    public static final Measure.MeasureLong MEASURE =
        Measure.MeasureLong.create(
            "row-converted-to-message-duration", "Row converted to avro message duration", "ns");
    public static final BucketBoundaries BUCKETS = SpezMetrics.nanosTimeBuckets();
    public static final Aggregation AGGREGATION = Aggregation.Distribution.create(BUCKETS);
    public static final List<TagKey> TAGS = List.of(SpezTagging.TAILER_TABLE_KEY);
    public static final View VIEW = View.create(NAME, DESCRIPTION, MEASURE, AGGREGATION, TAGS);
  }

  /**
   * This class groups together all of the parts needed to track metrics around messages published.
   */
  public static class MessagePublished {
    public static final View.Name NAME = View.Name.create("spez_msg_published_count");
    public static final String DESCRIPTION = "Number of messages sent by the publisher";
    public static final Measure.MeasureLong MEASURE =
        Measure.MeasureLong.create("message-published", DESCRIPTION, "");
    public static final BucketBoundaries BUCKETS = SpezMetrics.byteBuckets();
    public static final Aggregation AGGREGATION = Aggregation.Count.create();
    public static final List<TagKey> TAGS = List.of(SpezTagging.TAILER_TABLE_KEY);
    public static final View VIEW = View.create(NAME, DESCRIPTION, MEASURE, AGGREGATION, TAGS);
  }

  /**
   * This class groups together all of the parts needed to track metrics around message published
   * duration.
   */
  public static class MessagePublishedDuration {
    public static final View.Name NAME = View.Name.create("spez_message_published_duration");
    public static final String DESCRIPTION = "Message published duration in nanoseconds";
    public static final Measure.MeasureLong MEASURE =
        Measure.MeasureLong.create(
            "message-published-duration", "Message published duration", "ns");
    public static final BucketBoundaries BUCKETS = SpezMetrics.nanosTimeBuckets();
    public static final Aggregation AGGREGATION = Aggregation.Distribution.create(BUCKETS);
    public static final List<TagKey> TAGS = List.of(SpezTagging.TAILER_TABLE_KEY);
    public static final View VIEW = View.create(NAME, DESCRIPTION, MEASURE, AGGREGATION, TAGS);
  }

  /**
   * This class groups together all of the parts needed to track metrics around message publish
   * requested duration.
   */
  public static class MessagePublishRequestedDuration {
    public static final View.Name NAME =
        View.Name.create("spez_message_publish_requested_duration");
    public static final String DESCRIPTION = "Message publish requested duration in nanoseconds";
    public static final Measure.MeasureLong MEASURE =
        Measure.MeasureLong.create(
            "message-publish-requested-duration", "Message publish requested duration", "ns");
    public static final BucketBoundaries BUCKETS = SpezMetrics.nanosTimeBuckets();
    public static final Aggregation AGGREGATION = Aggregation.Distribution.create(BUCKETS);
    public static final List<TagKey> TAGS = List.of(SpezTagging.TAILER_TABLE_KEY);
    public static final View VIEW = View.create(NAME, DESCRIPTION, MEASURE, AGGREGATION, TAGS);
  }

  /**
   * This class groups together all of the parts needed to track metrics around messages received.
   */
  public static class MessageReceived {
    public static final View.Name NAME = View.Name.create("spez_msg_received_count");
    public static final String DESCRIPTION = "Number of messages received by the publisher";
    public static final Measure.MeasureLong MEASURE =
        Measure.MeasureLong.create("message-published", DESCRIPTION, "");
    public static final BucketBoundaries BUCKETS = SpezMetrics.byteBuckets();
    public static final Aggregation AGGREGATION = Aggregation.Count.create();
    public static final List<TagKey> TAGS = List.of(SpezTagging.TAILER_TABLE_KEY);
    public static final View VIEW = View.create(NAME, DESCRIPTION, MEASURE, AGGREGATION, TAGS);
  }

  /**
   * This class groups together all of the parts needed to track metrics around avro message size.
   */
  public static class MessageSize {
    public static final View.Name NAME = View.Name.create("spez_message_size");
    public static final String DESCRIPTION = "Avro message size in bytes";
    public static final Measure.MeasureLong MEASURE =
        Measure.MeasureLong.create("message-size", "Avro message size", "By");
    public static final BucketBoundaries BUCKETS = SpezMetrics.byteBuckets();
    public static final Aggregation AGGREGATION = Aggregation.Distribution.create(BUCKETS);
    public static final List<TagKey> TAGS = List.of(SpezTagging.TAILER_TABLE_KEY);
    public static final View VIEW = View.create(NAME, DESCRIPTION, MEASURE, AGGREGATION, TAGS);
  }

  /**
   * This class groups together all of the parts needed to track metrics around row queued for
   * conversion duration.
   */
  public static class QueuedForConversionDuration {
    public static final View.Name NAME = View.Name.create("spez_queued_for_conversion_duration");
    public static final String DESCRIPTION = "Row queued for conversion duration in nanoseconds";
    public static final Measure.MeasureLong MEASURE =
        Measure.MeasureLong.create(
            "row-queued-for-conversion-duration", "Row queued for conversion duration", "ns");
    public static final BucketBoundaries BUCKETS = SpezMetrics.nanosTimeBuckets();
    public static final Aggregation AGGREGATION = Aggregation.Distribution.create(BUCKETS);
    public static final List<TagKey> TAGS = List.of(SpezTagging.TAILER_TABLE_KEY);
    public static final View VIEW = View.create(NAME, DESCRIPTION, MEASURE, AGGREGATION, TAGS);
  }

  /**
   * This class groups together all of the parts needed to track metrics around message queued for
   * publishing duration.
   */
  public static class QueuedForPublishingDuration {
    public static final View.Name NAME = View.Name.create("spez_queued_for_publishing_duration");
    public static final String DESCRIPTION =
        "Message queued for publishing duration in nanoseconds";
    public static final Measure.MeasureLong MEASURE =
        Measure.MeasureLong.create(
            "message-queued-for-publishing-duration",
            "Message queued for publishing duration",
            "ns");
    public static final BucketBoundaries BUCKETS = SpezMetrics.nanosTimeBuckets();
    public static final Aggregation AGGREGATION = Aggregation.Distribution.create(BUCKETS);
    public static final List<TagKey> TAGS = List.of(SpezTagging.TAILER_TABLE_KEY);
    public static final View VIEW = View.create(NAME, DESCRIPTION, MEASURE, AGGREGATION, TAGS);
  }

  /**
   * This class groups together all of the parts needed to track metrics around row read duration.
   */
  public static class RowReadDuration {
    public static final View.Name NAME = View.Name.create("spez_row_read_duration");
    public static final String DESCRIPTION = "Spanner row read duration in nanoseconds";
    public static final Measure.MeasureLong MEASURE =
        Measure.MeasureLong.create("row-read-duration", "Spanner row read duration", "ns");
    public static final BucketBoundaries BUCKETS = SpezMetrics.nanosTimeBuckets();
    public static final Aggregation AGGREGATION = Aggregation.Distribution.create(BUCKETS);
    public static final List<TagKey> TAGS = List.of(SpezTagging.TAILER_TABLE_KEY);
    public static final View VIEW = View.create(NAME, DESCRIPTION, MEASURE, AGGREGATION, TAGS);
  }

  /** This class groups together all of the parts needed to track metrics around row size. */
  public static class RowSize {
    public static final View.Name NAME = View.Name.create("spez_row_size");
    public static final String DESCRIPTION = "Spanner row size in bytes";
    public static final Measure.MeasureLong MEASURE =
        Measure.MeasureLong.create("row-size", "Spanner row size", "By");
    public static final BucketBoundaries BUCKETS = SpezMetrics.byteBuckets();
    public static final Aggregation AGGREGATION = Aggregation.Distribution.create(BUCKETS);
    public static final List<TagKey> TAGS = List.of(SpezTagging.TAILER_TABLE_KEY);
    public static final View VIEW = View.create(NAME, DESCRIPTION, MEASURE, AGGREGATION, TAGS);
  }
}
