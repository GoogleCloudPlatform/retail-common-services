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
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.ViewData;
import io.opencensus.stats.ViewManager;
import java.util.Arrays;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpezMetrics {
  private static final Logger log = LoggerFactory.getLogger(SpezMetrics.class);
  private static final ViewManager viewManager = Stats.getViewManager();
  private static final StatsRecorder statsRecorder = Stats.getStatsRecorder();

  // videoSize will measure the size of processed videos.
  private static final Measure.MeasureLong MESSAGE_SIZE =
      Measure.MeasureLong.create("message-size", "Spanner message size", "By");

  private static final long MiB = 1 << 20;

  // Create view to see the processed message size distribution broken down by table.
  // The view has bucket boundaries (0, 16 * MiB, 65536 * MiB) that will group measure
  // values into histogram buckets.
  private static final View.Name MESSAGE_SIZE_VIEW_NAME =
      View.Name.create("spez_message_size");
  private static final View MESSAGE_SIZE_VIEW =
      View.create(
          MESSAGE_SIZE_VIEW_NAME,
          "processed message size over time",
          MESSAGE_SIZE,
          Aggregation.Distribution.create(
              BucketBoundaries.create(Arrays.asList(0.0, 16.0 * MiB, 256.0 * MiB))),
          Collections.singletonList(SpezTagging.TAILER_TABLE_KEY));


  public static final View.Name MSG_RECEIVED_VIEW_NAME = View.Name.create("spez_msg_received_count");
  public static final View.Name MSG_PUBLISHED_VIEW_NAME = View.Name.create("spez_msg_published_count");

  public SpezMetrics() {
    viewManager.registerView(MESSAGE_SIZE_VIEW);
  }

  public void addMessageSize(long size, String tableName) {
    statsRecorder.newMeasureMap().put(MESSAGE_SIZE, size).record(SpezTagging.tagForTable(tableName));
  }

  /** log the recorded to stats. */
  public void logStats() {
    for (var view : Arrays.asList(MESSAGE_SIZE_VIEW_NAME, MSG_RECEIVED_VIEW_NAME, MSG_PUBLISHED_VIEW_NAME)) {
      /*
      ViewData viewData = viewManager.getView(MESSAGE_SIZE_VIEW_NAME);
      log.info(
          String.format("Recorded stats for %s:\n %s", MESSAGE_SIZE_VIEW_NAME.asString(), viewData));
          */
      log.info(
          String.format("Recorded stats for %s:\n %s", view.asString(), viewManager.getView(view)));
    }
  }
}
