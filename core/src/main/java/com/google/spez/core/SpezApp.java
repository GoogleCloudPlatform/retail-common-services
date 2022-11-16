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

import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.spez.common.ListenableFutureErrorHandler;
import com.google.spez.common.LoggerDumper;
import com.google.spez.spanner.DatabaseFactory;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpezApp {
  private static final Logger log = LoggerFactory.getLogger(SpezApp.class);

  public static void setupLogScheduler(Runnable runnable) {
    final ListeningScheduledExecutorService scheduler =
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));
    var schedulerFuture = scheduler.scheduleAtFixedRate(runnable, 30, 30, TimeUnit.SECONDS);
    ListenableFutureErrorHandler.create(
        scheduler,
        schedulerFuture,
        (throwable) -> {
          log.error("logStats scheduled task error", throwable);
        });
  }

  /**
   * run a SpezApp with the given config.
   *
   * @param config configures the SpezApp
   * @throws ExecutionException task aborted exception.
   * @throws InterruptedException task interrupted exception.
   */
  public static void run(SpezConfig config) {
    try {
      LoggerDumper.dump();
      SpezMetrics.setupViews();

      var lptsDatabase = DatabaseFactory.openLptsDatabase(config.getLpts());
      log.info("Fetching last processed timestamp");
      var lastProcessedTimestamp =
          LastProcessedTimestamp.getLastProcessedTimestamp(
              lptsDatabase, config.getSink(), config.getLpts());

      var database = DatabaseFactory.openSinkDatabase(config.getSink());
      log.info("Retrieved last processed timestamp, parsing schema");
      SpannerSchema spannerSchema = new SpannerSchema(database, config.getSink());
      SchemaSet schemaSet = spannerSchema.getSchema();

      log.info("Successfully Processed the Table Schema. Starting the tailer now ...");
      var publisher = EventPublisher.create(config);
      var extractor = new MetadataExtractor(config);
      var handler = new RowProcessor(config.getSink(), publisher, extractor);

      final SpannerTailer tailer =
          new SpannerTailer(config, database, handler, lastProcessedTimestamp);
      tailer.start();
      final LptsUpdater lptsUpdater = LptsUpdater.create(config);
      lptsUpdater.start();
      setupLogScheduler(
          () -> {
            handler.logStats();
            tailer.logStats();
          });
    } catch (Exception ex) {
      log.error("Unhandled exception", ex);
      throw ex;
    }
  }
}
