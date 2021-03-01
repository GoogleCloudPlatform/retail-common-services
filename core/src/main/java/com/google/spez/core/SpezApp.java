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
import com.google.spez.core.internal.BothanDatabase;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpezApp {
  private static final Logger log = LoggerFactory.getLogger(SpezApp.class);

  /**
   * run a SpezApp with the given config.
   *
   * @param config configures the SpezApp
   * @throws ExecutionException task aborted exception.
   * @throws InterruptedException task interrupted exception.
   */
  public static void run(SpezConfig config) throws ExecutionException, InterruptedException {
    LoggerDumper.dump();

    var publisher = EventPublisher.create(config);
    final ListeningScheduledExecutorService scheduler =
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));
    var extractor = new MetadataExtractor(config);

    var database = BothanDatabase.openDatabase(config.getSink().getSettings());

    log.info("Fetching last processed timestamp");
    var lastProcessedTimestamp =
        LastProcessedTimestamp.getLastProcessedTimestamp(config.getSink(), config.getLpts());
    log.info("Retrieved last processed timestamp, parsing schema");
    SpannerSchema spannerSchema = new SpannerSchema(database, config.getSink());
    SchemaSet schemaSet = spannerSchema.getSchema();
    log.info("Successfully Processed the Table Schema. Starting the tailer now ...");
    var handler = new RowProcessor(config.getSink(), schemaSet, publisher, extractor);
    String databasePath = config.getSink().databasePath();
    log.info("Building database with path '{}'", databasePath);

    final SpannerTailer tailer =
        new SpannerTailer(config, database, handler, lastProcessedTimestamp);
    tailer.start();
    var schedulerFuture =
        scheduler.scheduleAtFixedRate(
            () -> {
              handler.logStats();
              tailer.logStats();
            },
            30,
            30,
            TimeUnit.SECONDS);
    ListenableFutureErrorHandler.create(
        scheduler,
        schedulerFuture,
        (throwable) -> {
          log.error("logStats scheduled task error", throwable);
        });
  }
}
