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
package com.google.spez.cdc;

import ch.qos.logback.classic.LoggerContext;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.spez.core.EventPublisher;
import com.google.spez.core.SpannerTailer;
import com.google.spez.core.SpannerToAvro.SchemaSet;
import com.google.spez.core.Spez;
import com.google.spez.core.SpezConfig;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static final int BUFFER_SIZE = 1024; // TODO(pdex): move to config
  private static final int THREAD_POOL = 12; // TODO(pdex): move to config

  private static final boolean DISRUPTOR = false;

  public static void main(String[] args) {
    SpezConfig config = SpezConfig.parse(ConfigFactory.load());
    // TODO(pdex): why are we making our own threadpool?
    final List<ListeningExecutorService> l =
        Spez.ServicePoolGenerator(THREAD_POOL, "Spanner Tailer Event Worker");

    final SpannerTailer tailer =
        new SpannerTailer(THREAD_POOL, 200000000); // TODO(pdex): move to config
    // final EventPublisher publisher = new EventPublisher(PROJECT_NAME, TOPIC_NAME);
    final ThreadLocal<EventPublisher> publisher =
        ThreadLocal.withInitial(
            () -> {
              return new EventPublisher(
                  config.getPubSub().getProjectId(), config.getPubSub().getTopic());
            });
    final ExecutorService workStealingPool = Executors.newWorkStealingPool();
    final ListeningExecutorService forkJoinPool =
        MoreExecutors.listeningDecorator(workStealingPool);
    final Map<String, String> metadata = new HashMap<>();
    final CountDownLatch doneSignal = new CountDownLatch(1);
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // Populate CDC Metadata
    metadata.put("SrcDatabase", config.getSpannerDb().getDatabase());
    metadata.put("SrcTablename", config.getSpannerDb().getTable());
    metadata.put("DstTopic", config.getPubSub().getTopic());
    // TODO(pdex): add UUID to metadata
    // TODO(pdex): add timestamp to metadata

    final ListenableFuture<SchemaSet> schemaSetFuture =
        tailer.getSchema(
            config.getSpannerDb().getProjectId(),
            config.getSpannerDb().getInstance(),
            config.getSpannerDb().getDatabase(),
            config.getSpannerDb().getTable(),
            config.getSpannerDb());

    Futures.addCallback(
        schemaSetFuture,
        new FutureCallback<SchemaSet>() {

          @Override
          public void onSuccess(SchemaSet schemaSet) {
            log.info("Successfully Processed the Table Schema. Starting the poller now ...");
            if (DISRUPTOR) {
              DisruptorHandler handler =
                  new DisruptorHandler(schemaSet, publisher, metadata, l.get(0));
              handler.start();
              tailer.setRingBuffer(handler.getRingBuffer());

              ScheduledFuture<?> result =
                  tailer.start(
                      2,
                      500,
                      config.getSpannerDb().getProjectId(),
                      config.getSpannerDb().getInstance(),
                      config.getSpannerDb().getDatabase(),
                      config.getSpannerDb().getTable(),
                      "lpts_table",
                      schemaSet.tsColName(),
                      "2000");

              doneSignal.countDown();
            } else {
              WorkStealingHandler handler =
                  new WorkStealingHandler(scheduler, schemaSet, publisher, metadata);
              tailer.start(
                  handler,
                  schemaSet.tsColName(),
                  l.size(),
                  THREAD_POOL,
                  500,
                  config.getSpannerDb().getProjectId(),
                  config.getSpannerDb().getInstance(),
                  config.getSpannerDb().getDatabase(),
                  config.getSpannerDb().getTable(),
                  "lpts_table",
                  "2000",
                  500,
                  500);

              scheduler.scheduleAtFixedRate(
                  () -> {
                    handler.logStats();
                    tailer.logStats();
                  },
                  30,
                  30,
                  TimeUnit.SECONDS);

              doneSignal.countDown();
            }
          }

          @Override
          public void onFailure(Throwable t) {
            log.error("Unable to process schema", t);
            System.exit(-1);
          }
        },
        l.get(l.size() % THREAD_POOL));

    try {
      log.debug("Dumping all known Loggers");
      LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
      java.util.Iterator<ch.qos.logback.classic.Logger> it = lc.getLoggerList().iterator();
      while (it.hasNext()) {
        ch.qos.logback.classic.Logger thisLog = it.next();
        log.debug("name: {} status: {}", thisLog.getName(), thisLog.getLevel());
      }
      log.info("waiting for doneSignal");
      doneSignal.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
