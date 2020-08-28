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
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.spez.core.EventPublisher;
import com.google.spez.core.ListenableFutureErrorHandler;
import com.google.spez.core.MetadataExtractor;
import com.google.spez.core.SpannerTailer;
import com.google.spez.core.SpannerToAvro.SchemaSet;
import com.google.spez.core.Spez;
import com.google.spez.core.SpezConfig;
import com.google.spez.core.WorkStealingHandler;
import com.typesafe.config.ConfigFactory;
import io.opencensus.contrib.zpages.ZPageHandlers;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static final int THREAD_POOL = 12; // TODO(pdex): move to config

  private static void setupStackdriver(SpezConfig config) throws IOException {
    // For demo purposes, always sample
    TraceConfig traceConfig = Tracing.getTraceConfig();
    traceConfig.updateActiveTraceParams(
        traceConfig
            .getActiveTraceParams()
            .toBuilder()
            .setSampler(Samplers.alwaysSample()) // TODO(pdex): move to config
            .build());

    // Create the Stackdriver trace exporter
    StackdriverTraceExporter.createAndRegister(
        StackdriverTraceConfiguration.builder()
            .setProjectId(config.getPubSub().getProjectId())
            .setCredentials(config.getAuth().getCredentials())
            .build());
  }

  public static void main(String[] args) throws Exception {
    SpezConfig config = SpezConfig.parse(ConfigFactory.load());

    setupStackdriver(config);

    ZPageHandlers.startHttpServerAndRegisterAll(8887);

    // TODO(pdex): why are we making our own threadpool?
    final List<ListeningExecutorService> l =
        Spez.ServicePoolGenerator(THREAD_POOL, "Spanner Tailer Event Worker");

    final SpannerTailer tailer =
        new SpannerTailer(
            config.getAuth().getCredentials(),
            THREAD_POOL,
            200000000); // TODO(pdex): move to config
    // final EventPublisher publisher = new EventPublisher(config.getPubSub().getProjectId(),
    // config.getPubSub().getTopic(), config);
    var publisher = EventPublisher.create(config);
    final CountDownLatch doneSignal = new CountDownLatch(1);
    final ListeningScheduledExecutorService scheduler =
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));
    var extractor = new MetadataExtractor(config);

    final ListenableFuture<SchemaSet> schemaSetFuture =
        tailer.getSchema(
            config.getSink().getProjectId(),
            config.getSink().getInstance(),
            config.getSink().getDatabase(),
            config.getSink().getTable(),
            config.getSink());

    Futures.addCallback(
        schemaSetFuture,
        new FutureCallback<SchemaSet>() {

          @Override
          public void onSuccess(SchemaSet schemaSet) {
            log.info("Successfully Processed the Table Schema. Starting the poller now ...");
            WorkStealingHandler handler = new WorkStealingHandler(schemaSet, publisher, extractor);
            tailer.start(
                handler,
                schemaSet.tsColName(),
                l.size(),
                THREAD_POOL,
                500,
                config.getSink().getProjectId(),
                config.getSink().getInstance(),
                config.getSink().getDatabase(),
                config.getSink().getTable(),
                "lpts_table",
                "2000",
                500,
                500,
                config.getSink(),
                config.getLpts());

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
            log.info("CALLING COUNTDOWN");
            doneSignal.countDown();
            log.info("COUNTDOWN CALLED");
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
      log.info("GOT  doneSignal");
    } catch (InterruptedException e) {
      log.error("Interrupted", e);
      throw e;
    }
  }
}
