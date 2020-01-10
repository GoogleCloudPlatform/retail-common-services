/*
 * Copyright 2019 Google LLC
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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.ByteString;
import com.google.spez.core.EventPublisher;
import com.google.spez.core.SpannerEvent;
import com.google.spez.core.SpannerEventHandler;
import com.google.spez.core.SpannerTailer;
import com.google.spez.core.SpannerToAvro;
import com.google.spez.core.SpannerToAvro.SchemaSet;
import com.google.spez.core.Spez;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static final String PROJECT_NAME = "retail-common-services-249016";
  private static final String INSTANCE_NAME = "test-db";
  private static final String DB_NAME = "test";
  private static final String TABLE_NAME = "test";
  private static final String TOPIC_NAME = "test-topic";
  private static final int BUFFER_SIZE = 1024;
  private static final int THREAD_POOL = 12;

  private static final boolean DISRUPTOR = false;

  public static void main(String[] args) {
    final List<ListeningExecutorService> l =
        Spez.ServicePoolGenerator(THREAD_POOL, "Spanner Tailer Event Worker");

    final SpannerTailer tailer = new SpannerTailer(THREAD_POOL, 200000000);
    final EventPublisher publisher = new EventPublisher(PROJECT_NAME, TOPIC_NAME);
    final Map<String, String> metadata = new HashMap<>();
    final CountDownLatch doneSignal = new CountDownLatch(1);
    final Disruptor<SpannerEvent> disruptor =
        new Disruptor<SpannerEvent>(
            SpannerEvent::new,
            BUFFER_SIZE,
            DaemonThreadFactory.INSTANCE,
            ProducerType.SINGLE,
            new YieldingWaitStrategy());

    // Populate CDC Metadata
    metadata.put("SrcDatabase", DB_NAME);
    metadata.put("SrcTablename", TABLE_NAME);
    metadata.put("DstTopic", TOPIC_NAME);

    final ListenableFuture<SchemaSet> schemaSetFuture =
        tailer.getSchema(PROJECT_NAME, INSTANCE_NAME, DB_NAME, TABLE_NAME);

    Futures.addCallback(
        schemaSetFuture,
        new FutureCallback<SchemaSet>() {

          @Override
          public void onSuccess(SchemaSet schemaSet) {
            log.info("Successfully Processed the Table Schema. Starting the poller now ...");

            if (DISRUPTOR) {
              disruptor
                  .handleEventsWith(
                      (event, sequence, endOfBatch) -> {
                        ListenableFuture<Boolean> x =
                            l.get(l.size() % THREAD_POOL)
                                .submit(
                                    () -> {
                                      Optional<ByteString> record =
                                          SpannerToAvro.MakeRecord(schemaSet, event.row());
                                      if (record.isPresent()) {
                                        publisher.publish(
                                            record.get(), metadata, event.timestamp());
                                        log.debug(
                                            "Published: "
                                                + record.get().toString()
                                                + " "
                                                + event.timestamp());
                                      }
                                      return Boolean.TRUE;
                                    });

                        Futures.addCallback(
                            x,
                            new FutureCallback<Boolean>() {

                              @Override
                              public void onSuccess(Boolean result) {
                                log.debug("Record Successfully Published");
                              }

                              @Override
                              public void onFailure(Throwable t) {
                                log.error("Unable to process record", t);
                              }
                            },
                            l.get(l.size() % THREAD_POOL));
                      })
                  .then((event, sequence, endOfBatch) -> event.clear());

              disruptor.start();

              final RingBuffer<SpannerEvent> ringBuffer = disruptor.getRingBuffer();

              tailer.setRingBuffer(ringBuffer);

              ScheduledFuture<?> result =
                  tailer.start(
                      2,
                      500,
                      PROJECT_NAME,
                      INSTANCE_NAME,
                      DB_NAME,
                      TABLE_NAME,
                      "lpts_table",
                      schemaSet.tsColName(),
                      "2000");

              doneSignal.countDown();

            } else {

              final SpannerEventHandler handler =
                  (bucket, s, timestamp) -> {
                    ListenableFuture<Boolean> x =
                        l.get(bucket)
                            .submit(
                                () -> {
                                  // TODO(xjdr): Throw if empty optional
                                  log.debug("Processing Record");
                                  Optional<ByteString> record =
                                      SpannerToAvro.MakeRecord(schemaSet, s);
                                  log.debug("Record Processed, getting ready to publish");
                                  publisher.publish(record.get(), metadata, timestamp);
                                  log.debug(
                                      "Published: " + record.get().toString() + " " + timestamp);

                                  return Boolean.TRUE;
                                });

                    Futures.addCallback(
                        x,
                        new FutureCallback<Boolean>() {

                          @Override
                          public void onSuccess(Boolean result) {
                            log.debug("Record Successfully Published");
                          }

                          @Override
                          public void onFailure(Throwable t) {
                            log.error("Unable to process record", t);
                          }
                        },
                        l.get(l.size() % THREAD_POOL));

                    return Boolean.TRUE;
                  };

              tailer.start(
                  handler,
                  schemaSet.tsColName(),
                  l.size(),
                  THREAD_POOL,
                  500,
                  PROJECT_NAME,
                  INSTANCE_NAME,
                  DB_NAME,
                  TABLE_NAME,
                  "lpts_table",
                  "2000",
                  500,
                  500);

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
      doneSignal.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
