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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.ByteString;
import com.google.spannerclient.Row;
import com.google.spez.core.EventPublisher;
import com.google.spez.core.SpannerEvent;
import com.google.spez.core.SpannerEventHandler;
import com.google.spez.core.SpannerToAvro;
import com.google.spez.core.SpannerToAvro.SchemaSet;
import com.google.spez.core.Spez;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DisruptorHandler implements SpannerEventHandler, EventHandler<SpannerEvent> {
  private static final Logger log = LoggerFactory.getLogger(DisruptorHandler.class);

  private static final int BUFFER_SIZE = 1024;
  private static final int THREAD_POOL = 12;

  private final Disruptor<SpannerEvent> disruptor =
      new Disruptor<SpannerEvent>(
          SpannerEvent::new,
          BUFFER_SIZE,
          DaemonThreadFactory.INSTANCE,
          ProducerType.SINGLE,
          new YieldingWaitStrategy());
  private final List<ListeningExecutorService> l =
      Spez.ServicePoolGenerator(THREAD_POOL, "Spanner Tailer Event Worker");
  private final SchemaSet schemaSet;
  private final ThreadLocal<EventPublisher> publisher;
  private final Map<String, String> metadata;
  private final Executor executor;

  public DisruptorHandler(
      SchemaSet schemaSet,
      ThreadLocal<EventPublisher> publisher,
      Map<String, String> metadata,
      Executor executor) {
    this.schemaSet = schemaSet;
    this.publisher = publisher;
    this.metadata = metadata;
    this.executor = executor;
  }

  public void start() {
    disruptor.handleEventsWith(this).then((event, sequence, endOfBatch) -> event.clear());

    disruptor.start();
  }

  @Override
  public void onEvent(SpannerEvent event, long sequence, boolean endOfBatch) {
    process(0, event.row(), event.timestamp());
  }

  /**
   * Processes the event from the {@code SpannerTailer} in the form of a method callback.
   *
   * @param bucket Consistent Hash Table bucket id to aid in construction of method specific thread
   *     pooling strategies
   * @param event The Cloud Spanner {@code Struct} to create an immutable representation of the
   *     event.
   * @param timestamp the Cloud Spanner Commit Timestamp for the event
   */
  @Override
  public Boolean process(int bucket, Row row, String timestamp) {
    ListenableFuture<Boolean> x =
        l.get(l.size() % THREAD_POOL)
            .submit(
                () -> {
                  Optional<ByteString> record = SpannerToAvro.MakeRecord(schemaSet, row);
                  if (record.isPresent()) {
                    publisher.get().publish(record.get(), metadata, timestamp, executor);
                    log.info("Published: " + record.get().toString() + " " + timestamp);
                  }
                  return Boolean.TRUE;
                });

    Futures.addCallback(
        x,
        new FutureCallback<Boolean>() {

          @Override
          public void onSuccess(Boolean result) {
            log.info("Record Successfully Published");
          }

          @Override
          public void onFailure(Throwable t) {
            log.error("Unable to process record", t);
          }
        },
        l.get(l.size() % THREAD_POOL));

    return Boolean.TRUE;
  }

  public RingBuffer<SpannerEvent> getRingBuffer() {
    return disruptor.getRingBuffer();
  }
}
