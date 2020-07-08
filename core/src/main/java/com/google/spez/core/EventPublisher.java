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

import static com.google.cloud.pubsub.v1.OpenCensusUtil.OPEN_CENSUS_MESSAGE_TRANSFORM;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.google.spannerclient.Options;
import com.google.spannerclient.PubSub;
import com.google.spannerclient.PublishOptions;
import com.google.spannerclient.Publisher;
import io.opencensus.common.Scope;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class published events from Cloud Spanner to Pub/Sub */
public class EventPublisher {

  public static class BufferPayload {
    final PubsubMessage message;
    final SettableFuture<String> future;
    final Scope scope;

    public BufferPayload(PubsubMessage message, SettableFuture<String> future, Scope scope) {
      this.message = message;
      this.future = future;
      this.scope = scope;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);
  private static final int DEFAULT_BUFFER_SIZE = 950; // TODO(pdex): move to config
  private static final int DEFAULT_BUFFER_TIME = 30; // TODO(pdex): move to config
  private static final Tracer tracer = Tracing.getTracer();

  private final LinkedTransferQueue<BufferPayload> buffer = new LinkedTransferQueue<>();
  private final AtomicLong bufferSize = new AtomicLong(0);

  private final SpezConfig config;
  private final ScheduledExecutorService scheduler;
  private final String projectName;
  private final String topicName;
  private final int publishBufferSize;
  private final int publishBufferTime;

  private Publisher publisher;
  private Instant lastPublished;

  public EventPublisher(String projectName, String topicName, SpezConfig config) {
    Preconditions.checkNotNull(projectName);
    Preconditions.checkNotNull(topicName);
    Preconditions.checkNotNull(config);

    this.config = config;
    this.scheduler = Executors.newScheduledThreadPool(2);
    this.projectName = projectName;
    this.topicName = topicName;
    this.publishBufferSize = DEFAULT_BUFFER_SIZE;
    this.publishBufferTime = DEFAULT_BUFFER_TIME;
    this.publisher = configurePubSub(projectName, topicName);
    this.lastPublished = Instant.now();
  }

  public EventPublisher(
      String projectName,
      String topicName,
      int publishBufferSize,
      int publishBufferTime,
      SpezConfig config) {
    Preconditions.checkNotNull(projectName);
    Preconditions.checkNotNull(topicName);
    Preconditions.checkArgument(publishBufferSize > 0);
    Preconditions.checkArgument(publishBufferTime > 0);

    this.config = config;
    this.scheduler = Executors.newScheduledThreadPool(2);
    this.projectName = projectName;
    this.topicName = topicName;
    this.publishBufferSize = publishBufferSize;
    this.publishBufferTime = publishBufferTime;
    this.publisher = configurePubSub(projectName, topicName);
    this.lastPublished = Instant.now();
  }

  private Publisher configurePubSub(String projectName, String topicName) {
    Preconditions.checkNotNull(projectName);
    Preconditions.checkNotNull(topicName);

    scheduler.scheduleAtFixedRate(this::maybePublish, 0, publishBufferTime, TimeUnit.SECONDS);

    return PubSub.getPublisher(
        config.getAuth().getCredentials(), Options.DEFAULT(), projectName, topicName);
  }

  @SuppressWarnings("MustBeClosedChecker")
  private ListenableFuture<String> addToBuffer(PubsubMessage message, Span parent) {
    Scope scopedSpan =
        tracer.spanBuilderWithExplicitParent("EventPublisher.publish", parent).startScopedSpan();
    SettableFuture<String> future = SettableFuture.create();
    buffer.add(new BufferPayload(OPEN_CENSUS_MESSAGE_TRANSFORM.apply(message), future, scopedSpan));
    bufferSize.incrementAndGet();
    Futures.addCallback(
        future,
        new FutureCallback<String>() {

          @Override
          public void onSuccess(String result) {
            scopedSpan.close();
          }

          @Override
          public void onFailure(Throwable t) {
            scopedSpan.close();
          }
        },
        scheduler);
    return future;
  }

  private void publishBuffer() {
    ArrayList<BufferPayload> sink = new ArrayList<>();
    int numberDrained = buffer.drainTo(sink);
    ArrayList<PubsubMessage> messages = new ArrayList<>(numberDrained);
    for (var payload : sink) {
      messages.add(payload.message);
    }
    if (numberDrained == 0) {
      return;
    }
    final ListenableFuture<PublishResponse> future =
        PubSub.publishAsync(
            PublishOptions.DEFAULT(),
            publisher,
            PublishRequest.newBuilder()
                .setTopic(publisher.getTopicPath())
                .addAllMessages(messages)
                .build());

    lastPublished = Instant.now();
    log.debug("{} messages drained and published", numberDrained);
    bufferSize.getAndAdd(-1 * numberDrained);

    Futures.addCallback(
        future,
        new FutureCallback<PublishResponse>() {

          @Override
          public void onSuccess(PublishResponse response) {
            if (response.getMessageIdsCount() > sink.size()) {
              log.warn(
                  "Too many response messages {} for request size {}",
                  response.getMessageIdsCount(),
                  sink.size());
            } else if (response.getMessageIdsCount() < sink.size()) {
              log.warn(
                  "Insufficient number of response messages {} for request size {}",
                  response.getMessageIdsCount(),
                  sink.size());
            }
            for (int i = 0; i < sink.size(); i++) {
              var payload = sink.get(i);
              if (i < response.getMessageIdsCount()) {
                payload.future.set(response.getMessageIds(i));
              } else {
                payload.future.set("UNAVAILABLE");
              }
            }
          }

          @Override
          public void onFailure(Throwable t) {
            for (var payload : sink) {
              payload.future.setException(t);
            }
          }
        },
        scheduler);
  }

  private void maybePublish() {
    Instant now = Instant.now();
    Duration d = Duration.between(lastPublished, now);
    long size = bufferSize.get();
    if (size >= publishBufferSize || d.getSeconds() > publishBufferTime) {
      log.debug("publish buffer size {} duration {}", size, d.getSeconds());
      publishBuffer();
    } else {
      log.debug("didn't publish buffer size {} duration {}", size, d.getSeconds());
    }
  }

  /**
   * Publishes a Bytestring and associated metadata map as a pub/sub message to the configured
   * topic.
   *
   * @param data Body of the Pub/Sub message
   * @param attrMap Attribute Map of data to be published as metadata with your message
   * @param timestamp the Commit Timestamp of the Spanner Record to be published
   */
  public ListenableFuture<String> publish(
      ByteString data,
      Map<String, String> attrMap,
      String timestamp,
      Span parent,
      Executor executor) {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(attrMap);
    Preconditions.checkNotNull(timestamp);
    Preconditions.checkNotNull(executor);

    final PubsubMessage.Builder builder =
        PubsubMessage.newBuilder().setData(data).setOrderingKey(publisher.getTopicPath());

    if (attrMap.size() > 0) {
      attrMap
          .entrySet()
          .forEach(
              e -> {
                builder.putAttributes(e.getKey(), e.getValue());
              });
    }

    ListenableFuture<String> future = addToBuffer(builder.build(), parent);

    maybePublish();
    return future;
  }
}
