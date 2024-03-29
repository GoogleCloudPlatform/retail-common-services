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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.google.spannerclient.Options;
import com.google.spannerclient.PubSub;
import com.google.spannerclient.PublishOptions;
import com.google.spannerclient.Publisher;
import com.google.spez.common.ListenableFutureErrorHandler;
import com.google.spez.common.UsefulExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class published events from Cloud Spanner to Pub/Sub. */
@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class EventPublisher {

  public static class BufferPayload {
    final PubsubMessage message;
    final SettableFuture<String> future;
    final EventState eventState;

    /**
     * Constructor.
     *
     * @param message pubsub message
     * @param future completion future
     * @param eventState trace propagation
     */
    public BufferPayload(
        PubsubMessage message, SettableFuture<String> future, EventState eventState) {
      this.message = message;
      this.future = future;
      this.eventState = eventState;
    }
  }

  public static class PublishCallback implements FutureCallback<PublishResponse> {
    final List<BufferPayload> sink;
    final String tableName;
    LinkedTransferQueue<BufferPayload> failures;

    PublishCallback(
        List<BufferPayload> sink, String tableName, LinkedTransferQueue<BufferPayload> failures) {
      this.sink = sink;
      this.tableName = tableName;
      this.failures = failures;
    }

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
        String result;
        if (i < response.getMessageIdsCount()) {
          payload.future.set(response.getMessageIds(i));
          result = response.getMessageIds(i);
        } else {
          payload.future.set("UNAVAILABLE");
          result = "UNAVAILABLE";
        }
        if (log.isDebugEnabled()) {
          String uuid = payload.message.getAttributesMap().get(SpezConfig.SINK_UUID_KEY);
          log.trace("Published message uuid {}, set future to '{}'", uuid, result);
        }
      }
    }

    @Override
    public void onFailure(Throwable t) {
      log.error("Published failed for {} messages - queueing retries", sink.size(), t);
      for (var payload : sink) {
        // payload.future.setException(t);
        payload.eventState.messageRetrying(t);
        failures.add(payload);
      }
    }
  }

  private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);
  private static final int DEFAULT_BUFFER_SIZE = 950; // TODO(pdex): move to config
  /**
   * PubSub has a limit of 1000 messages per PublishRequest. We will batch up at most
   * MAX_PUBLISH_SIZE messages for each request. This number should be less than 1000 base on
   * experience with PubSub grumpiness.
   */
  private static final int MAX_PUBLISH_SIZE = 950;

  private static final int MAX_RETRY_COUNT = 3; // TODO(xjdr): Maybe make this a config?

  private final LinkedTransferQueue<BufferPayload> buffer = new LinkedTransferQueue<>();
  private final LinkedTransferQueue<BufferPayload> failures = new LinkedTransferQueue<>();
  @VisibleForTesting final AtomicLong bufferSize = new AtomicLong(0);
  private final AtomicBoolean publishSubmitted = new AtomicBoolean(false);

  private final String tableName;
  private final ListeningScheduledExecutorService scheduler;
  private final int publishBufferSize;
  private final int publishBufferTime;

  private final Publisher publisher;
  @VisibleForTesting final Runnable runPublishBuffer;

  /**
   * constructor visible for testing.
   *
   * @param tableName name of the table that we're publishing from
   * @param scheduler scheduler service
   * @param publisher publisher
   * @param publishSize publish size
   * @param publishTime publish time
   * @param runPublishBuffer runnable to perform publishing
   */
  @VisibleForTesting
  public EventPublisher(
      String tableName,
      ListeningScheduledExecutorService scheduler,
      Publisher publisher,
      int publishSize,
      int publishTime,
      Runnable runPublishBuffer) {
    Preconditions.checkNotNull(tableName, "tableName must not be null");
    Preconditions.checkNotNull(scheduler, "scheduler must not be null");
    Preconditions.checkNotNull(publisher, "publisher must not be null");
    Preconditions.checkArgument(publishSize >= 0, "publishSize must be greater than or equal to 0");
    Preconditions.checkArgument(publishTime >= 0, "publishTime must be greater than or equal to 0");
    this.tableName = tableName;
    this.scheduler = scheduler;
    this.publisher = publisher;
    if (runPublishBuffer != null) {
      this.runPublishBuffer = runPublishBuffer;
    } else {
      this.runPublishBuffer = this::publishBufferTimeout;
    }
    this.publishBufferSize = publishSize;
    this.publishBufferTime = publishTime;
  }

  public EventPublisher(
      String tableName,
      ListeningScheduledExecutorService scheduler,
      Publisher publisher,
      int publishSize,
      int publishTime) {
    this(tableName, scheduler, publisher, publishSize, publishTime, null);
  }

  /**
   * create an EventPublisher from a config.
   *
   * @param config used to configure the EventPublisher
   * @return an EventPublisher instance
   */
  public static EventPublisher create(SpezConfig config) {
    Preconditions.checkNotNull(config);
    var scheduler = UsefulExecutors.listeningScheduler();
    var publisher =
        PubSub.getPublisher(
            config.getAuth().getCredentials(),
            Options.DEFAULT(),
            config.getPubSub().getProjectId(),
            config.getPubSub().getTopic());

    var eventPublisher =
        new EventPublisher(
            config.getSink().getTable(),
            scheduler,
            publisher,
            DEFAULT_BUFFER_SIZE,
            config.getPubSub().getBufferTimeout());
    eventPublisher.start();
    return eventPublisher;
  }

  @VisibleForTesting
  void start() {
    var future =
        scheduler.scheduleAtFixedRate(
            runPublishBuffer, 0, publishBufferTime, TimeUnit.MILLISECONDS);
    ListenableFutureErrorHandler.create(
        scheduler,
        future,
        (throwable) -> {
          log.error("EventPublisher scheduled task error", throwable);
        });
  }

  @SuppressWarnings("MustBeClosedChecker")
  private ListenableFuture<String> addToBuffer(PubsubMessage message, EventState eventState) {
    SettableFuture<String> future = SettableFuture.create();
    buffer.add(new BufferPayload(OPEN_CENSUS_MESSAGE_TRANSFORM.apply(message), future, eventState));
    bufferSize.incrementAndGet();
    return future;
  }

  private void addRetryToBuffer(BufferPayload payload) {
    buffer.add(payload);
    bufferSize.incrementAndGet();
  }

  @VisibleForTesting
  void publishBuffer() {
    publishBuffer("testing", false);
  }

  void publishBufferTimeout() {
    publishSubmitted.set(true);
    publishBuffer(String.format("Scheduler timeout %d ms exceeded", publishBufferTime), true);
  }

  void publishBufferThreshold(long size) {
    publishBuffer(String.format("Buffer size %d greater than %d", size, publishBufferSize), false);
  }

  void publishBuffer(String reason, boolean timeout) {
    if (!timeout) {
      long size = bufferSize.get();
      if (size < publishBufferSize) {
        log.trace(
            "Buffer size {} too small for non-timeout publish attempt reason: {}", size, reason);
        publishSubmitted.set(false);
        return;
      }
    }
    ArrayList<BufferPayload> sink = new ArrayList<>();
    int numberDrained = buffer.drainTo(sink, MAX_PUBLISH_SIZE);
    ArrayList<PubsubMessage> messages = new ArrayList<>(numberDrained); // NOPMD
    for (var payload : sink) {
      messages.add(payload.message);
      payload.eventState.messagePublishRequested();
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

    if (timeout) {
      log.trace("{} messages drained and published for reason: {}", numberDrained, reason);
    } else {
      log.trace("{} messages drained and published for reason: {}", numberDrained, reason);
    }
    bufferSize.getAndAdd(-1 * numberDrained);

    Futures.addCallback(future, new PublishCallback(sink, tableName, failures), scheduler);
    long triggerSize = bufferSize.get();
    if (triggerSize >= MAX_PUBLISH_SIZE) {
      // We have enough messages for another batch, fire off in the same thread.
      log.trace("Triggering additional publishBuffer for size {}", triggerSize);
      publishBuffer(reason + ": triggered additional publish", false);
    }
    publishSubmitted.set(false);

    if (!failures.isEmpty()) {
      ArrayList<BufferPayload> fsink = new ArrayList<>();
      failures.drainTo(fsink, MAX_PUBLISH_SIZE);
      for (var payload : fsink) {
        if (payload.eventState.getRetryCount() >= MAX_RETRY_COUNT) {
          payload.eventState.messageRetryCountExceeded(payload.future);
        } else {
          addRetryToBuffer(payload);
        }
      }
    }
  }

  private void maybePublish(EventState eventState) {
    long size = bufferSize.get();
    eventState.queuedForPublishing(size);
    if (size >= publishBufferSize) {
      log.trace("publish buffer size {}", size);
      if (!publishSubmitted.get()) {
        publishSubmitted.set(true);
        UsefulExecutors.submit(
            scheduler,
            () -> publishBufferThreshold(size),
            (throwable) -> {
              log.error("Error while calling this::publishBuffer", throwable);
            });
      } else {
        log.trace("skipped submit publish for buffer size {}", size);
      }
    } else {
      log.trace("didn't publish buffer size {}", size);
    }
  }

  /**
   * Publishes a Bytestring and associated metadata map as a pub/sub message to the configured
   * topic.
   *
   * @param data Body of the Pub/Sub message
   * @param attrMap Attribute Map of data to be published as metadata with your message
   * @param parent propagate tracing
   */
  public ListenableFuture<String> publish(
      ByteString data, Map<String, String> attrMap, EventState eventState) {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(attrMap);
    Preconditions.checkNotNull(eventState);

    final PubsubMessage.Builder builder = PubsubMessage.newBuilder().setData(data)
        // .setOrderingKey(publisher.getTopicPath())
        ;

    String uuid = attrMap.get(SpezConfig.SINK_UUID_KEY);
    eventState.uuid(uuid);
    log.trace("Received message uuid {}", uuid);
    if (attrMap.size() > 0) {
      attrMap
          .entrySet()
          .forEach(
              e -> {
                builder.putAttributes(e.getKey(), e.getValue());
              });
    }

    ListenableFuture<String> future = addToBuffer(builder.build(), eventState);
    maybePublish(eventState);
    return future;
  }
}
