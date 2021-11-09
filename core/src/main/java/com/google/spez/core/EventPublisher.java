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
import io.opencensus.metrics.data.AttachmentValue;
import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.ViewManager;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagMetadata;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tags;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
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

    PublishCallback(List<BufferPayload> sink) {
      this.sink = sink;
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
        String uuid = payload.message.getAttributesMap().get(SpezConfig.SINK_UUID_KEY);
        statsRecorder
            .newMeasureMap()
            .put(MSG_PUBLISHED, 1)
            .putAttachment("attach_uuid", AttachmentValue.AttachmentValueString.create(uuid))
            .record(
                Tags.getTagger()
                    .currentBuilder()
                    .put(
                        SpezTagging.TAILER_TABLE_KEY,
                        TagValue.create(payload.eventState.tableName),
                        TagMetadata.create(TagMetadata.TagTtl.UNLIMITED_PROPAGATION))
                    .put(
                        TAG_UUID,
                        TagValue.create(uuid),
                        TagMetadata.create(TagMetadata.TagTtl.UNLIMITED_PROPAGATION))
                    .build());
        String result;
        if (i < response.getMessageIdsCount()) {
          payload.future.set(response.getMessageIds(i));
          result = response.getMessageIds(i);
        } else {
          payload.future.set("UNAVAILABLE");
          result = "UNAVAILABLE";
        }
        log.debug("Published message uuid {}, set future to '{}'", uuid, result);
      }
    }

    @Override
    public void onFailure(Throwable t) {
      log.error("Published failed for {} messages", sink.size(), t);
      for (var payload : sink) {
        payload.future.setException(t);
      }
    }
  }

  private static final MeasureLong MSG_RECEIVED =
      MeasureLong.create("msg_received", "the number of messages received by the publisher", "");
  private static final MeasureLong MSG_PUBLISHED =
      MeasureLong.create("msg_published", "the number of messages sent by the publisher", "");
  private static final Aggregation counter = Aggregation.Count.create();
  private static final TagKey TAG_UUID = TagKey.create("tag_uuid");

  private static void setupViews() {
    ViewManager viewManager = Stats.getViewManager();
    viewManager.registerView(
        View.create(
            SpezMetrics.MSG_RECEIVED_VIEW_NAME,
            "The count of messages received by the publisher",
            MSG_RECEIVED,
            counter,
            Arrays.asList(SpezTagging.TAILER_TABLE_KEY)));
    viewManager.registerView(
        View.create(
            SpezMetrics.MSG_PUBLISHED_VIEW_NAME,
            "The count of messages sent by the publisher",
            MSG_PUBLISHED,
            counter,
            Arrays.asList(SpezTagging.TAILER_TABLE_KEY)));
  }

  static {
    setupViews();
  }

  private static final StatsRecorder statsRecorder = Stats.getStatsRecorder();

  private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);
  private static final int DEFAULT_BUFFER_SIZE = 950; // TODO(pdex): move to config
  /**
   * PubSub has a limit of 1000 messages per PublishRequest. We will batch up at most
   * MAX_PUBLISH_SIZE messages for each request. This number should be less than 1000 base on
   * experience with PubSub grumpiness.
   */
  private static final int MAX_PUBLISH_SIZE = 950;

  private static final Tracer tracer = Tracing.getTracer();

  private final LinkedTransferQueue<BufferPayload> buffer = new LinkedTransferQueue<>();
  @VisibleForTesting final AtomicLong bufferSize = new AtomicLong(0);

  private final ListeningScheduledExecutorService scheduler;
  private final int publishBufferSize;
  private final int publishBufferTime;

  private final Publisher publisher;
  @VisibleForTesting final Runnable runPublishBuffer;

  /**
   * constructor visible for testing.
   *
   * @param scheduler scheduler service
   * @param publisher publisher
   * @param publishSize publish size
   * @param publishTime publish time
   * @param runPublishBuffer runnable to perform publishing
   */
  @VisibleForTesting
  public EventPublisher(
      ListeningScheduledExecutorService scheduler,
      Publisher publisher,
      int publishSize,
      int publishTime,
      Runnable runPublishBuffer) {
    Preconditions.checkNotNull(scheduler, "scheduler must not be null");
    Preconditions.checkNotNull(publisher, "publisher must not be null");
    Preconditions.checkArgument(publishSize >= 0, "publishSize must be greater than or equal to 0");
    Preconditions.checkArgument(publishTime >= 0, "publishTime must be greater than or equal to 0");
    this.scheduler = scheduler;
    this.publisher = publisher;
    if (runPublishBuffer != null) {
      this.runPublishBuffer = runPublishBuffer;
    } else {
      this.runPublishBuffer = this::publishBuffer;
    }
    this.publishBufferSize = publishSize;
    this.publishBufferTime = publishTime;
  }

  public EventPublisher(
      ListeningScheduledExecutorService scheduler,
      Publisher publisher,
      int publishSize,
      int publishTime) {
    this(scheduler, publisher, publishSize, publishTime, null);
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
            scheduler, publisher, DEFAULT_BUFFER_SIZE, config.getPubSub().getBufferTimeout());
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
    Futures.addCallback(
        future,
        new FutureCallback<String>() {

          @Override
          public void onSuccess(String result) {
            eventState.messagePublished(result);
          }

          @Override
          public void onFailure(Throwable t) {
            // TODO(pdex): eventState needs to handle publish error
          }
        },
        scheduler);
    return future;
  }

  @VisibleForTesting
  void publishBuffer() {
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

    log.debug("{} messages drained and published", numberDrained);
    bufferSize.getAndAdd(-1 * numberDrained);

    Futures.addCallback(future, new PublishCallback(sink), scheduler);
    if (bufferSize.get() >= MAX_PUBLISH_SIZE) {
      // We have enough messages for another batch, fire off in the same thread.
      publishBuffer();
    }
  }

  private void maybePublish(EventState eventState) {
    long size = bufferSize.get();
    eventState.queuedForPublishing(size);
    if (size >= publishBufferSize) {
      log.debug("publish buffer size {}", size);
      UsefulExecutors.submit(
          scheduler,
          runPublishBuffer,
          (throwable) -> {
            log.error("Error while calling this::publishBuffer", throwable);
          });
    } else {
      log.debug("didn't publish buffer size {}", size);
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
    statsRecorder
        .newMeasureMap()
        .put(MSG_RECEIVED, 1)
        .putAttachment("attach_uuid", AttachmentValue.AttachmentValueString.create(uuid))
        .record(
            Tags.getTagger()
                .currentBuilder()
                .put(
                    SpezTagging.TAILER_TABLE_KEY,
                    TagValue.create(eventState.tableName),
                    TagMetadata.create(TagMetadata.TagTtl.UNLIMITED_PROPAGATION))
                .put(
                    TAG_UUID,
                    TagValue.create(uuid),
                    TagMetadata.create(TagMetadata.TagTtl.UNLIMITED_PROPAGATION))
                .build());
    log.debug("Received message uuid {}", uuid);
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
