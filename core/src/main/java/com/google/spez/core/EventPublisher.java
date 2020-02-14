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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AsyncFunction;
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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class published events from Cloud Spanner to Pub/Sub */
public class EventPublisher {
  private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);
  private static final int DEFAULT_BUFFER_SIZE = 950;
  private static final int DEFAULT_BUFFER_TIME = 30;

  private final List<PubsubMessage> buffer;
  private final ScheduledExecutorService scheduler;
  private final String projectName;
  private final String topicName;
  private final int bufferSize;
  private final int bufferTime;

  private Publisher publisher;
  private Instant lastPublished;

  public EventPublisher(String projectName, String topicName) {
    Preconditions.checkNotNull(projectName);
    Preconditions.checkNotNull(topicName);

    this.buffer = new ArrayList<>();
    this.scheduler = Executors.newScheduledThreadPool(2);
    this.projectName = projectName;
    this.topicName = topicName;
    this.bufferSize = DEFAULT_BUFFER_SIZE;
    this.bufferTime = DEFAULT_BUFFER_TIME;
    this.publisher = configurePubSub(projectName, topicName);
    this.lastPublished = Instant.now();
  }

  public EventPublisher(String projectName, String topicName, int bufferSize, int bufferTime) {
    Preconditions.checkNotNull(projectName);
    Preconditions.checkNotNull(topicName);
    Preconditions.checkArgument(bufferSize > 0);
    Preconditions.checkArgument(bufferTime > 0);

    this.buffer = new ArrayList<>();
    this.scheduler = Executors.newScheduledThreadPool(2);
    this.projectName = projectName;
    this.topicName = topicName;
    this.bufferSize = bufferSize;
    this.bufferTime = bufferTime;
    this.publisher = configurePubSub(projectName, topicName);
    this.lastPublished = Instant.now();
  }

  private Publisher configurePubSub(String projectName, String topicName) {
    Preconditions.checkNotNull(projectName);
    Preconditions.checkNotNull(topicName);

    scheduler.scheduleAtFixedRate(
        () -> {
          final Instant now = Instant.now();
          final Duration d = Duration.between(lastPublished, now);

          if (buffer.size() > 0 && d.getSeconds() > bufferTime) {
            final ListenableFuture<PublishResponse> future =
                PubSub.publishAsync(
                    PublishOptions.DEFAULT(),
                    publisher,
                    PublishRequest.newBuilder()
                        .setTopic(publisher.getTopicPath())
                        .addAllMessages(buffer)
                        .build());

            lastPublished = Instant.now();
            buffer.clear();
          }
        },
        30,
        30,
        TimeUnit.SECONDS);

    return PubSub.getPublisher(Options.DEFAULT(), projectName, topicName);
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
      ByteString data, Map<String, String> attrMap, String timestamp, Executor executor) {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(attrMap);
    Preconditions.checkNotNull(timestamp);
    Preconditions.checkNotNull(executor);
    final Instant now = Instant.now();
    final Duration d = Duration.between(lastPublished, now);

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

    builder.putAttributes("Timestamp", timestamp);
    buffer.add(builder.build());

    if (buffer.size() >= bufferSize || d.getSeconds() > bufferTime) {
      final ListenableFuture<PublishResponse> future =
          PubSub.publishAsync(
              PublishOptions.DEFAULT(),
              publisher,
              PublishRequest.newBuilder()
                  .setTopic(publisher.getTopicPath())
                  .addAllMessages(buffer)
                  .build());

      lastPublished = Instant.now();
      buffer.clear();

      AsyncFunction<PublishResponse, String> getMessageId =
          new AsyncFunction<>() {
            public ListenableFuture<String> apply(PublishResponse response) {
              if (response.getMessageIdsCount() > 0) {
                return Futures.immediateFuture(response.getMessageIds(0));
              }
              return Futures.immediateFuture("");
            }
          };

      return Futures.transformAsync(future, getMessageId, executor);

    } else {
      final SettableFuture<String> r = SettableFuture.create();
      r.set("BUFFERED");

      return r;
    }
  }
}
