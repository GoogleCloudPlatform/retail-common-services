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
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.google.spannerclient.Options;
import com.google.spannerclient.PubSub;
import com.google.spannerclient.PublishOptions;
import com.google.spannerclient.Publisher;
import java.util.Map;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class published events from Cloud Spanner to Pub/Sub */
public class EventPublisher {
  private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

  private final String projectName;
  private final String topicName;

  private Publisher publisher;

  public EventPublisher(String projectName, String topicName) {
    Preconditions.checkNotNull(projectName);
    Preconditions.checkNotNull(topicName);

    this.projectName = projectName;
    this.topicName = topicName;
    this.publisher = configurePubSub(projectName, topicName);
  }

  private Publisher configurePubSub(String projectName, String topicName) {
    Preconditions.checkNotNull(projectName);
    Preconditions.checkNotNull(topicName);

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

    final PubsubMessage.Builder builder =
        PubsubMessage.newBuilder().setData(data).setOrderingKey(publisher.getTopicPath());

    attrMap
        .entrySet()
        .forEach(
            e -> {
              builder.putAttributes(e.getKey(), e.getValue());
            });

    builder.putAttributes("Timestamp", timestamp);

    ListenableFuture<PublishResponse> future =
        PubSub.publishAsync(
            PublishOptions.DEFAULT(),
            publisher,
            PublishRequest.newBuilder()
                .setTopic(publisher.getTopicPath())
                .addMessages(builder)
                .build());
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
  }
}
