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
import com.google.common.collect.ImmutableList;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class published events from Cloud Spanner to Pub/Sub */
public class EventPublisher {
  private static final Logger log = LoggerFactory.getLogger(SpannerTailer.class);

  private final String projectName;
  private final String topicName;

  private Publisher publisher;

  private final List<PubsubMessage> messages = new ArrayList<>();

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

    // TODO(xjdr): Don't do this
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
  public ListenableFuture<PublishResponse> publish(
      ImmutableList<ByteString> datas, Map<String, String> attrMap, String timestamp) {
    Preconditions.checkNotNull(datas);
    Preconditions.checkNotNull(attrMap);
    Preconditions.checkNotNull(timestamp);

    PubsubMessage.Builder[] builder = new PubsubMessage.Builder[1];
    SettableFuture<PublishResponse> r = SettableFuture.create();
    for (final ByteString data : datas) {
      //      final PubsubMessage.Builder builder =
      builder[0] =
          PubsubMessage.newBuilder().setData(data).setOrderingKey(publisher.getTopicPath());

      attrMap
          .entrySet()
          .forEach(
              e -> {
                builder[0].putAttributes(e.getKey(), e.getValue());
              });

      builder[0].putAttributes("Timestamp", timestamp);

      //      messages.add(builder.build());
    }

    messages.add(builder[0].build());

    if (messages.size() >= 950) {
      r.setFuture(
          PubSub.publishAsync(
              PublishOptions.DEFAULT(),
              publisher,
              PublishRequest.newBuilder()
                  .setTopic(publisher.getTopicPath())
                  .addAllMessages(messages)
                  .build()));

      messages.clear();
    } else {
      r.set(PublishResponse.newBuilder().build());
    }

    // else {
    //  messages.add(builder[0].build());
    // }

    // ListenableFuture<PublishResponse> resp =
    //     PubSub.publishAsync(
    //         PublishOptions.DEFAULT(),
    //         publisher,
    //         PublishRequest.newBuilder()
    //             .setTopic(publisher.getTopicPath())
    //             .addAllMessages(messages)
    //             .build());

    return r;
  }
}
