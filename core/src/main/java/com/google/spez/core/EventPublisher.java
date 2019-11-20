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
package com.google.spez.core;

import com.google.api.core.ApiFuture;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/** This class published events from Cloud Spanner to Pub/Sub */
public class EventPublisher {
  private static final Logger log = LoggerFactory.getLogger(SpannerTailer.class);

  private final Publisher publisher;

  public EventPublisher(String projectId, String topic) {
    Preconditions.checkNotNull(projectId);
    Preconditions.checkNotNull(topic);

    this.publisher = configurePubSub(projectId, topic);
  }

  private Publisher configurePubSub(String projectId, String topic) {
    Preconditions.checkNotNull(projectId);
    Preconditions.checkNotNull(topic);

    // TODO(XJDR): Convert these values to a config
    final RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setInitialRpcTimeout(Duration.ofSeconds(10L))
            .setMaxRpcTimeout(Duration.ofSeconds(20L))
            .setMaxAttempts(5)
            .setTotalTimeout(Duration.ofSeconds(30L))
            .build();

    final ProjectTopicName topicName = ProjectTopicName.of(projectId, topic);
    try {
      final Publisher publisher =
          Publisher.newBuilder(topicName)
              // TODO(XJDR): Determine if this transform is appropriate
              // .setTransform(OpenCensusUtil.OPEN_CENSUS_MESSAGE_TRANSFORM)
              .setRetrySettings(retrySettings)
              .build();
      return publisher;
    } catch (IOException e) {
      log.error("Was not able to create a publisher for topic: " + topicName, e);
      System.exit(1);
    }

    // TODO(xjdr): Don't do this
    return null;
  }

  /**
   * Publishes a Bytestring and associated metadata map as a pub/sub message to the configured
   * topic.
   *
   * @param data Body of the Pub/Sub message
   * @param attrMap Attribute Map of data to be published as metadata with your message
   * @param timestamp the Commit Timestamp of the Spanner Record to be published
   */
  public List<ApiFuture<String>> publish(
      ByteString data, Map<String, String> attrMap, String timestamp) {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(attrMap);

    final List<ApiFuture<String>> pubSubFutureList = new ArrayList<>();

    PubsubMessage.Builder messageBuilder = PubsubMessage.newBuilder().setData(data);

    messageBuilder.putAttributes("Timestamp", timestamp);

    attrMap
        .entrySet()
        .forEach(
            e -> {
              messageBuilder.putAttributes(e.getKey(), e.getValue());
            });

    final ApiFuture<String> pubSubFuture = publisher.publish(messageBuilder.build());

    pubSubFutureList.add(pubSubFuture);

    return pubSubFutureList;
  }
}
