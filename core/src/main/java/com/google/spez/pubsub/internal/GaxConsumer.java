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

package com.google.spez.pubsub.internal;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.google.spez.core.SpezConfig;
import com.google.spez.pubsub.PubSubConsumer;
import com.google.spez.pubsub.PubSubListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GaxConsumer implements PubSubConsumer {
  private static final Logger log = LoggerFactory.getLogger(GaxConsumer.class);

  private final Subscriber subscriber;

  private GaxConsumer(Subscriber subscriber) {
    this.subscriber = subscriber;
  }

  private static boolean subscriptionExists(
      SubscriptionAdminClient adminClient, SpezConfig.PubSubConfig config) {
    try {
      var subscriptionName =
          SubscriptionName.of(config.getProjectId(), config.getUpdaterSubscriptionName());
      var subscription = adminClient.getSubscription(subscriptionName);
      if (subscription != null) {
        return true;
      }
    } catch (Exception ex) {
    }
    return false;
  }

  private static Subscriber createSubscriber(
      SpezConfig.PubSubConfig config, PubSubListener listener) {
    MessageReceiver receiver =
        (PubsubMessage message, AckReplyConsumer consumer) -> {
          listener.onMessage(message);
          consumer.ack();
        };

    var subscriptionName =
        ProjectSubscriptionName.of(config.getProjectId(), config.getUpdaterSubscriptionName());
    Subscriber subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
    // System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
    return subscriber;
  }

  public static PubSubConsumer createConsumer(
      SpezConfig.PubSubConfig config, PubSubListener listener) {
    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
      if (!subscriptionExists(subscriptionAdminClient, config)) {
        var subscriptionName =
            SubscriptionName.of(config.getProjectId(), config.getUpdaterSubscriptionName());
        var topicName = TopicName.of(config.getProjectId(), config.getTopic());
        var subscription =
            subscriptionAdminClient.createSubscription(
                subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);
      }
      return new GaxConsumer(createSubscriber(config, listener));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void open() {
    subscriber.startAsync();
  }

  @Override
  public void close() {
    subscriber.stopAsync();
  }
}
