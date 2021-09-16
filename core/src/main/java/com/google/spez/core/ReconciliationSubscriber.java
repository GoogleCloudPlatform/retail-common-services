package com.google.spez.core;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;


/**
 *
 */
public class ReconciliationSubscriber {
  private final SpezConfig.PubSubConfig config;
  private final Reconciler reconciler;
  private final ProjectSubscriptionName subscription;
  private final Subscriber subscriber;

  ReconciliationSubscriber(SpezConfig.PubSubConfig config, Reconciler reconciler) {
    this.config = config;
    this.reconciler = reconciler;
    this.subscription = ProjectSubscriptionName.of(config.getProjectId(), config.getSubscription());
    this.subscriber = Subscriber.newBuilder(subscription, this::receiveMessage).build();
  }

  public void start() {
    subscriber.startAsync().awaitRunning();
  }

  public void stop() {
    subscriber.stopAsync().awaitTerminated();
  }

  public void receiveMessage(final PubsubMessage message, final AckReplyConsumer consumer) {
    var messageId = message.getMessageId();
    var commitTimestamp = message.getAttributesMap().get(SpezConfig.SINK_TIMESTAMP_KEY);
    var uuid = message.getAttributesMap().get(SpezConfig.SINK_UUID_KEY);
    reconciler.onReceive(messageId, commitTimestamp, uuid);
    consumer.ack();
  }

  public void publishMessage(String messageId, String commitTimestamp, String uuid) {
    reconciler.onPublish(messageId, commitTimestamp, uuid);
  }
}
