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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureToListenableFuture;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/** This class published events from Cloud Spanner to Pub/Sub. */
@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class GaxEventPublisher {
  private static final Logger log = LoggerFactory.getLogger(GaxEventPublisher.class);

  private final Publisher publisher;

  /**
   * Constructor.
   *
   * @param projectId project id to publish to
   * @param topic topic to publish to
   */
  public GaxEventPublisher(String projectId, String topic) {
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
            .setInitialRpcTimeout(Duration.ofSeconds(10L)) // TODO(pdex): move to config
            .setMaxRpcTimeout(Duration.ofSeconds(20L)) // TODO(pdex): move to config
            .setMaxAttempts(5) // TODO(pdex): move to config
            .setTotalTimeout(Duration.ofSeconds(30L)) // TODO(pdex): move to config
            .build();
    final BatchingSettings batchingSettings =
        BatchingSettings.newBuilder()
            .setElementCountThreshold(100L) // TODO(pdex): move to config
            .setDelayThreshold(Duration.ofSeconds(30)) // TODO(pdex): move to config
            .setRequestByteThreshold(10 * 1024L) // TODO(pdex): move to config
            .setIsEnabled(true) // TODO(pdex): move to config
            .build();

    final ProjectTopicName topicName = ProjectTopicName.of(projectId, topic);
    try {
      final Publisher publisher =
          Publisher.newBuilder(topicName)
              // TODO(XJDR): Determine if this transform is appropriate
              // .setTransform(OpenCensusUtil.OPEN_CENSUS_MESSAGE_TRANSFORM)
              .setRetrySettings(retrySettings)
              .setBatchingSettings(batchingSettings)
              .build();
      return publisher;
    } catch (IOException e) {
      log.error("Was not able to create a publisher for topic: " + topicName, e);
      throw new RuntimeException(e);
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
      ByteString data, Map<String, String> attrMap, String timestamp, Executor executor) {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(attrMap);
    Preconditions.checkNotNull(timestamp);
    Preconditions.checkNotNull(executor);

    PubsubMessage.Builder messageBuilder = PubsubMessage.newBuilder().setData(data);

    messageBuilder.putAttributes("Timestamp", timestamp);

    attrMap
        .entrySet()
        .forEach(
            e -> {
              messageBuilder.putAttributes(e.getKey(), e.getValue());
            });

    ApiFuture<String> future = publisher.publish(messageBuilder.build());
    // TODO(pdex): use the executor to transform the ApiFuture?
    return new ApiFutureToListenableFuture<String>(future);
  }

  /**
   * extract a string from a throwable.
   *
   * @param throwable throwable to extract from
   * @return a human readable error message
   */
  public String extractError(Throwable throwable) {
    if (throwable instanceof ApiException) {
      ApiException apiException = ((ApiException) throwable);
      return String.format(
          "error code '%s' retryable? '%s'",
          apiException.getStatusCode().getCode(), apiException.isRetryable());
    }
    return throwable.getMessage();
  }
}
