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
package com.google.spannerclient;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.*;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSub {
  private static final Logger log = LoggerFactory.getLogger(PubSub.class);

  private PubSub() {}

  public static Publisher getPublisher(
      GoogleCredentials credentials, Options options, String projectName, String topicName) {
    Preconditions.checkNotNull(credentials);
    Preconditions.checkNotNull(projectName);
    Preconditions.checkNotNull(topicName);

    final String topicPath = String.format("projects/%s/topics/%s", projectName, topicName);
    return new Publisher(topicPath, credentials);
  }

  public static PublishResponse publish(
      PublishOptions options, Publisher publisher, PublishRequest req) {
    final CountDownLatch doneSignal = new CountDownLatch(1);
    final ListenableFuture<PublishResponse> executeFuture = publishAsync(options, publisher, req);
    final PublishResponse[] resp = new PublishResponse[1];

    Futures.addCallback(
        executeFuture,
        new FutureCallback<PublishResponse>() {

          @Override
          public void onSuccess(PublishResponse response) {
            if (response != null) {
              resp[0] = response;
            }
            doneSignal.countDown();
          }

          @Override
          public void onFailure(Throwable t) {
            // Prolly Log or throw maybe?
            doneSignal.countDown();
          }
        },
        MoreExecutors.directExecutor());

    try {
      doneSignal.await();
    } catch (InterruptedException e) {
      // Prolly Wait or Throw
    }

    if (resp[0] != null) {
      return resp[0];
    } else {
      // be better than this
      return null;
    }
  }

  public static ListenableFuture<PublishResponse> publishAsync(
      PublishOptions options, Publisher publisher, PublishRequest req) {
    return publisher.publish(options, req);
  }
}
