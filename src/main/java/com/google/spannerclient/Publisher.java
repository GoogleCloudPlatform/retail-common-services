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
package com.google.spannerclient;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.*;
import com.google.pubsub.v1.*;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Publisher {
  private static final Logger log = LoggerFactory.getLogger(Publisher.class);

  private static final int DEFAULT_POOL_SIZE = 4;
  private static final String DEFAULT_TARGET = "pubsub.googleapis.com";

  private final String topicPath;
  private final String topic;
  private final GoogleCredentials credentials;

  private PublisherGrpcClient stub;

  Publisher(String topicPath, GoogleCredentials credentials) {
    Preconditions.checkNotNull(topicPath);
    Preconditions.checkNotNull(credentials);

    this.topicPath = topicPath;
    this.topic = topicPath.substring(topicPath.lastIndexOf('/') + 1);
    this.credentials = credentials;
  }

  ListenableFuture<PublishResponse> publish(PublishOptions options, PublishRequest req) {
    Preconditions.checkNotNull(options);
    Preconditions.checkNotNull(req);

    final SettableFuture<PublishResponse> f = SettableFuture.create();
    final PublisherGrpcClient stub =
        new PublisherGrpcClient(Util.buildManagedChannel(DEFAULT_TARGET), credentials);

    this.stub = stub;

    ListenableFuture<PublishResponse> resp = stub.publish(Context.getDefault(), req);

    Futures.addCallback(
        resp,
        new FutureCallback<PublishResponse>() {

          @Override
          public void onSuccess(PublishResponse result) {
            log.debug("Published Record: " + result);
            f.set(result);
            try {
              stub.close();
            } catch (IOException e) {
              log.error("Error Closing Managed Channel: ", e);
            }
          }

          @Override
          public void onFailure(Throwable t) {
            f.setException(t);
            log.error("Exception Publishing Record: ", t);
            try {
              stub.close();
            } catch (IOException e) {
              log.error("Error Closing Managed Channel: ", e);
            }
          }
        },
        MoreExecutors.directExecutor());

    return f;
  }

  public String getTopicPath() {
    return topicPath;
  }

  public void close() throws IOException {
    stub.close();
  }

  // Topic??
  // public Table getTable(String tableName) {
  //   return new Table(this, tableName);
  // }

  PublisherGrpcClient stub() {
    return stub;
  }
}
