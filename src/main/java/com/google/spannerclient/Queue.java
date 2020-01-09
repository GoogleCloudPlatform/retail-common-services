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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.spanner.v1.*;
import java.util.List;
import java.util.Optional;

public class Queue {
  private final Database database;
  private final String queueName;

  Queue(Database database, String queueName) {
    this.database = database;
    this.queueName = queueName;
  }

  public RowCursor query(String sql) {
    return null;
  }

  public ListenableFuture<Queue> createQueue(Database db, String queueName) {
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(queueName);

    final SettableFuture<Queue> returnFuture = SettableFuture.create();

    final String queueSql =
        "CREATE TABLE "
            + queueName
            + " ("
            + "  MessageId    INT64 NOT NULL,"
            + "  Offset       INT64 NOT NULL,"
            + "  Metadata     String(MAX)," // Format will be "Key1:Val1#Key2:Val2#
            + "  Data         Bytes(MAX) NOT NULL,"
            + "  Timestamp    TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)"
            + ") PRIMARY KEY (MessageId)";

    final Optional<SessionContext> sessionOptional = db.sessionPool().tryGet();
    final SettableFuture<Queue> qTableFuture = SettableFuture.create();

    if (sessionOptional.isPresent()) {
      final SessionContext session = sessionOptional.get();
      if (session.lock()) {

        ListenableFuture<ResultSet> resultSetListenableFuture =
            db.stub()
                .executeSql(
                    Context.getDefault(),
                    ExecuteSqlRequest.newBuilder()
                        .setSession(session.getName())
                        .setSql(queueSql)
                        .build());
        Futures.addCallback(
            resultSetListenableFuture,
            new FutureCallback<ResultSet>() {

              @Override
              public void onSuccess(ResultSet resultSet) {
                qTableFuture.set(getClazz());
                session.unlock();
              }

              @Override
              public void onFailure(Throwable t) {
                qTableFuture.setException(t);
                session.unlock();
              }
            },
            MoreExecutors.directExecutor());

      } else {
        // throw prolly
      }
    }

    final String subscriptionsSql =
        "CREATE TABLE "
            + queueName
            + "Subscriptions"
            + " ("
            + "  SubscriptionId      INT64 NOT NULL,"
            + "  LastProcessedOffset INT64 NOT NULL,"
            + "  Metadata            String(MAX)," // Format will be "Key1:Val1#Key2:Val2#
            + "  LastAckedTimestamp  TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)"
            + ") PRIMARY KEY (SubscriptionId)";

    final Optional<SessionContext> sessionOptional_ = db.sessionPool().tryGet();
    final SettableFuture<Queue> subsTableFuture = SettableFuture.create();

    if (sessionOptional.isPresent()) {
      final SessionContext session = sessionOptional.get();
      if (session.lock()) {

        ListenableFuture<ResultSet> resultSetListenableFuture =
            db.stub()
                .executeSql(
                    Context.getDefault(),
                    ExecuteSqlRequest.newBuilder()
                        .setSession(session.getName())
                        .setSql(subscriptionsSql)
                        .build());
        Futures.addCallback(
            resultSetListenableFuture,
            new FutureCallback<ResultSet>() {

              @Override
              public void onSuccess(ResultSet resultSet) {
                subsTableFuture.set(getClazz());
                session.unlock();
              }

              @Override
              public void onFailure(Throwable t) {
                subsTableFuture.setException(t);
                session.unlock();
              }
            },
            MoreExecutors.directExecutor());

      } else {
        // throw prolly
      }
    }

    List<ListenableFuture<Queue>> qList = ImmutableList.of(qTableFuture, subsTableFuture);
    Futures.addCallback(
        Futures.successfulAsList(qList),
        new FutureCallback<List<Queue>>() {

          @Override
          public void onSuccess(List<Queue> result) {
            returnFuture.set(result.get(1));
          }

          @Override
          public void onFailure(Throwable t) {
            returnFuture.setException(t);
          }
        },
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  public long createSubscription(String topic) {
    return 0; // returns subscriptionId
  }

  public boolean send(String topic, String messageKey, String data) {
    return true;
  }

  public boolean recieve(
      String topic, long subscriptionId, ReceiveOptions receiveOptions, MessageCallback handler) {
    return true;
  }

  public Database getDatabase() {
    return database;
  }

  public String getQueueName() {
    return queueName;
  }

  private Queue getClazz() {
    return this;
  }
}
