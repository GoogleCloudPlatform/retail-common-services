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
import com.google.spanner.v1.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

public class Database {
  private static final int DEFAULT_POOL_SIZE = 4;
  private static final String DEFAULT_TARGET = "spanner.googleapis.com";

  private final String dbPath;
  private final String dbName;
  private final GoogleCredentials credentials;
  private final CreateSessionRequest createSessionRequest;

  private GrpcClient stub;
  private ConcurrentRingBuffer<SessionContext> sessionPool;

  Database(String dbPath, GoogleCredentials credentials) {
    Preconditions.checkNotNull(dbPath);
    Preconditions.checkNotNull(credentials);

    this.dbPath = dbPath;
    this.dbName = dbPath.substring(dbPath.lastIndexOf('/') + 1);
    this.credentials = credentials;
    this.createSessionRequest = CreateSessionRequest.newBuilder().setDatabase(dbPath).build();
  }

  Database(String projectId, String instance, String database, GoogleCredentials credentials) {
    Preconditions.checkNotNull(projectId);
    Preconditions.checkNotNull(instance);
    Preconditions.checkNotNull(database);
    Preconditions.checkNotNull(credentials);

    this.dbPath =
        String.format("projects/%s/instances/%s/databases/%s", projectId, instance, database);
    this.dbName = database;
    this.credentials = credentials;
    this.createSessionRequest = CreateSessionRequest.newBuilder().setDatabase(dbPath).build();
  }

  ListenableFuture<Database> open(Options options) {
    return open(options, DEFAULT_POOL_SIZE);
  }

  ListenableFuture<Database> open(Options options, int poolSize) {
    Preconditions.checkNotNull(options);
    Preconditions.checkArgument(poolSize > 0);

    final SettableFuture<Database> f = SettableFuture.create();
    final GrpcClient stub = new GrpcClient(Util.buildManagedChannel(DEFAULT_TARGET), credentials);
    final ConcurrentRingBuffer<SessionContext> sessionPool =
        new ConcurrentRingBuffer<SessionContext>(poolSize);
    final List<ListenableFuture<Session>> createSessionFutureList = new ArrayList<>();

    this.stub = stub;
    this.sessionPool = sessionPool;

    IntStream.range(0, poolSize)
        .forEachOrdered(
            i ->
                createSessionFutureList.add(
                    stub.createSession(Context.getDefault(), createSessionRequest)));

    Futures.addCallback(
        Futures.successfulAsList(createSessionFutureList),
        new FutureCallback<List<Session>>() {
          @Override
          public void onSuccess(List<Session> sessionList) {
            if (sessionList.size() == poolSize) {
              // Log Prolly?

              sessionList.forEach(
                  s -> {
                    if (!sessionPool.tryPut(new SessionContext(s))) {
                      // Log prolly?
                    }
                  });
              f.set(getClazz());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            f.setException(t);
          }
        },
        MoreExecutors.directExecutor());

    return f;
  }

  private Database getClazz() {
    return this;
  }

  public Table getTable(String tableName) {
    return new Table(this, tableName);
  }

  public Queue getQueue(String queueName) {
    return new Queue(this, queueName);
  }

  GrpcClient stub() {
    return stub;
  }

  public ConcurrentRingBuffer<SessionContext> sessionPool() {
    return sessionPool;
  }

  public void close() throws InterruptedException {
    final CountDownLatch lock = new CountDownLatch(sessionPool.size());
    sessionPool
        .getSessionList()
        .forEach(
            s -> {
              if (s != null) {
                ListenableFuture<Empty> sf =
                    stub.deleteSession(
                        Context.getDefault(),
                        DeleteSessionRequest.newBuilder().setName(s.getName()).build());
                Futures.addCallback(
                    sf,
                    new FutureCallback<Empty>() {
                      @Override
                      public void onSuccess(@NullableDecl Empty result) {
                        lock.countDown();
                      }

                      @Override
                      public void onFailure(Throwable t) {}
                    },
                    MoreExecutors.directExecutor());
              }
            });

    lock.await(30, TimeUnit.SECONDS);
    System.out.println(" ---------------- Cleaned Up Sessions ----------------- ");
  }
}
