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
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.*;
import com.google.spanner.v1.*;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Database {
  private static final Logger log = LoggerFactory.getLogger(Database.class);

  private static final int DEFAULT_POOL_SIZE = 4;
  private static final String DEFAULT_TARGET = "spanner.googleapis.com";

  private final String dbPath;
  private final String dbName;
  private final Instant lastChecked;
  private final GoogleCredentials credentials;
  private final CreateSessionRequest createSessionRequest;
  private final ScheduledExecutorService scheduler;

  private GrpcClient stub;
  private ConcurrentRingBuffer<SessionContext> sessionPool;

  Database(String dbPath, GoogleCredentials credentials) {
    Preconditions.checkNotNull(dbPath);
    Preconditions.checkNotNull(credentials);

    this.dbPath = dbPath;
    this.dbName = dbPath.substring(dbPath.lastIndexOf('/') + 1);
    this.lastChecked = Instant.now();
    this.credentials = credentials;
    this.scheduler = Executors.newScheduledThreadPool(2);
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
    this.lastChecked = Instant.now();
    this.credentials = credentials;
    this.scheduler = Executors.newScheduledThreadPool(2);
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

    createSession(poolSize, f);

    return f;
  }

  public void createSession(int poolSize, SettableFuture<Database> f) {
    final List<ListenableFuture<Session>> createSessionFutureList = new ArrayList<>();

    log.info("Creating {} sessions", poolSize);
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
            // TODO(pdex): sessionList.size will always be equal to poolSize
            //             remove this check and replace with a session healthcheck
            if (sessionList.size() == poolSize) {
              // Log Prolly?

              sessionList.forEach(
                  s -> {
                    if (s == null) {
                      log.error("Session was null!");
                    } else {
                      if (!sessionPool.tryPut(new SessionContext(s))) {
                        // Log prolly?
                        log.error("Couldn't put session {}", s);
                      }
                    }
                  });
              if (f != null) {
                f.set(getClazz());
              }
            }
          }

          @Override
          public void onFailure(Throwable t) {
            if (f != null) {
              f.setException(t);
            }
          }
        },
        MoreExecutors.directExecutor());
  }

  public void close() throws IOException {
    stub.close();
  }

  void staleCheck() {
    scheduler.scheduleAtFixedRate(
        () -> {
          final Instant now = Instant.now();
          final Duration d = Duration.between(lastChecked, now);

          if (d.toMinutes() >= 30) {
            sessionPool
                .getSessionList()
                .forEach(
                    s -> {
                      Futures.addCallback(
                          stub.getSession(
                              Context.getDefault(),
                              GetSessionRequest.newBuilder().setName(s.getName()).build()),
                          new FutureCallback<Session>() {

                            @Override
                            public void onSuccess(Session s) {
                              if (!sessionPool.tryPut(new SessionContext(s))) {
                                // Log prolly?
                                log.error("Couldn't put session {}", s);
                              }
                            }

                            @Override
                            public void onFailure(Throwable t) {
                              createSession(1, null);
                              log.error("Error creating new Session:", t);
                            }
                          },
                          MoreExecutors.directExecutor());
                    });
          }
        },
        30,
        30,
        TimeUnit.MINUTES);
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
}
