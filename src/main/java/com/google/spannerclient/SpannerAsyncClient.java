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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.*;
import com.google.protobuf.*;
import com.google.spanner.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

public class SpannerAsyncClient {
  private static final int DEFAULT_POOL_SIZE = 20;
  private static final int DEFAULT_POLLER_SIZE = 4;
  private static final String DEFAULT_TARGET = "spanner.googleapis.com";

  private final ManagedChannel channel;
  private final GrpcClient client;
  private final SessionPool sessionPool;
  private final CreateSessionRequest createSessionRequest;
  private final BatchCreateSessionsRequest batchCreateSessionRequest;
  private final ScheduledExecutorService scheduler;
  private final ScheduledFuture<?> poller;

  public SpannerAsyncClient(String database, GoogleCredentials credentials) {
    Preconditions.checkNotNull(database);
    Preconditions.checkNotNull(credentials);

    this.channel = buildChannel();
    this.client = new GrpcClient(channel, credentials);
    this.sessionPool = new SessionPool(DEFAULT_POOL_SIZE);
    this.scheduler = Executors.newScheduledThreadPool(DEFAULT_POLLER_SIZE);
    this.createSessionRequest = CreateSessionRequest.newBuilder().setDatabase(database).build();
    this.batchCreateSessionRequest =
        BatchCreateSessionsRequest.newBuilder()
            .setDatabase(database)
            .setSessionCount(DEFAULT_POOL_SIZE)
            .build();

    buildSessionPool();
    this.poller = startSessionPoller();
  }

  public SpannerAsyncClient(String database, GoogleCredentials credentials, int poolSize) {
    Preconditions.checkNotNull(database);
    Preconditions.checkNotNull(credentials);
    Preconditions.checkArgument(poolSize > 0);

    this.channel = buildChannel();
    this.client = new GrpcClient(channel, credentials);
    this.sessionPool = new SessionPool(poolSize);
    this.scheduler = Executors.newScheduledThreadPool(DEFAULT_POLLER_SIZE);
    this.createSessionRequest = CreateSessionRequest.newBuilder().setDatabase(database).build();
    this.batchCreateSessionRequest =
        BatchCreateSessionsRequest.newBuilder()
            .setDatabase(database)
            .setSessionCount(poolSize)
            .build();

    buildSessionPool();
    this.poller = startSessionPoller();
  }

  private ManagedChannel buildChannel() {
    try {
      return NettyChannelBuilder.forTarget(DEFAULT_TARGET)
          .sslContext(GrpcSslContexts.forClient().build())
          .build();
    } catch (SSLException e) {
      // TODO(xjdr): Do something better here
      e.printStackTrace();
    }

    return null;
  }

  private void buildSessionPool() {
    final CountDownLatch lock = new CountDownLatch(sessionPool.getMaxPoolSize());

    ListenableFuture<BatchCreateSessionsResponse> batchCreateSessionsResponseFuture =
        client.batchCreateSession(Context.getDefault(), batchCreateSessionRequest);

    Futures.addCallback(
        batchCreateSessionsResponseFuture,
        new FutureCallback<BatchCreateSessionsResponse>() {
          @Override
          public void onSuccess(
              @NullableDecl BatchCreateSessionsResponse batchCreateSessionsResponse) {
            Preconditions.checkNotNull(batchCreateSessionsResponse);

            batchCreateSessionsResponse.getSessionList().forEach(sessionPool::addSession);
            lock.countDown();
          }

          @Override
          public void onFailure(Throwable t) {
            // Error
            lock.countDown();
          }
        },
        MoreExecutors.directExecutor());
    try {
      lock.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // TODO(xjdr): Do something better here
      e.printStackTrace();
    }
  }

  ScheduledFuture<?> startSessionPoller() {
    return scheduler.scheduleAtFixedRate(
            () -> {
              List<Session> pendingSessions = new ArrayList<>();
              sessionPool
                  .getSessionList()
                  .forEach(
                      session -> {
                        pendingSessions.add(session);
                        ListenableFuture<Session> getSessionFuture =
                            client.getSession(
                                Context.getDefault(),
                                GetSessionRequest.newBuilder().setName(session.getName()).build());
                        Futures.addCallback(
                            getSessionFuture,
                            new FutureCallback<Session>() {
                              @Override
                              public void onSuccess(@Nullable Session session_) {
                                pendingSessions.remove(session_);
                              }

                              @Override
                              public void onFailure(Throwable t) {}
                            },
                            MoreExecutors.directExecutor());
                      });

              pendingSessions.forEach(
                  s -> {
                    sessionPool.removeSession(s);
                    ListenableFuture<Session> sf =
                        client.createSession(Context.getDefault(), createSessionRequest);
                    Futures.addCallback(
                        sf,
                        new FutureCallback<Session>() {
                          @Override
                          public void onSuccess(@Nullable Session session) {
                            sessionPool.addSession(session);
                          }

                          @Override
                          public void onFailure(Throwable t) {}
                        },
                        MoreExecutors.directExecutor());
                  });
            },
            30,
            30,
            TimeUnit.SECONDS);
  }

  ListenableFuture<RowCursor> executeSql(String sql) {
    Preconditions.checkNotNull(sql);

    final Optional<Session> sessionOptional = sessionPool.getSession();
    final SettableFuture<RowCursor> resultSetFuture = SettableFuture.create();

    if (sessionOptional.isPresent()) {
      final Session session = sessionOptional.get();
      ListenableFuture<ResultSet> resultSetListenableFuture =
          client.executeSql(
              Context.getDefault(),
              ExecuteSqlRequest.newBuilder().setSession(session.getName()).setSql(sql).build());
      Futures.addCallback(
          resultSetListenableFuture,
          new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(@NullableDecl ResultSet resultSet) {
              final ImmutableList<StructType.Field> fieldList =
                  ImmutableList.copyOf(resultSet.getMetadata().getRowType().getFieldsList());
              final ImmutableList<ListValue> rowList =
                  ImmutableList.copyOf(resultSet.getRowsList());
              final RowCursor rowCursor = RowCursor.of(fieldList, rowList);

              resultSetFuture.set(rowCursor);
              sessionPool.addSession(session);
            }

            @Override
            public void onFailure(Throwable t) {
              resultSetFuture.setException(t);
              sessionPool.addSession(session);
            }
          },
          MoreExecutors.directExecutor());
    }

    return resultSetFuture;
  }

  ListenableFuture<RowCursor> executeSqlReadOnlyStrong(String sql) {
    Preconditions.checkNotNull(sql);

    final Optional<Session> sessionOptional = sessionPool.getSession();
    final SettableFuture<RowCursor> resultSetFuture = SettableFuture.create();

    if (sessionOptional.isPresent()) {
      final Session session = sessionOptional.get();
      ListenableFuture<ResultSet> resultSetListenableFuture =
          client.executeSql(
              Context.getDefault(),
              ExecuteSqlRequest.newBuilder()
                  .setSession(session.getName())
                  .setSql(sql)
                  .setTransaction(
                      TransactionSelector.newBuilder()
                          .setSingleUse(
                              TransactionOptions.newBuilder()
                                  .setReadOnly(
                                      TransactionOptions.ReadOnly.newBuilder().setStrong(true))
                                  .build())
                          .build())
                  .build());

      Futures.addCallback(
          resultSetListenableFuture,
          new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(@NullableDecl ResultSet resultSet) {
              final ImmutableList<StructType.Field> fieldList =
                  ImmutableList.copyOf(resultSet.getMetadata().getRowType().getFieldsList());
              final ImmutableList<ListValue> rowList =
                  ImmutableList.copyOf(resultSet.getRowsList());
              final RowCursor rowCursor = RowCursor.of(fieldList, rowList);

              resultSetFuture.set(rowCursor);
              sessionPool.addSession(session);
            }

            @Override
            public void onFailure(Throwable t) {
              resultSetFuture.setException(t);
              sessionPool.addSession(session);
            }
          },
          MoreExecutors.directExecutor());
    }

    return resultSetFuture;
  }

  void executeStreamingSql(String sql, SpannerStreamingHandler handler) {
    Preconditions.checkNotNull(sql);

    final Optional<Session> sessionOptional = sessionPool.getSession();

    if (sessionOptional.isPresent()) {
      final Session session = sessionOptional.get();
      client.executeStreamingSql(
          Context.getDefault(),
          ExecuteSqlRequest.newBuilder().setSession(session.getName()).setSql(sql).build(),
          new StreamObserver<PartialResultSet>() {
            @Override
            public void onNext(PartialResultSet value) {
              final ImmutableList<StructType.Field> fieldList =
                  ImmutableList.copyOf(value.getMetadata().getRowType().getFieldsList());
              final ImmutableList<Value> values = ImmutableList.copyOf(value.getValuesList());
              if (value.getChunkedValue()) {
                // Build proper values
              }

              handler.apply(Row.of(fieldList, values));
            }

            @Override
            public void onError(Throwable t) {
              sessionPool.addSession(session);
            }

            @Override
            public void onCompleted() {
              sessionPool.addSession(session);
            }
          });
    }
  }

  void executeStreamingSqlReadOnlyStrong(String sql, SpannerStreamingHandler handler) {
    Preconditions.checkNotNull(sql);

    final Optional<Session> sessionOptional = sessionPool.getSession();

    if (sessionOptional.isPresent()) {
      final Session session = sessionOptional.get();
      client.executeStreamingSql(
          Context.getDefault(),
          ExecuteSqlRequest.newBuilder()
              .setSession(session.getName())
              .setSql(sql)
              .setTransaction(
                  TransactionSelector.newBuilder()
                      .setSingleUse(
                          TransactionOptions.newBuilder()
                              .setReadOnly(TransactionOptions.ReadOnly.newBuilder().setStrong(true))
                              .build())
                      .build())
              .build(),
          new StreamObserver<PartialResultSet>() {
            @Override
            public void onNext(PartialResultSet value) {
              final ImmutableList<StructType.Field> fieldList =
                  ImmutableList.copyOf(value.getMetadata().getRowType().getFieldsList());
              final ImmutableList<Value> values = ImmutableList.copyOf(value.getValuesList());
              if (value.getChunkedValue()) {
                // Build proper values
              }

              // This should probably be wrapped in an executor
              handler.apply(Row.of(fieldList, values));
            }

            @Override
            public void onError(Throwable t) {
              sessionPool.addSession(session);
            }

            @Override
            public void onCompleted() {
              sessionPool.addSession(session);
            }
          });
    }
  }

  void close() throws InterruptedException {
    poller.cancel(true);
    scheduler.shutdownNow();
    final CountDownLatch lock = new CountDownLatch(sessionPool.getMaxPoolSize());
    sessionPool
        .getSessionList()
        .forEach(
            s -> {
              ListenableFuture<Empty> sf =
                  client.deleteSession(
                      Context.getDefault(),
                      DeleteSessionRequest.newBuilder().setName(s.getName()).build());
              Futures.addCallback(
                  sf,
                  new FutureCallback<Empty>() {
                    @Override
                    public void onSuccess(@NullableDecl Empty result) {
                      sessionPool.removeSession(s);
                      lock.countDown();
                    }

                    @Override
                    public void onFailure(Throwable t) {}
                  },
                  MoreExecutors.directExecutor());
            });

    lock.await(30, TimeUnit.SECONDS);
    System.out.println(" ---------------- Cleaned Up Sessions ----------------- ");
  }
}
