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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.*;
import com.google.protobuf.*;
import com.google.spanner.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  //  private final SessionPool sessionPool;
  private final ConcurrentRingBuffer<SessionContext> sessionPool;
  private final CreateSessionRequest createSessionRequest;
  private final BatchCreateSessionsRequest batchCreateSessionRequest;
  private final ScheduledExecutorService scheduler;
  private final ScheduledFuture<?> poller;
  private final Map<Session, SessionContext> expiredSessionMap;

  public SpannerAsyncClient(String database, GoogleCredentials credentials) {
    Preconditions.checkNotNull(database);
    Preconditions.checkNotNull(credentials);

    this.channel = buildChannel();
    this.client = new GrpcClient(channel, credentials);
    //    this.sessionPool = new SessionPool(DEFAULT_POOL_SIZE);
    this.sessionPool = new ConcurrentRingBuffer<SessionContext>(DEFAULT_POOL_SIZE);
    this.scheduler = Executors.newScheduledThreadPool(DEFAULT_POLLER_SIZE);
    this.createSessionRequest = CreateSessionRequest.newBuilder().setDatabase(database).build();
    this.batchCreateSessionRequest =
        BatchCreateSessionsRequest.newBuilder()
            .setDatabase(database)
            .setSessionCount(DEFAULT_POOL_SIZE)
            .build();

    buildSessionPool();
    this.poller = startSessionPoller();
    this.expiredSessionMap = new ConcurrentHashMap<>();
  }

  public SpannerAsyncClient(String database, GoogleCredentials credentials, int poolSize) {
    Preconditions.checkNotNull(database);
    Preconditions.checkNotNull(credentials);
    Preconditions.checkArgument(poolSize > 0);

    this.channel = buildChannel();
    this.client = new GrpcClient(channel, credentials);
    //    this.sessionPool = new SessionPool(poolSize);
    this.sessionPool = new ConcurrentRingBuffer<SessionContext>(DEFAULT_POOL_SIZE);
    this.scheduler = Executors.newScheduledThreadPool(DEFAULT_POLLER_SIZE);
    this.createSessionRequest = CreateSessionRequest.newBuilder().setDatabase(database).build();
    this.batchCreateSessionRequest =
        BatchCreateSessionsRequest.newBuilder()
            .setDatabase(database)
            .setSessionCount(poolSize)
            .build();

    buildSessionPool();
    this.poller = startSessionPoller();
    this.expiredSessionMap = new ConcurrentHashMap<>();
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
    final CountDownLatch lock = new CountDownLatch(1);

    ListenableFuture<BatchCreateSessionsResponse> batchCreateSessionsResponseFuture =
        client.batchCreateSession(Context.getDefault(), batchCreateSessionRequest);

    Futures.addCallback(
        batchCreateSessionsResponseFuture,
        new FutureCallback<BatchCreateSessionsResponse>() {
          @Override
          public void onSuccess(
              @NullableDecl BatchCreateSessionsResponse batchCreateSessionsResponse) {
            Preconditions.checkNotNull(batchCreateSessionsResponse);

            batchCreateSessionsResponse
                .getSessionList()
                .forEach(
                    s -> {
                      boolean ok = sessionPool.tryPut(new SessionContext(s));
                      // This failed?
                      assert (ok);
                    });
            lock.countDown();
          }

          @Override
          public void onFailure(Throwable t) {
            // TODO(xjdr): Throw or do something else here
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
          sessionPool
              .getSessionList()
              .forEach(
                  ctx -> {
                    expiredSessionMap.put(ctx.getSession(), ctx);
                    long lastUsed = ctx.lastUsed().toMinutes();
                    long threshold = Duration.ofMinutes(45).toMinutes();
                    if (lastUsed > threshold) {
                      ListenableFuture<Session> getSessionFuture =
                          client.getSession(
                              Context.getDefault(),
                              GetSessionRequest.newBuilder().setName(ctx.getName()).build());
                      Futures.addCallback(
                          getSessionFuture,
                          new FutureCallback<Session>() {
                            @Override
                            public void onSuccess(@Nullable Session session_) {
                              expiredSessionMap.remove(session_);
                            }

                            @Override
                            public void onFailure(Throwable t) {}
                          },
                          MoreExecutors.directExecutor());
                    }
                  });
        },
        30,
        30,
        TimeUnit.SECONDS);
  }

  ListenableFuture<RowCursor> executeSql(String sql) {
    Preconditions.checkNotNull(sql);

    final Optional<SessionContext> sessionOptional = sessionPool.tryGet();
    final SettableFuture<RowCursor> resultSetFuture = SettableFuture.create();

    if (sessionOptional.isPresent()) {
      final SessionContext session = sessionOptional.get();
      if (!session.lock()) {
        // throw
      }

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
              session.unlock();
            }

            @Override
            public void onFailure(Throwable t) {
              resultSetFuture.setException(t);
              session.unlock();
            }
          },
          MoreExecutors.directExecutor());
    }

    return resultSetFuture;
  }

  ListenableFuture<RowCursor> executeSqlReadOnlyStrong(String sql) {
    Preconditions.checkNotNull(sql);

    final Optional<SessionContext> sessionOptional = sessionPool.tryGet();
    final SettableFuture<RowCursor> resultSetFuture = SettableFuture.create();

    if (sessionOptional.isPresent()) {
      final SessionContext session = sessionOptional.get();
      if (!session.lock()) {
        // TODO(xjdr): Throw
      }

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
              session.unlock();
            }

            @Override
            public void onFailure(Throwable t) {
              resultSetFuture.setException(t);
              session.unlock();
            }
          },
          MoreExecutors.directExecutor());
    }

    return resultSetFuture;
  }

  void executeStreamingSql(String sql, StreamObserver<Row> handler) {
    Preconditions.checkNotNull(sql);

    final Optional<SessionContext> sessionOptional = sessionPool.tryGet();
    final List<PartialResultSet> resultSetList = new ArrayList<>();

    if (sessionOptional.isPresent()) {
      final SessionContext session = sessionOptional.get();
      if (!session.lock()) {
        // TODO(xjdr): Throw
      }
      client.executeStreamingSql(
          Context.getDefault(),
          ExecuteSqlRequest.newBuilder().setSession(session.getName()).setSql(sql).build(),
          new StreamObserver<PartialResultSet>() {
            @Override
            public void onNext(PartialResultSet value) {
              final ImmutableList<StructType.Field> fieldList =
                  ImmutableList.copyOf(value.getMetadata().getRowType().getFieldsList());
              if (value.getChunkedValue()) {
                resultSetList.add(value);
              } else {
                ResultSet resultSet =
                    PartialResultSetCombiner.combine(ImmutableList.of(value), fieldList.size(), 0);
                RowCursor rowCursor =
                    RowCursor.of(fieldList, ImmutableList.copyOf(resultSet.getRowsList()));
                while (rowCursor.next()) {
                  handler.onNext(rowCursor.getCurrentRow());
                }
              }
            }

            @Override
            public void onError(Throwable t) {
              session.unlock();
              handler.onError(t);
            }

            @Override
            public void onCompleted() {
              if (resultSetList.size() >= 1) {
                final ImmutableList<StructType.Field> fieldList =
                    ImmutableList.copyOf(
                        resultSetList.get(0).getMetadata().getRowType().getFieldsList());
                final ResultSet resultSet =
                    PartialResultSetCombiner.combine(resultSetList, fieldList.size(), 0);
                final ImmutableList<ListValue> rowList =
                    ImmutableList.copyOf(resultSet.getRowsList());
                rowList.forEach(
                    v -> {
                      handler.onNext(Row.of(fieldList, ImmutableList.copyOf(v.getValuesList())));
                    });
              }
              session.unlock();
              handler.onCompleted();
            }
          });
    }
  }

  void executeStreamingSqlReadOnlyStrong(String sql, StreamObserver<Row> handler) {
    Preconditions.checkNotNull(sql);

    final Optional<SessionContext> sessionOptional = sessionPool.tryGet();

    if (sessionOptional.isPresent()) {
      final SessionContext session = sessionOptional.get();
      if (!session.lock()) {
        // TODO(xjdr): Throw
      }
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
              handler.onNext(Row.of(fieldList, values));
            }

            @Override
            public void onError(Throwable t) {
              session.unlock();
              handler.onError(t);
            }

            @Override
            public void onCompleted() {
              session.unlock();
              handler.onCompleted();
            }
          });
    }
  }

  void close() throws InterruptedException {
    poller.cancel(true);
    scheduler.shutdownNow();
    final CountDownLatch lock = new CountDownLatch(sessionPool.size());
    sessionPool
        .getSessionList()
        .forEach(
            s -> {
              if (s != null) {
                ListenableFuture<Empty> sf =
                    client.deleteSession(
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
