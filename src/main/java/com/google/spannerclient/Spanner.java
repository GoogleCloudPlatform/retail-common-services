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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ListValue;
import com.google.spanner.v1.*;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Spanner {
  private static final Logger log = LoggerFactory.getLogger(Spanner.class);

  public static Database openDatabase(
      Options options, String dbPath, GoogleCredentials credentials) {
    return null;
  }

  public static ListenableFuture<Database> openDatabaseAsync(
      Options options, String dbPath, GoogleCredentials credentials) {
    return new Database(dbPath, credentials).open(Options.DEFAULT());
  }

  public static Database createDatabase(
      Options options, String dbPath, GoogleCredentials credentials) {
    return null;
  }

  public static ListenableFuture<Database> createDatabaseAsync(
      Options options, String dbPath, GoogleCredentials credentials) {
    return null;
  }

  public static Row readRow(ReadOptions readOptions, Table table, Key key, ColumnList columnList) {
    return null;
  }

  public static Row readRow(
      ReadOptions readOptions, Table table, RowSet rowSet, ColumnList columnList) {
    return null;
  }

  public static ListenableFuture<Row> readRowAsync(
      ReadOptions readOptions, Table table, Key key, ColumnList columnList) {
    return null;
  }

  public static ListenableFuture<Row> readRowAsync(
      ReadOptions readOptions, Table table, RowSet rowSet, ColumnList columnList) {
    return null;
  }

  public static RowCursor execute(QueryOptions options, Database db, Query query) {
    final CountDownLatch doneSignal = new CountDownLatch(1);
    final ListenableFuture<RowCursor> executeFuture = executeAsync(options, db, query);
    RowCursor[] rowCursor = new RowCursor[1];

    Futures.addCallback(
        executeFuture,
        new FutureCallback<RowCursor>() {

          @Override
          public void onSuccess(RowCursor result) {
            if (rowCursor != null) {
              rowCursor[0] = result;
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

    if (rowCursor[0] != null) {
      return rowCursor[0];
    } else {
      // be better than this
      return null;
    }
  }

  public static ListenableFuture<RowCursor> executeAsync(
      QueryOptions options, Database db, Query query) {
    //    Preconditions.checkNotNull(options);
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(query);

    final Optional<SessionContext> sessionOptional = db.sessionPool().tryGet();
    final SettableFuture<RowCursor> resultSetFuture = SettableFuture.create();

    if (sessionOptional.isPresent()) {
      final SessionContext session = sessionOptional.get();
      log.info("took out session '{}'", session);
      if (session.lock()) {
        final ExecuteSqlRequest req = getExecuteSqlRequest(options, query, session.getName());
        final ListenableFuture<ResultSet> resultSetListenableFuture =
            db.stub().executeSql(Context.getDefault(), req);
        Futures.addCallback(
            resultSetListenableFuture,
            new FutureCallback<ResultSet>() {

              @Override
              public void onSuccess(ResultSet resultSet) {
                final ImmutableList<StructType.Field> fieldList =
                    ImmutableList.copyOf(resultSet.getMetadata().getRowType().getFieldsList());
                final ImmutableList<ListValue> rowList =
                    ImmutableList.copyOf(resultSet.getRowsList());
                final RowCursor rowCursor = RowCursor.of(fieldList, rowList);

                resultSetFuture.set(rowCursor);
                cleanup();
              }

              @Override
              public void onFailure(Throwable t) {
                resultSetFuture.setException(t);
                cleanup();
              }

              public void cleanup() {
                session.unlock();
                if (!db.sessionPool().tryPut(session)) {
                  log.error(
                      "Couldn't put session '{}' back into pool '{}'", session, db.sessionPool());
                } else {
                  log.info("put away session '{}'", session);
                }
              }
            },
            MoreExecutors.directExecutor());
      } else {
        // throw prolly
        log.warn("Couldn't lock SessionContext");
      }
    } else {
      log.warn("Couldn't get a SessionContext");
    }

    return resultSetFuture;
  }

  public static void executeStreaming(
      QueryOptions options, Database db, StreamObserver<Row> handler, Query query) {
    // Preconditions.checkNotNull(options);
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(handler);
    Preconditions.checkNotNull(query);

    final Optional<SessionContext> sessionOptional = db.sessionPool().tryGet();
    final List<PartialResultSet> resultSetList = new ArrayList<>();
    if (sessionOptional.isPresent()) {
      final SessionContext session = sessionOptional.get();
      log.info("took out session '{}'", session);
      if (session.lock()) {
        final ExecuteSqlRequest req = getExecuteSqlRequest(options, query, session.getName());
        db.stub()
            .executeStreamingSql(
                Context.getDefault(),
                req,
                new StreamObserver<PartialResultSet>() {
                  ImmutableList<StructType.Field> fieldList = null;

                  @Override
                  public void onNext(PartialResultSet value) {
                    // Only the first PartialResultSet will contain Metadata
                    if (fieldList == null) {
                      fieldList =
                          ImmutableList.copyOf(value.getMetadata().getRowType().getFieldsList());
                    }
                    if (value.getChunkedValue()) {
                      log.debug("got chunked");
                      // this value is chunked, we can't process it until we received a non chunked
                      // value
                      // append it to the resultSetList and process it later
                      resultSetList.add(value);
                    } else {
                      log.debug("got regular");
                      resultSetList.add(value);
                      final ResultSet resultSet =
                          PartialResultSetCombiner.combine(resultSetList, fieldList.size(), 0);
                      final RowCursor rowCursor =
                          RowCursor.of(fieldList, ImmutableList.copyOf(resultSet.getRowsList()));
                      while (rowCursor.next()) {
                        handler.onNext(rowCursor.getCurrentRow());
                      }
                      resultSetList.clear();
                    }
                  }

                  @Override
                  public void onError(Throwable t) {
                    cleanup();
                    handler.onError(t);
                  }

                  @Override
                  public void onCompleted() {
                    if (resultSetList.size() >= 1 && fieldList != null) {
                      log.warn(
                          "onCompleted called with resuletSetList.size = " + resultSetList.size());
                      final ResultSet resultSet =
                          PartialResultSetCombiner.combine(resultSetList, fieldList.size(), 0);
                      final ImmutableList<ListValue> rowList =
                          ImmutableList.copyOf(resultSet.getRowsList());
                      rowList.forEach(
                          v -> {
                            handler.onNext(
                                Row.of(fieldList, ImmutableList.copyOf(v.getValuesList())));
                          });
                    }
                    cleanup();
                    handler.onCompleted();
                  }

                  public void cleanup() {
                    session.unlock();
                    if (!db.sessionPool().tryPut(session)) {
                      log.error(
                          "Couldn't put session '{}' back into pool '{}'",
                          session,
                          db.sessionPool());
                    } else {
                      log.info("put away session '{}'", session);
                    }
                  }
                });

      } else {
        // -------
        log.warn("Couldn't lock SessionContext");
      }
    } else {
      log.warn("Couldn't get a SessionContext");
    }
  }

  private static ExecuteSqlRequest getExecuteSqlRequest(
      QueryOptions options, Query query, String session) {
    // If strong and stale are both set, will return strong;
    if (options.readOnly()) {
      if (options.strong()) {
        return ExecuteSqlRequest.newBuilder()
            .setSession(session)
            .setSql(query.getSql())
            .setTransaction(
                TransactionSelector.newBuilder()
                    .setSingleUse(
                        TransactionOptions.newBuilder()
                            .setReadOnly(TransactionOptions.ReadOnly.newBuilder().setStrong(true))
                            .build()))
            .build();
      }

      if (options.stale()) {
        Preconditions.checkArgument(options.maxStaleness() > 0);
        return ExecuteSqlRequest.newBuilder()
            .setSession(session)
            .setSql(query.getSql())
            .setTransaction(
                TransactionSelector.newBuilder()
                    .setSingleUse(
                        TransactionOptions.newBuilder()
                            .setReadOnly(
                                TransactionOptions.ReadOnly.newBuilder()
                                    .setMaxStaleness(
                                        com.google.protobuf.Duration.newBuilder()
                                            .setSeconds(options.maxStaleness())
                                            .build()))
                            .build()))
            .build();
      }
    }

    return ExecuteSqlRequest.newBuilder().setSession(session).setSql(query.getSql()).build();
  }
}
