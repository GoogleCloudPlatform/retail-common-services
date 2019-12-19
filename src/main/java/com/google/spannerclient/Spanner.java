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

public class Spanner {

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
    Preconditions.checkNotNull(options);
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(query);

    final Optional<SessionContext> sessionOptional = db.sessionPool().tryGet();
    final SettableFuture<RowCursor> resultSetFuture = SettableFuture.create();

    if (sessionOptional.isPresent()) {
      final SessionContext session = sessionOptional.get();
      if (session.lock()) {

        ListenableFuture<ResultSet> resultSetListenableFuture =
            db.stub()
                .executeSql(
                    Context.getDefault(),
                    ExecuteSqlRequest.newBuilder()
                        .setSession(session.getName())
                        .setSql(query.getSql())
                        .build());
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
              }

              @Override
              public void onFailure(Throwable t) {
                resultSetFuture.setException(t);
              }
            },
            MoreExecutors.directExecutor());

      } else {
        // throw prolly
      }
    }

    return resultSetFuture;
  }

  public static void executeStreaming(
      QueryOptions options, Database db, SpannerStreamingHandler handler, Query query) {
    // Preconditions.checkNotNull(options);
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(handler);
    Preconditions.checkNotNull(query);

    final Optional<SessionContext> sessionOptional = db.sessionPool().tryGet();
    final List<PartialResultSet> resultSetList = new ArrayList<>();

    if (sessionOptional.isPresent()) {
      final SessionContext session = sessionOptional.get();
      if (session.lock()) {
        db.stub()
            .executeStreamingSql(
                Context.getDefault(),
                ExecuteSqlRequest.newBuilder()
                    .setSession(session.getName())
                    .setSql(query.getSql())
                    .build(),
                new StreamObserver<PartialResultSet>() {
                  @Override
                  public void onNext(PartialResultSet value) {
                    final ImmutableList<StructType.Field> fieldList =
                        ImmutableList.copyOf(value.getMetadata().getRowType().getFieldsList());
                    if (value.getChunkedValue()) {
                      resultSetList.add(value);
                    } else {
                      ResultSet resultSet =
                          PartialResultSetCombiner.combine(
                              ImmutableList.of(value), fieldList.size(), 0);
                      RowCursor rowCursor =
                          RowCursor.of(fieldList, ImmutableList.copyOf(resultSet.getRowsList()));
                      while (rowCursor.next()) {
                        handler.apply(rowCursor.getCurrentRow());
                      }
                    }
                  }

                  @Override
                  public void onError(Throwable t) {
                    session.unlock();
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
                            handler.apply(
                                Row.of(fieldList, ImmutableList.copyOf(v.getValuesList())));
                          });
                    }
                    session.unlock();
                  }
                });

      } else {
        // -------
      }
    }
  }
}
