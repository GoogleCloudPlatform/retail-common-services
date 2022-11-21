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

package com.google.spez.spanner.internal;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.spannerclient.Query;
import com.google.spannerclient.Spanner;
import com.google.spez.common.Inexcusables;
import com.google.spez.spanner.Database;
import com.google.spez.spanner.QueryOptions;
import com.google.spez.spanner.Row;
import com.google.spez.spanner.RowCursor;
import com.google.spez.spanner.Settings;
import io.grpc.stub.StreamObserver;
import java.io.IOException;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class BothanDatabase implements Database {
  private final com.google.spannerclient.Database database;

  private BothanDatabase(com.google.spannerclient.Database database) {
    this.database = database;
  }

  private static com.google.spannerclient.Settings convert(Settings settings) {
    return com.google.spannerclient.Settings.newBuilder()
        .setCredentials(settings.credentials())
        .setDatabase(settings.database())
        .setInstance(settings.instance())
        .setPoolSize(settings.poolSize())
        .setProjectId(settings.projectId())
        .setScheduler(settings.scheduler())
        .setStalenessCheck(settings.stalenessCheck())
        .build();
  }

  /**
   * Asynchronously open a database.
   *
   * @param settings Settings to open the database
   * @param service service to use for async
   * @return a Database wrapped in a ListenableFuture
   */
  public static ListenableFuture<Database> openDatabaseAsync(
      Settings settings, ListeningExecutorService service) {
    ListenableFuture<com.google.spannerclient.Database> dbFuture =
        Spanner.openDatabaseAsync(convert(settings));
    return Futures.transform(dbFuture, BothanDatabase::new, service);
  }

  public static Database openDatabase(Settings settings) {
    return Inexcusables.getInexcusably(
        openDatabaseAsync(settings, MoreExecutors.newDirectExecutorService()));
  }

  private ListenableFuture<RowCursor> executeAsync(String query, ListeningExecutorService service) {
    ListenableFuture<com.google.spannerclient.RowCursor> executeFuture =
        Spanner.executeAsync(
            com.google.spannerclient.QueryOptions.DEFAULT(), database, Query.create(query));
    return Futures.transform(executeFuture, BothanRowCursor::new, service);
  }

  @Override
  public RowCursor execute(String query) {
    return Inexcusables.getInexcusably(
        executeAsync(query, MoreExecutors.newDirectExecutorService()));
  }

  @Override
  public RowCursor executeMutate(String query) {
    return null;
  }

  private static com.google.spannerclient.QueryOptions convert(QueryOptions options) {
    return com.google.spannerclient.QueryOptions.newBuilder()
        .setMaxStaleness(options.maxStaleness())
        .setReadOnly(options.readOnly())
        .setStale(options.stale())
        .setStrong(options.strong())
        .build();
  }

  @Override
  public void executeStreaming(QueryOptions options, StreamObserver<Row> observer, String query) {
    Spanner.executeStreaming(
        convert(options), database, new DelegatingStreamObserver(observer), Query.create(query));
  }

  @Override
  public void close() throws IOException {
    database.close();
  }

  private static class DelegatingStreamObserver
      implements StreamObserver<com.google.spannerclient.Row> {
    private StreamObserver<Row> delegate;

    public DelegatingStreamObserver(StreamObserver<Row> delegate) {
      this.delegate = delegate;
    }

    @Override
    public void onNext(com.google.spannerclient.Row row) {
      delegate.onNext(new BothanRow(row));
    }

    @Override
    public void onError(Throwable t) {
      delegate.onError(t);
    }

    @Override
    public void onCompleted() {
      delegate.onCompleted();
    }
  }
}
