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

package com.google.spez.core.internal;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.spannerclient.Query;
import com.google.spannerclient.QueryOptions;
import com.google.spannerclient.Settings;
import com.google.spannerclient.Spanner;
import com.google.spez.common.Inexcusables;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class BothanDatabase implements Database {
  private final com.google.spannerclient.Database database;

  private BothanDatabase(com.google.spannerclient.Database database) {
    this.database = database;
  }

  public static ListenableFuture<Database> openDatabaseAsync(
      Settings settings, ListeningExecutorService service) {
    ListenableFuture<com.google.spannerclient.Database> dbFuture =
        Spanner.openDatabaseAsync(settings);
    return Futures.transform(dbFuture, BothanDatabase::new, service);
  }

  public static Database openDatabase(Settings settings) {
    return Inexcusables.getInexcusably(
        openDatabaseAsync(settings, MoreExecutors.newDirectExecutorService()));
  }

  @Override
  public ListenableFuture<RowCursor> executeAsync(String query, ListeningExecutorService service) {
    ListenableFuture<com.google.spannerclient.RowCursor> executeFuture =
        Spanner.executeAsync(QueryOptions.DEFAULT(), database, Query.create(query));
    return Futures.transform(executeFuture, BothanRowCursor::new, service);
  }

  @Override
  public RowCursor execute(String query) {
    return Inexcusables.getInexcusably(
        executeAsync(query, MoreExecutors.newDirectExecutorService()));
  }
}
