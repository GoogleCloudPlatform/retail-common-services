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

import com.google.common.util.concurrent.ListenableFuture;

public class Query {
  private String sql;

  private Query(String sql) {
    this.sql = sql;
  }

  public static Query create(String sql) {
    return new Query(sql);
  }

  public Query bind(String name, String value) {
    return this;
  }

  public Query append(String sql) {
    return this;
  }

  public RowCursor execute(QueryOptions options, Database db) {
    return null;
  }

  public ListenableFuture<RowCursor> executeAsync(QueryOptions options, Database db) {
    return null;
  }

  public void executeStreaming(
      QueryOptions options, Database db, SpannerStreamingHandler handler) {}

  public String getSql() {
    return sql;
  }
}
