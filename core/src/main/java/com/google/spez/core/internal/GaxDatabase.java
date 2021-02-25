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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.spannerclient.Query;
import com.google.spannerclient.QueryOptions;
import com.google.spannerclient.Spanner;
import io.grpc.stub.StreamObserver;

import java.io.IOException;

public class GaxDatabase implements Database {
  @Override
  public ListenableFuture<RowCursor> executeAsync(String query, ListeningExecutorService service) {
    return null;
  }

  @Override
  public RowCursor execute(String query) {
    return null;
  }

  @Override
  public void executeStreaming(QueryOptions options, StreamObserver<Row> observer, Query query) {
  }

  @Override
  public void close() throws IOException {
  }
}
