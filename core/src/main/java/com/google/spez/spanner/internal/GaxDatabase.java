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

import com.google.api.core.ApiFuture;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.AsyncResultSet.CallbackResponse;
import com.google.cloud.spanner.AsyncResultSet.ReadyCallback;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.spez.spanner.Database;
import com.google.spez.spanner.QueryOptions;
import com.google.spez.spanner.Row;
import com.google.spez.spanner.RowCursor;
import com.google.spez.spanner.Settings;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

public class GaxDatabase implements Database {
  private static final Logger log = LoggerFactory.getLogger(GaxDatabase.class);
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final DatabaseClient client;

  private GaxDatabase(DatabaseClient client) {
    this.client = client;
  }

  /**
   * Open a database.
   *
   * @param settings Settings to open the database
   * @return a Database
   */
  public static Database openDatabase(Settings settings) {
    SessionPoolOptions sessionPoolOptions =
        SessionPoolOptions.newBuilder()
            .setRemoveInactiveSessionAfter(Duration.ofSeconds(15))
            .build();
    Spanner spannerClient =
        SpannerOptions.newBuilder()
            .setProjectId(settings.projectId())
            .setSessionPoolOption(sessionPoolOptions)
            .build()
            .getService();
    DatabaseId databaseId =
        DatabaseId.of(settings.projectId(), settings.instance(), settings.database());
    DatabaseClient databaseClient = spannerClient.getDatabaseClient(databaseId);
    return new GaxDatabase(databaseClient);
  }

  @Override
  public RowCursor execute(String query) {
    ResultSet resultSet = client.singleUse().executeQuery(Statement.of(query));
    return new GaxRowCursor(resultSet);
  }

  private AsyncResultSet executeAsync(String query) {
    return client.singleUse().executeQueryAsync(Statement.of(query));
  }

  private ReadyCallback buildCallback(StreamObserver<Row> observer) {
    return new ReadyCallback() {
      @Override
      public CallbackResponse cursorReady(AsyncResultSet resultSet) {
        try {
          while (true) {
            switch (resultSet.tryNext()) {
                // OK: There is a row ready.
              case OK:
                observer.onNext(new GaxRow(resultSet));
                break;

                // DONE: There are no more rows in the result set.
              case DONE:
                observer.onCompleted();
                return CallbackResponse.DONE;

                // NOT_READY: There are currently no more rows in the buffer.
              case NOT_READY:
                return CallbackResponse.CONTINUE;

              default:
                throw new IllegalStateException();
            }
          }
        } catch (SpannerException e) {
          log.error("Error in AsyncCallback", e);
          observer.onError(e);
          return CallbackResponse.DONE;
        }
      }
    };
  }

  @Override
  public void executeStreaming(QueryOptions options, StreamObserver<Row> observer, String query) {
    try (AsyncResultSet resultSet = executeAsync(query)) {
      // Setting a callback will automatically start the iteration over the results of the query
      // using the specified executor. The callback will be called at least once. The returned
      // ApiFuture is done when the callback has returned DONE and all resources used by the
      // AsyncResultSet have been released.
      ApiFuture<Void> finished = resultSet.setCallback(executor, buildCallback(observer));

      // This ApiFuture is done when the callback has returned DONE and all resources of the
      // asynchronous result set have been released.
      finished.get();
    } catch (ExecutionException | InterruptedException e) {
      log.error("Error in executeStreaming", e);
    }
  }

  @Override
  public void close() throws IOException {}
}
