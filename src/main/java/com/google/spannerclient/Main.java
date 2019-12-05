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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.*;
import java.io.FileInputStream;
import java.util.concurrent.CountDownLatch;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

class Main {
  private static final ImmutableList<String> DEFAULT_SERVICE_SCOPES =
      ImmutableList.<String>builder()
          .add("https://www.googleapis.com/auth/cloud-platform")
          .add("https://www.googleapis.com/auth/spanner.data")
          .build();

  public static void main(String[] args) {
    final CountDownLatch doneSignal = new CountDownLatch(1);

    try {
      final GoogleCredentials credentials =
          GoogleCredentials.fromStream(new FileInputStream("/var/run/secret/cloud.google.com/service-account.json"))
              .createScoped(DEFAULT_SERVICE_SCOPES);

      final String database = "projects/{project_id}/instances/test-db/databases/test";
      final String sql =
          "SELECT * FROM "
              + "test"
              + " "
              + "WHERE Timestamp > '"
              + "2019-08-08T20:02:48.000000Z"
              + "' "
              + "ORDER BY Timestamp ASC";

      final SpannerAsyncClient client = new SpannerAsyncClient(database, credentials, 2);
      final ListenableFuture<RowCursor> resultSetListenableFuture = client.executeSql(sql);

      Futures.addCallback(
          resultSetListenableFuture,
          new FutureCallback<RowCursor>() {
            @Override
            public void onSuccess(@NullableDecl RowCursor rowCursor) {

              while (rowCursor.next()) {
                System.out.println("UUID: " + rowCursor.getLong("UUID"));
                System.out.println("SortingKey: " + rowCursor.getString(1));
                System.out.println("Timestamp: " + rowCursor.getTimestamp(2));
                System.out.println("Data: " + rowCursor.getString(3));
              }

              doneSignal.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
              doneSignal.countDown();
            }
          },
          MoreExecutors.directExecutor());

      doneSignal.await();

      client.close();

    } catch (Exception e) {
      System.out.println("Exception: " + e.toString());
    }
  }
}
