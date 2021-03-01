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

package com.google.spez.core;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.testing.RemoteSpannerHelper;
import java.io.IOException;
import java.util.List;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
@EnabledIfEnvironmentVariable(named = "INTEGRATION_TESTS", matches = "true")
public class LastProcessedTimestampTest extends SpannerIntegrationTest implements WithAssertions {
  RemoteSpannerHelper helper;
  Database database;

  @BeforeEach
  void setUp() throws Throwable {
    helper = RemoteSpannerHelper.create(InstanceId.of(projectId, INSTANCE_ID));
    database =
        helper.createTestDatabase(
            List.of(
                "CREATE TABLE lpts (\n"
                    + "  instance STRING(MAX) NOT NULL,\n"
                    + "  database STRING(MAX) NOT NULL,\n"
                    + "  table STRING(MAX) NOT NULL,\n"
                    + "  CommitTimestamp TIMESTAMP NOT NULL"
                    + " OPTIONS (allow_commit_timestamp=true),\n"
                    + "  LastProcessedTimestamp TIMESTAMP NOT NULL,\n"
                    + ") PRIMARY KEY (instance, database, table)"));
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void noTimestampShouldThrow() throws IOException {
    var credentials = GoogleCredentials.getApplicationDefault(); // NOPMD
    var sink = // NOPMD
        new SpezConfig.SinkConfig(
            projectId,
            INSTANCE_ID,
            database.getId().getDatabase(),
            "Singers",
            "SingerId",
            "timestamp",
            credentials);
    var lpts = // NOPMD
        new SpezConfig.LptsConfig(
            projectId, INSTANCE_ID, database.getId().getDatabase(), "lpts", credentials);
    var throwable =
        catchThrowable(() -> LastProcessedTimestamp.getLastProcessedTimestamp(sink, lpts));
    assertThat(throwable)
        .isInstanceOf(RuntimeException.class)
        .hasMessage("LastProcessedTimestamp was unavailable");
  }

  @Test
  void findTimestampForTable() throws Exception {
    var credentials = GoogleCredentials.getApplicationDefault(); // NOPMD
    var setTimestamp = Timestamp.ofTimeSecondsAndNanos(0, 0);
    var client =
        SpannerOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService()
            .getDatabaseClient(database.getId());
    client
        .readWriteTransaction()
        .run(
            new TransactionRunner.TransactionCallable<Void>() {
              @Override
              public Void run(TransactionContext transaction) throws Exception {
                String sql =
                    "INSERT INTO lpts ("
                        + "instance, "
                        + "database, "
                        + "table, "
                        + "CommitTimestamp, "
                        + "LastProcessedTimestamp) VALUES ('"
                        + INSTANCE_ID
                        + "', '"
                        + database.getId().getDatabase()
                        + "', 'Singers', PENDING_COMMIT_TIMESTAMP(),'"
                        + setTimestamp
                        + "')";
                transaction.executeUpdate(Statement.of(sql));
                return null;
              }
            });

    var sink =
        new SpezConfig.SinkConfig(
            projectId,
            INSTANCE_ID,
            database.getId().getDatabase(),
            "Singers",
            "SingerId",
            "timestamp",
            credentials);
    var lpts =
        new SpezConfig.LptsConfig(
            projectId, INSTANCE_ID, database.getId().getDatabase(), "lpts", credentials);
    var timestamp = LastProcessedTimestamp.getLastProcessedTimestamp(sink, lpts);
    assertThat(timestamp).isEqualTo("1970-01-01T00:00:00Z");
  }
}
