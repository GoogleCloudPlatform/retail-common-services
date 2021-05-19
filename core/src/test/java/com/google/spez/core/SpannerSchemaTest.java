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
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.testing.RemoteSpannerHelper;
import com.google.spez.core.internal.BothanDatabase;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.SchemaBuilder;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
@EnabledIfEnvironmentVariable(named = "INTEGRATION_TESTS", matches = "true")
class SpannerSchemaTest extends SpannerIntegrationTest implements WithAssertions {
  RemoteSpannerHelper helper;
  Database database;

  static String TIMESTAMP = "timestamp";

  @BeforeEach
  void setUp() throws Throwable {
    helper = RemoteSpannerHelper.create(InstanceId.of(projectId, INSTANCE_ID));
    database =
        helper.createTestDatabase(
            List.of(
                "CREATE TABLE Singers (\n"
                    + "  SingerId   INT64 NOT NULL,\n"
                    + "  FirstName  STRING(1024),\n"
                    + "  LastName   STRING(1024),\n"
                    + "  SingerInfo BYTES(MAX),\n"
                    + "  timestamp  TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)\n"
                    + ") PRIMARY KEY (SingerId)"));
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void getSchema() throws IOException {
    var sinkConfig =
        new SpezConfig.SinkConfig(
            projectId,
            INSTANCE_ID,
            database.getId().getDatabase(),
            "Singers",
            "SingerId",
            TIMESTAMP,
            30,
            GoogleCredentials.getApplicationDefault());
    var database = BothanDatabase.openDatabase(sinkConfig.getSettings());
    SpannerSchema spannerSchema = new SpannerSchema(database, sinkConfig);
    var result = spannerSchema.getSchema();
    var expectedAvroSchema =
        SchemaBuilder.record("Singers")
            .namespace("avroNamespace")
            .fields()
            .name("SingerId")
            .type()
            .longType()
            .noDefault()
            .name("FirstName")
            .type()
            .optional()
            .stringType()
            .name("LastName")
            .type()
            .optional()
            .stringType()
            .name(TIMESTAMP)
            .type()
            .stringType()
            .noDefault()
            .endRecord();
    assertThat(result.avroSchema()).isEqualTo(expectedAvroSchema);
    var expectedSchema =
        Map.of(
            "FirstName",
            "STRING(1024)",
            "LastName",
            "STRING(1024)",
            "SingerId",
            "INT64",
            "SingerInfo",
            "BYTES(MAX)",
            TIMESTAMP,
            "TIMESTAMP");
    assertThat(result.spannerSchema()).containsExactlyInAnyOrderEntriesOf(expectedSchema);
  }
}
