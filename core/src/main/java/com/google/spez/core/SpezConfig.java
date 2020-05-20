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

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpezConfig {
  private static final Logger log = LoggerFactory.getLogger(SpezConfig.class);

  public static class AuthConfig {
    private final String service_account;

    public AuthConfig(String service_account) {
      this.service_account = service_account;
    }

    public static AuthConfig parse(Config config) {
      return new AuthConfig(config.getString("service_account"));
    }

    public String getServiceAccount() {
      return service_account;
    }

    public String toString() {
      return "[service_account=" + service_account + "]";
    }
  }

  public static class PubSubConfig {
    private final String project_id;
    private final String topic;

    public PubSubConfig(String project_id, String topic) {
      this.project_id = project_id;
      this.topic = topic;
    }

    public static PubSubConfig parse(Config config) {
      return new PubSubConfig(config.getString("project_id"), config.getString("topic"));
    }

    public String getProjectId() {
      return project_id;
    }

    public String getTopic() {
      return topic;
    }

    public String toString() {
      return "[project_id=" + project_id + ", topic=" + topic + "]";
    }
  }

  public static class SpannerDbConfig {
    private final String project_id;
    private final String instance;
    private final String database;
    private final String table;
    private final String uuid_field_name;
    private final String timestamp_field_name;

    public SpannerDbConfig(
        String project_id,
        String instance,
        String database,
        String table,
        String uuid_field_name,
        String timestamp_field_name) {
      this.project_id = project_id;
      this.instance = instance;
      this.database = database;
      this.table = table;
      this.uuid_field_name = uuid_field_name;
      this.timestamp_field_name = timestamp_field_name;
    }

    public static SpannerDbConfig parse(Config config) {
      return new SpannerDbConfig(
          config.getString("project_id"),
          config.getString("instance"),
          config.getString("database"),
          config.getString("table"),
          config.getString("uuid_field_name"),
          config.getString("timestamp_field_name"));
    }

    public String getProjectId() {
      return project_id;
    }

    public String getInstance() {
      return instance;
    }

    public String getDatabase() {
      return database;
    }

    public String getTable() {
      return table;
    }

    public String getUuidFieldName() {
      return uuid_field_name;
    }

    public String getTimestampFieldName() {
      return timestamp_field_name;
    }

    public String toString() {
      return "[project_id="
          + project_id
          + ", instance="
          + instance
          + ", database="
          + database
          + ", table="
          + table
          + ", uuid_field_name="
          + uuid_field_name
          + ", timestamp_field_name="
          + timestamp_field_name
          + "]";
    }

    public String databasePath() {
      return new StringBuilder()
          .append("projects/")
          .append(project_id)
          .append("/instances/")
          .append(instance)
          .append("/databases/")
          .append(database)
          .toString();
    }

    public String tablePath() {
      return new StringBuilder().append(databasePath()).append("/tables/").append(table).toString();
    }
  }

  private final AuthConfig auth;
  private final PubSubConfig pubsub;
  private final SpannerDbConfig spannerdb;

  public SpezConfig(AuthConfig auth, PubSubConfig pubsub, SpannerDbConfig spannerdb) {
    this.auth = auth;
    this.pubsub = pubsub;
    this.spannerdb = spannerdb;
  }

  public static SpezConfig parse(Config config) {
    AuthConfig auth = AuthConfig.parse(config.getConfig("spez.auth"));
    PubSubConfig pubsub = PubSubConfig.parse(config.getConfig("spez.pubsub"));
    SpannerDbConfig spannerdb = SpannerDbConfig.parse(config.getConfig("spez.spannerdb"));

    log.info("parsed auth: " + auth);
    log.info("parsed pubsub: " + pubsub);
    log.info("parsed spannerdb: " + spannerdb);

    return new SpezConfig(auth, pubsub, spannerdb);
  }

  public AuthConfig getAuth() {
    return auth;
  }

  public PubSubConfig getPubSub() {
    return pubsub;
  }

  public SpannerDbConfig getSpannerDb() {
    return spannerdb;
  }
}
