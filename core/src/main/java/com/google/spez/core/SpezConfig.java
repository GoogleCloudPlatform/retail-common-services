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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.spannerclient.Settings;
import com.google.spez.common.AuthConfig;
import com.google.spez.common.StackdriverConfig;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class SpezConfig {
  private static final Logger log = LoggerFactory.getLogger(SpezConfig.class);
  public static final String BASE_NAME = "spez";
  public static final String PROJECT_ID_KEY = "spez.project_id";
  public static final String AUTH_CLOUD_SECRETS_DIR_KEY = "spez.auth.cloud_secrets_dir";
  public static final String AUTH_CREDENTIALS_KEY = "spez.auth.credentials";
  public static final String AUTH_SCOPES_KEY = "spez.auth.scopes";
  public static final String LOGLEVEL_DEFAULT_KEY = "spez.loglevel.default";
  public static final String LOGLEVEL_CDC_KEY = "spez.loglevel.cdc";
  public static final String LOGLEVEL_CORE_KEY = "spez.loglevel.core";
  public static final String LOGLEVEL_NETTY_KEY = "spez.loglevel.netty";
  public static final String LOGLEVEL_SPANNERCLIENT_KEY = "spez.loglevel.spannerclient";
  public static final String PUBSUB_PROJECT_ID_KEY = "spez.pubsub.project_id";
  public static final String PUBSUB_TOPIC_KEY = "spez.pubsub.topic";
  public static final String PUBSUB_BUFFER_TIMEOUT_KEY = "spez.pubsub.buffer_timeout";
  public static final String SINK_PROJECT_ID_KEY = "spez.sink.project_id";
  public static final String SINK_INSTANCE_KEY = "spez.sink.instance";
  public static final String SINK_DATABASE_KEY = "spez.sink.database";
  public static final String SINK_TABLE_KEY = "spez.sink.table";
  public static final String SINK_UUID_COLUMN_KEY = "spez.sink.uuid_column";
  public static final String SINK_TIMESTAMP_COLUMN_KEY = "spez.sink.timestamp_column";
  public static final String LPTS_PROJECT_ID_KEY = "spez.lpts.project_id";
  public static final String LPTS_INSTANCE_KEY = "spez.lpts.instance";
  public static final String LPTS_DATABASE_KEY = "spez.lpts.database";
  public static final String LPTS_TABLE_KEY = "spez.lpts.table";
  public static final String SINK_UUID_KEY = "spez.sink.uuid";
  public static final String SINK_TIMESTAMP_KEY = "spez.sink.commit_timestamp";
  public static final String SINK_POLL_RATE_KEY = "spez.sink.poll_rate";

  public static class PubSubConfig {
    private final String projectId;
    private final String topic;
    private final int bufferTimeout;

    /** PubSubConfig value object constructor. */
    public PubSubConfig(String projectId, String topic, int bufferTimeout) {
      this.projectId = projectId;
      this.topic = topic;
      this.bufferTimeout = bufferTimeout;
    }

    /** PubSubConfig value object parser. */
    public static PubSubConfig parse(Config config) {
      return new PubSubConfig(
          config.getString(PUBSUB_PROJECT_ID_KEY),
          config.getString(PUBSUB_TOPIC_KEY),
          config.getInt(PUBSUB_BUFFER_TIMEOUT_KEY));
    }

    /** projectId getter. */
    public String getProjectId() {
      return projectId;
    }

    /** topic getter. */
    public String getTopic() {
      return topic;
    }

    /** bufferTimeout getter. */
    public int getBufferTimeout() {
      return bufferTimeout;
    }
  }

  public static class SinkConfig {
    private final String projectId;
    private final String instance;
    private final String database;
    private final String table;
    private final String uuidColumn;
    private final String timestampColumn;
    private final int pollRate;
    private final GoogleCredentials credentials;

    /** SinkConfig value object constructor. */
    public SinkConfig(
        String projectId,
        String instance,
        String database,
        String table,
        String uuidColumn,
        String timestampColumn,
        int pollRate,
        GoogleCredentials credentials) {
      this.projectId = projectId;
      this.instance = instance;
      this.database = database;
      this.table = table;
      this.uuidColumn = uuidColumn;
      this.timestampColumn = timestampColumn;
      this.pollRate = pollRate;
      this.credentials = credentials;
    }

    /** SinkConfig value object parser. */
    public static SinkConfig parse(Config config, GoogleCredentials credentials) {
      return new SinkConfig(
          config.getString(SINK_PROJECT_ID_KEY),
          config.getString(SINK_INSTANCE_KEY),
          config.getString(SINK_DATABASE_KEY),
          config.getString(SINK_TABLE_KEY),
          config.getString(SINK_UUID_COLUMN_KEY),
          config.getString(SINK_TIMESTAMP_COLUMN_KEY),
          config.getInt(SINK_POLL_RATE_KEY),
          credentials);
    }

    /** projectId getter. */
    public String getProjectId() {
      return projectId;
    }

    /** instance getter. */
    public String getInstance() {
      return instance;
    }

    /** database getter. */
    public String getDatabase() {
      return database;
    }

    /** table getter. */
    public String getTable() {
      return table;
    }

    /** settings getter. */
    public Settings getSettings() {
      return Settings.newBuilder()
          .setProjectId(projectId)
          .setInstance(instance)
          .setDatabase(database)
          .setCredentials(credentials)
          .build();
    }

    /** uuidColumn getter. */
    public String getUuidColumn() {
      return uuidColumn;
    }

    /** timestampColumn getter. */
    public String getTimestampColumn() {
      return timestampColumn;
    }

    /** pollRate getter. */
    public int getPollRate() {
      return pollRate;
    }

    /** databasePath getter. */
    public String databasePath() {
      return new StringBuilder()
          .append("projects/")
          .append(projectId)
          .append("/instances/")
          .append(instance)
          .append("/databases/")
          .append(database)
          .toString();
    }

    /** tablePath getter. */
    public String tablePath() {
      return new StringBuilder().append(databasePath()).append("/tables/").append(table).toString();
    }
  }

  public static class LptsConfig {
    private final String projectId;
    private final String instance;
    private final String database;
    private final String table;
    private final GoogleCredentials credentials;

    /** LptsConfig value object constructor. */
    public LptsConfig(
        String projectId,
        String instance,
        String database,
        String table,
        GoogleCredentials credentials) {
      this.projectId = projectId;
      this.instance = instance;
      this.database = database;
      this.table = table;
      this.credentials = credentials;
    }

    /** LptsConfig value object parser. */
    public static LptsConfig parse(Config config, GoogleCredentials credentials) {
      return new LptsConfig(
          config.getString(LPTS_PROJECT_ID_KEY),
          config.getString(LPTS_INSTANCE_KEY),
          config.getString(LPTS_DATABASE_KEY),
          config.getString(LPTS_TABLE_KEY),
          credentials);
    }

    /** projectId getter. */
    public String getProjectId() {
      return projectId;
    }

    /** instance getter. */
    public String getInstance() {
      return instance;
    }

    /** database getter. */
    public String getDatabase() {
      return database;
    }

    /** table getter. */
    public String getTable() {
      return table;
    }

    /** settings getter. */
    public Settings getSettings() {
      return Settings.newBuilder()
          .setProjectId(projectId)
          .setInstance(instance)
          .setDatabase(database)
          .setCredentials(credentials)
          .build();
    }

    /** databasePath getter. */
    public String databasePath() {
      return new StringBuilder()
          .append("projects/")
          .append(projectId)
          .append("/instances/")
          .append(instance)
          .append("/databases/")
          .append(database)
          .toString();
    }

    /** tablePath getter. */
    public String tablePath() {
      return new StringBuilder().append(databasePath()).append("/tables/").append(table).toString();
    }
  }

  private final AuthConfig auth;
  private final PubSubConfig pubsub;
  private final SinkConfig sink;
  private final StackdriverConfig stackdriver;
  private final LptsConfig lpts;

  /** SpezConfig value object constructor. */
  public SpezConfig(
      AuthConfig auth,
      PubSubConfig pubsub,
      SinkConfig sink,
      StackdriverConfig stackdriver,
      LptsConfig lpts) {
    this.auth = auth;
    this.pubsub = pubsub;
    this.sink = sink;
    this.stackdriver = stackdriver;
    this.lpts = lpts;
  }

  /** SpezConfig log helper. */
  public static void logParsedValues(Config config, List<String> configKeys) {
    log.info("=============================================");
    log.info("Spez configured with the following properties");
    for (var key : configKeys) {
      if (key.equals(AUTH_SCOPES_KEY)) {
        log.info("{}={}", key, config.getStringList(key));
      } else {
        log.info("{}={}", key, config.getString(key));
      }
    }
    log.info("=============================================");
  }

  /** SpezConfig value object parser. */
  public static SpezConfig parse(Config config) {
    List<String> configKeys =
        Lists.newArrayList(
            PROJECT_ID_KEY,
            AUTH_CLOUD_SECRETS_DIR_KEY,
            AUTH_CREDENTIALS_KEY,
            AUTH_SCOPES_KEY,
            LOGLEVEL_DEFAULT_KEY,
            LOGLEVEL_CDC_KEY,
            LOGLEVEL_CORE_KEY,
            LOGLEVEL_NETTY_KEY,
            LOGLEVEL_SPANNERCLIENT_KEY,
            PUBSUB_PROJECT_ID_KEY,
            PUBSUB_TOPIC_KEY,
            PUBSUB_BUFFER_TIMEOUT_KEY,
            SINK_PROJECT_ID_KEY,
            SINK_INSTANCE_KEY,
            SINK_DATABASE_KEY,
            SINK_TABLE_KEY,
            SINK_UUID_COLUMN_KEY,
            SINK_TIMESTAMP_COLUMN_KEY,
            SINK_POLL_RATE_KEY,
            LPTS_PROJECT_ID_KEY,
            LPTS_INSTANCE_KEY,
            LPTS_DATABASE_KEY,
            LPTS_TABLE_KEY);
    AuthConfig.Parser authParser = AuthConfig.newParser(BASE_NAME);
    StackdriverConfig.Parser stackdriverParser = StackdriverConfig.newParser(BASE_NAME);
    configKeys.addAll(authParser.configKeys());
    configKeys.addAll(stackdriverParser.configKeys());
    logParsedValues(config, configKeys);
    AuthConfig auth = authParser.parse(config);
    PubSubConfig pubsub = PubSubConfig.parse(config);
    SinkConfig sink = SinkConfig.parse(config, auth.getCredentials());
    StackdriverConfig stackdriver = stackdriverParser.parse(config);
    LptsConfig lpts = LptsConfig.parse(config, auth.getCredentials());

    return new SpezConfig(auth, pubsub, sink, stackdriver, lpts);
  }

  public AuthConfig getAuth() {
    return auth;
  }

  public PubSubConfig getPubSub() {
    return pubsub;
  }

  public SinkConfig getSink() {
    return sink;
  }

  public StackdriverConfig getStackdriver() {
    return stackdriver;
  }

  public LptsConfig getLpts() {
    return lpts;
  }

  /** baseMetadata getter. */
  public Map<String, String> getBaseMetadata() {
    Map<String, String> base = Maps.newHashMap();
    base.put(SINK_INSTANCE_KEY, sink.getInstance());
    base.put(SINK_DATABASE_KEY, sink.getDatabase());
    base.put(SINK_TABLE_KEY, sink.getTable());
    base.put(PUBSUB_TOPIC_KEY, pubsub.getTopic());
    return base;
  }
}
