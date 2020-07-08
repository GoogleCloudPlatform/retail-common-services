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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.spannerclient.Settings;
import com.typesafe.config.Config;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpezConfig {
  private static final Logger log = LoggerFactory.getLogger(SpezConfig.class);
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
  public static final List<String> CONFIG_KEYS =
      Arrays.asList(
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
          SINK_PROJECT_ID_KEY,
          SINK_INSTANCE_KEY,
          SINK_DATABASE_KEY,
          SINK_TABLE_KEY,
          SINK_UUID_COLUMN_KEY,
          SINK_TIMESTAMP_COLUMN_KEY,
          LPTS_PROJECT_ID_KEY,
          LPTS_INSTANCE_KEY,
          LPTS_DATABASE_KEY,
          LPTS_TABLE_KEY);
  public static final String SINK_UUID_KEY = "spez.sink.uuid";
  public static final String SINK_TIMESTAMP_KEY = "spez.sink.commit_timestamp";

  public static class AuthConfig {
    private final String cloudSecretsDir;
    private final String credentialsFile;
    private final ImmutableList<String> scopes;
    private GoogleCredentials credentials;

    public AuthConfig(String cloudSecretsDir, String credentialsFile, List<String> scopes) {
      this.cloudSecretsDir = cloudSecretsDir;
      this.credentialsFile = credentialsFile;
      this.scopes = ImmutableList.copyOf(scopes);
    }

    public static AuthConfig parse(Config config) {
      return new AuthConfig(
          config.getString(AUTH_CLOUD_SECRETS_DIR_KEY),
          config.getString(AUTH_CREDENTIALS_KEY),
          config.getStringList(AUTH_SCOPES_KEY));
    }

    public GoogleCredentials getCredentials() {
      if (credentials != null) {
        return credentials;
      }

      try {
        var path = Paths.get(cloudSecretsDir, credentialsFile);
        if (!path.toFile().exists()) {
          var dir = new java.io.File(cloudSecretsDir);
          if (!dir.exists()) {
            throw new RuntimeException(
                AUTH_CLOUD_SECRETS_DIR_KEY + " '" + cloudSecretsDir + "' does not exist");
          }
          var listing = java.util.Arrays.asList(dir.list());
          var suggest = "";
          if (listing.size() > 0) {
            var joiner = new java.util.StringJoiner("', or '");
            for (var file : listing) {
              joiner.add(file);
            }
            var candidates = joiner.toString();
            suggest = ", did you mean '" + candidates + "'";
          }
          log.error(
              "{} does not exist in directory {}{}", credentialsFile, cloudSecretsDir, suggest);
        }
        var stream = new FileInputStream(path.toFile());
        credentials = GoogleCredentials.fromStream(stream).createScoped(scopes);
        return credentials;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class PubSubConfig {
    private final String projectId;
    private final String topic;

    public PubSubConfig(String projectId, String topic) {
      this.projectId = projectId;
      this.topic = topic;
    }

    public static PubSubConfig parse(Config config) {
      return new PubSubConfig(
          config.getString(PUBSUB_PROJECT_ID_KEY), config.getString(PUBSUB_TOPIC_KEY));
    }

    public String getProjectId() {
      return projectId;
    }

    public String getTopic() {
      return topic;
    }
  }

  public static class SinkConfig {
    private final String projectId;
    private final String instance;
    private final String database;
    private final String table;
    private final String uuidColumn;
    private final String timestampColumn;
    private final GoogleCredentials credentials;

    public SinkConfig(
        String projectId,
        String instance,
        String database,
        String table,
        String uuidColumn,
        String timestampColumn,
        GoogleCredentials credentials) {
      this.projectId = projectId;
      this.instance = instance;
      this.database = database;
      this.table = table;
      this.uuidColumn = uuidColumn;
      this.timestampColumn = timestampColumn;
      this.credentials = credentials;
    }

    public static SinkConfig parse(Config config, GoogleCredentials credentials) {
      return new SinkConfig(
          config.getString(SINK_PROJECT_ID_KEY),
          config.getString(SINK_INSTANCE_KEY),
          config.getString(SINK_DATABASE_KEY),
          config.getString(SINK_TABLE_KEY),
          config.getString(SINK_UUID_COLUMN_KEY),
          config.getString(SINK_TIMESTAMP_COLUMN_KEY),
          credentials);
    }

    public String getProjectId() {
      return projectId;
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

    public Settings getSettings() {
      return Settings.newBuilder()
          .setProjectId(projectId)
          .setInstance(instance)
          .setDatabase(database)
          .setCredentials(credentials)
          .build();
    }

    public String getUuidColumn() {
      return uuidColumn;
    }

    public String getTimestampColumn() {
      return timestampColumn;
    }

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

    public static LptsConfig parse(Config config, GoogleCredentials credentials) {
      return new LptsConfig(
          config.getString(LPTS_PROJECT_ID_KEY),
          config.getString(LPTS_INSTANCE_KEY),
          config.getString(LPTS_DATABASE_KEY),
          config.getString(LPTS_TABLE_KEY),
          credentials);
    }

    public String getProjectId() {
      return projectId;
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

    public Settings getSettings() {
      return Settings.newBuilder()
          .setProjectId(projectId)
          .setInstance(instance)
          .setDatabase(database)
          .setCredentials(credentials)
          .build();
    }

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

    public String tablePath() {
      return new StringBuilder().append(databasePath()).append("/tables/").append(table).toString();
    }
  }

  private final AuthConfig auth;
  private final PubSubConfig pubsub;
  private final SinkConfig sink;
  private final LptsConfig lpts;

  public SpezConfig(AuthConfig auth, PubSubConfig pubsub, SinkConfig sink, LptsConfig lpts) {
    this.auth = auth;
    this.pubsub = pubsub;
    this.sink = sink;
    this.lpts = lpts;
  }

  public static void logParsedValues(Config config) {
    log.info("=============================================");
    log.info("Spez configured with the following properties");
    for (var key : CONFIG_KEYS) {
      if (key.equals(AUTH_SCOPES_KEY)) {
        log.info("{}={}", key, config.getStringList(key));
      } else {
        log.info("{}={}", key, config.getString(key));
      }
    }
    log.info("=============================================");
  }

  public static SpezConfig parse(Config config) {
    AuthConfig auth = AuthConfig.parse(config);
    PubSubConfig pubsub = PubSubConfig.parse(config);
    SinkConfig sink = SinkConfig.parse(config, auth.getCredentials());
    LptsConfig lpts = LptsConfig.parse(config, auth.getCredentials());
    logParsedValues(config);

    return new SpezConfig(auth, pubsub, sink, lpts);
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

  public LptsConfig getLpts() {
    return lpts;
  }

  public Map<String, String> getBaseMetadata() {
    Map<String, String> base = Maps.newHashMap();
    base.put(SINK_INSTANCE_KEY, sink.getInstance());
    base.put(SINK_DATABASE_KEY, sink.getDatabase());
    base.put(SINK_TABLE_KEY, sink.getTable());
    base.put(PUBSUB_TOPIC_KEY, pubsub.getTopic());
    return base;
  }
}
