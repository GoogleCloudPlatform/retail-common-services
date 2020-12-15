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

package com.google.spez.common;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class AuthConfig {
  public static class Parser {
    private final String AUTH_CLOUD_SECRETS_DIR_KEY;
    private final String AUTH_CREDENTIALS_KEY;
    private final String AUTH_SCOPES_KEY;

    public Parser(String baseKeyPath) {
      AUTH_CLOUD_SECRETS_DIR_KEY = baseKeyPath + ".auth.cloud_secrets_dir";
      AUTH_CREDENTIALS_KEY = baseKeyPath + ".auth.credentials";
      AUTH_SCOPES_KEY = baseKeyPath + ".auth.scopes";
    }

    /** AuthConfig value object parser. */
    public AuthConfig parse(Config config) {
      return new AuthConfig(
          AUTH_CLOUD_SECRETS_DIR_KEY,
          config.getString(AUTH_CLOUD_SECRETS_DIR_KEY),
          config.getString(AUTH_CREDENTIALS_KEY),
          config.getStringList(AUTH_SCOPES_KEY));
    }

    public List<String> configKeys() {
      return List.of(AUTH_CLOUD_SECRETS_DIR_KEY, AUTH_CREDENTIALS_KEY, AUTH_SCOPES_KEY);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(AuthConfig.class);
  private final String secretsDirKey;
  private final String cloudSecretsDir;
  private final String credentialsFile;
  private final ImmutableList<String> scopes;
  private GoogleCredentials credentials;

  /** AuthConfig value object constructor. */
  public AuthConfig(
      String secretsDirKey, String cloudSecretsDir, String credentialsFile, List<String> scopes) {
    this.secretsDirKey = secretsDirKey;
    this.cloudSecretsDir = cloudSecretsDir;
    this.credentialsFile = credentialsFile;
    this.scopes = ImmutableList.copyOf(scopes);
  }

  public static Parser newParser(String baseKeyPath) {
    return new Parser(baseKeyPath);
  }

  /** Credentials getter. */
  public GoogleCredentials getCredentials() {
    if (credentials != null) {
      return credentials;
    }

    try {
      var path = Paths.get(cloudSecretsDir, credentialsFile);
      if (!path.toFile().exists()) {
        var dir = new java.io.File(cloudSecretsDir);
        if (!dir.exists()) {
          throw new RuntimeException(secretsDirKey + " '" + cloudSecretsDir + "' does not exist");
        }
        var listing = java.util.Arrays.asList(dir.list());
        var suggest = new StringBuilder();
        if (listing.size() > 0) {
          var joiner = new java.util.StringJoiner("', or '");
          for (var file : listing) {
            joiner.add(file);
          }
          var candidates = joiner.toString();
          suggest.append(", did you mean '").append(candidates).append("'");
        }
        log.error(
            "{} does not exist in directory {}{}",
            credentialsFile,
            cloudSecretsDir,
            suggest.toString());
      }
      var stream = new FileInputStream(path.toFile());
      credentials = GoogleCredentials.fromStream(stream).createScoped(scopes);
      return credentials;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
