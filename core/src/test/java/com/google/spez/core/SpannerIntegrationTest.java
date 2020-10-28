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

import com.google.cloud.spanner.*;
import com.google.spez.common.Inexcusables;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerIntegrationTest {
  private static final Logger log = LoggerFactory.getLogger(SpannerIntegrationTest.class);
  protected static final String INSTANCE_ID = "integration-test-instance";
  protected static String projectId;
  private static InstanceAdminClient instanceAdmin;

  @BeforeAll
  public static void setUpBase() {
    var credsFile = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
    projectId = System.getenv("GOOGLE_CLOUD_PROJECT");
    if (!Files.exists(Paths.get(credsFile))) {
      log.error("Could not find a valid credentials file at '{}'", credsFile);
      log.error(
          "Please set GOOGLE_APPLICATION_CREDENTIALS to the location of your credentials file.");
      throw new IllegalStateException();
    }
    if (projectId.isBlank()) {
      log.error("Please set GOOGLE_CLOUD_PROJECT to a valid GCP project id");
      throw new IllegalStateException();
    }
    SpannerOptions options =
        SpannerOptions.newBuilder()
            .setProjectId(projectId)
            .setAutoThrottleAdministrativeRequests()
            .build();
    Spanner client = options.getService();
    instanceAdmin = client.getInstanceAdminClient();
    var instanceInfo =
        InstanceInfo.newBuilder(InstanceId.of(projectId, INSTANCE_ID))
            .setDisplayName(INSTANCE_ID)
            .setInstanceConfigId(InstanceConfigId.of(projectId, "regional-us-central1"))
            .setNodeCount(1)
            .build();
    var result = instanceAdmin.createInstance(instanceInfo);
    Inexcusables.getInexcusably(result);
  }

  @AfterAll
  public static void tearDownBase() {
    instanceAdmin.deleteInstance(INSTANCE_ID);
  }
}
