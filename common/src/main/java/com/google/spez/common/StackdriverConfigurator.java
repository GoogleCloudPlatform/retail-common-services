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

import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;

public class StackdriverConfigurator {
  /**
   * configures stackdriver based on the given config.
   *
   * @param config stackdriver config object
   * @param authConfig auth config object
   * @throws IOException exception thrown during network i/o
   */
  public static void setupStackdriver(StackdriverConfig config, AuthConfig authConfig)
      throws IOException {
    // For demo purposes, always sample
    TraceConfig traceConfig = Tracing.getTraceConfig();
    traceConfig.updateActiveTraceParams(
        traceConfig
            .getActiveTraceParams()
            .toBuilder()
            .setSampler(Samplers.alwaysSample()) // TODO(pdex): move to config
            .build());

    // Create the Stackdriver trace exporter
    StackdriverTraceExporter.createAndRegister(
        StackdriverTraceConfiguration.builder()
            .setProjectId(config.getProjectId())
            .setCredentials(authConfig.getCredentials())
            .build());
  }
}
