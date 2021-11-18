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

import com.typesafe.config.Config;
import io.opencensus.common.Duration;
import java.util.concurrent.TimeUnit;
import java.util.List;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class StackdriverConfig {
  public static class Parser {
    private final String projectIdKey;
    private final String samplingRateKey;
    private final String exportRateKey;

    public Parser(String baseKeyPath) {
      projectIdKey = baseKeyPath + ".project_id";
      samplingRateKey = baseKeyPath + ".trace_sampling_rate";
      exportRateKey = baseKeyPath + ".stats_export_rate";
    }

    /** StackdriverConfig value object parser. */
    public StackdriverConfig parse(Config config) {
      double samplingRate = config.getDouble(samplingRateKey);
      if (samplingRate < 0.0 || samplingRate > 1.0) {
        throw new RuntimeException(
            "Invalid trace_sampling_rate '"
                + samplingRate
                + "' must be a value between 0.0 and 1.0 inclusive");
      }
      Duration exportRate = Duration.fromMillis(config.getDuration(exportRateKey, TimeUnit.MILLISECONDS));
      return new StackdriverConfig(config.getString(projectIdKey), samplingRate, exportRate);
    }

    public List<String> configKeys() {
      return List.of(projectIdKey, samplingRateKey, exportRateKey);
    }
  }

  private final String projectId;
  private final double samplingRate;
  private final Duration exportRate;

  /** SinkConfig value object constructor. */
  private StackdriverConfig(String projectId, double samplingRate, Duration exportRate) {
    this.projectId = projectId;
    this.samplingRate = samplingRate;
    this.exportRate = exportRate;
  }

  public static Parser newParser(String baseKeyPath) {
    return new Parser(baseKeyPath + ".stackdriver");
  }

  /** projectId getter. */
  public String getProjectId() {
    return projectId;
  }

  /** samplingRate getter. */
  public double getSamplingRate() {
    return samplingRate;
  }

  /** exportRate getter. */
  public Duration getExportRate() {
    return exportRate;
  }
}
