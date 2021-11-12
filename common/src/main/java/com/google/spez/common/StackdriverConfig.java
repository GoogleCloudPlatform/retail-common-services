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
import java.util.List;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class StackdriverConfig {
  public static class Parser {
    private final String projectIdKey;
    private final String samplingRateKey;

    public Parser(String baseKeyPath) {
      projectIdKey = baseKeyPath + ".project_id";
      samplingRateKey = baseKeyPath + ".trace_sampling_rate";
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
      return new StackdriverConfig(config.getString(projectIdKey), samplingRate);
    }

    public List<String> configKeys() {
      return List.of(projectIdKey, samplingRateKey);
    }
  }

  private final String projectId;
  private final double samplingRate;

  /** SinkConfig value object constructor. */
  private StackdriverConfig(String projectId, double samplingRate) {
    this.projectId = projectId;
    this.samplingRate = samplingRate;
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
}
