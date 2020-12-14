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

public class StackdriverConfig {
  public static class Parser {
    private final String PROJECT_ID_KEY;

    public Parser(String baseKeyPath) {
      PROJECT_ID_KEY = baseKeyPath + ".project_id";
    }

    /** StackdriverConfig value object parser. */
    public StackdriverConfig parse(Config config) {
      return new StackdriverConfig(config.getString(PROJECT_ID_KEY));
    }

    public List<String> configKeys() {
      return List.of(PROJECT_ID_KEY);
    }
  }

  private final String projectId;

  /** SinkConfig value object constructor. */
  private StackdriverConfig(String projectId) {
    this.projectId = projectId;
  }

  public static Parser newParser(String baseKeyPath) {
    return new Parser(baseKeyPath);
  }

  /** projectId getter. */
  public String getProjectId() {
    return projectId;
  }
}
