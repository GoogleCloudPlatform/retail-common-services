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

package com.google.spez.spanner;

import com.google.spez.core.SpezConfig;
import com.google.spez.spanner.internal.BothanDatabase;
import com.google.spez.spanner.internal.GaxDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseFactory {
  private static final Logger log = LoggerFactory.getLogger(DatabaseFactory.class);

  public static Database openDatabase(
      boolean useCustomClient, Settings settings, String databasePath) {
    log.info("Building database with path '{}'", databasePath);
    if (useCustomClient) {
      return BothanDatabase.openDatabase(settings);
    }
    return GaxDatabase.openDatabase(settings);
  }

  public static Database openLptsDatabase(SpezConfig.LptsConfig config) {
    return openDatabase(config.useCustomClient(), config.getSettings(), config.databasePath());
  }

  public static Database openSinkDatabase(SpezConfig.SinkConfig config) {
    return openDatabase(config.useCustomClient(), config.getSettings(), config.databasePath());
  }
}
