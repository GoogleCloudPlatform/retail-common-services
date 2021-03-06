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

package com.google.spez.cdc;

import com.google.spez.common.StackdriverConfigurator;
import com.google.spez.core.SpezApp;
import com.google.spez.core.SpezConfig;
import com.typesafe.config.ConfigFactory;
import io.opencensus.contrib.zpages.ZPageHandlers;

class Main {
  public static void main(String[] args) {
    try {
      SpezConfig config = SpezConfig.parse(ConfigFactory.load());

      StackdriverConfigurator.setupStackdriver(config.getStackdriver(), config.getAuth());

      ZPageHandlers.startHttpServerAndRegisterAll(8887);

      SpezApp.run(config);
    } catch (Exception ex) {
      System.exit(-1);
    }
  }
}
