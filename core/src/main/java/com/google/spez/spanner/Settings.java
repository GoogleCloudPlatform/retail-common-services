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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.value.AutoValue;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@AutoValue
public abstract class Settings {
  public abstract GoogleCredentials credentials();

  public abstract String projectId();

  public abstract String instance();

  public abstract String database();

  public abstract String path();

  public abstract int poolSize();

  public abstract ScheduledExecutorService scheduler();

  public abstract int stalenessCheck();

  public static Builder newBuilder() {
    return new AutoValue_Settings.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    private static final int DEFAULT_POOL_SIZE = 4;
    private static final int DEFAULT_STALENESS_CHECK = 30;

    public abstract Builder setCredentials(GoogleCredentials credentials);

    public abstract Builder setProjectId(String projectId);

    abstract String projectId();

    public abstract Builder setInstance(String instance);

    abstract String instance();

    public abstract Builder setDatabase(String database);

    abstract String database();

    public abstract Builder setPoolSize(int poolSize);

    abstract Optional<Integer> poolSize();

    public abstract Builder setScheduler(ScheduledExecutorService scheduler);

    abstract Optional<ScheduledExecutorService> scheduler();

    public abstract Builder setStalenessCheck(int minutes);

    abstract Optional<Integer> stalenessCheck();

    abstract Builder setPath(String path);

    abstract Settings autoBuild();

    public Settings build() {
      if (!poolSize().isPresent()) {
        setPoolSize(DEFAULT_POOL_SIZE);
      }
      if (!scheduler().isPresent()) {
        setScheduler(Executors.newScheduledThreadPool(2));
      }
      if (!stalenessCheck().isPresent()) {
        setStalenessCheck(DEFAULT_STALENESS_CHECK);
      }
      setPath(
          String.format(
              "projects/%s/instances/%s/databases/%s", projectId(), instance(), database()));
      return autoBuild();
    }
  }
}
