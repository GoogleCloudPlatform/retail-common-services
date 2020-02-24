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
package com.google.spannerclient;

import com.google.api.client.util.Preconditions;

public class QueryOptions {
  private final boolean readOnly;
  private final boolean strong;
  private final boolean stale;
  private final int maxStaleness;

  private QueryOptions(boolean readOnly, boolean strong, boolean stale, int maxStaleness) {
    if (strong) {
      Preconditions.checkArgument(
          stale == false, "Cannot have both Strong Reads and Stale Reads configured");
    }
    if (stale) {
      Preconditions.checkArgument(
          strong == false, "Cannot have both Strong Reads and Stale Reads configured");
      Preconditions.checkArgument(maxStaleness > 0);
    }
    this.readOnly = readOnly == true;
    this.strong = strong == true;
    this.stale = stale == true;
    this.maxStaleness = maxStaleness;
  }

  public boolean readOnly() {
    return readOnly;
  }

  public boolean strong() {
    return strong;
  }

  public boolean stale() {
    return stale;
  }

  public int maxStaleness() {
    return maxStaleness;
  }

  public static QueryOptions DEFAULT() {
    return new QueryOptions(false, false, false, 0);
  }

  public static QueryOptions.Builder newBuilder() {
    return new QueryOptions.Builder();
  }

  public static class Builder {
    private boolean readOnly;
    private boolean strong;
    private boolean stale;
    private int maxStaleness;

    private Builder() {}

    public Builder setReadOnly(boolean b) {
      this.readOnly = b;

      return this;
    }

    public Builder setStrong(boolean b) {
      this.strong = b;

      return this;
    }

    public Builder setStale(boolean b) {
      this.stale = b;

      return this;
    }

    public Builder setMaxStaleness(int i) {
      this.maxStaleness = i;

      return this;
    }

    public QueryOptions build() {

      return new QueryOptions(this.readOnly, this.strong, this.stale, this.maxStaleness);
    }
  }
}
