/*
 * Copyright 2019 Google LLC
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

import com.google.cloud.Timestamp;
import com.google.spannerclient.Row;

public class SpannerEvent {

  private Row row;
  private Timestamp timestamp;

  public SpannerEvent() {}

  public void clear() {
    this.row = null;
    this.timestamp = null;
  }

  public Row row() {
    return row;
  }

  public void set(Row row, String tsColName) {
    this.row = row;
    this.timestamp = row.getTimestamp(tsColName);
  }

  public String timestamp() {
    return timestamp.toString();
  }
}
