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

import com.google.cloud.Timestamp;
import com.google.common.base.Preconditions;
import com.google.spannerclient.Row;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerEvent {
  private static final Logger log = LoggerFactory.getLogger(SpannerTailer.class);

  private Row row;
  private Timestamp timestamp;
  private AtomicInteger lock = new AtomicInteger();

  public SpannerEvent() {}

  public void clear() {
    if (lock.compareAndSet(1, 0)) {
      this.row = null;
      this.timestamp = null;
    } else {
      log.error("Tried to clear an event that was not in use");
    }
  }

  public Row row() {
    return row;
  }

  public void set(Row row, String tsColName) {
    Preconditions.checkNotNull(row);
    Preconditions.checkNotNull(tsColName);

    if (lock.compareAndSet(0, 1)) {
      this.row = row;
      this.timestamp = row.getTimestamp(tsColName);
    } else {
      log.error("Tried to set an event that was still in use");
    }
  }

  public String timestamp() {
    return timestamp.toString();
  }
}
