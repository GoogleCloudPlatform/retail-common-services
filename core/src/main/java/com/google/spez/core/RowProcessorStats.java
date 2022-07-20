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

import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowProcessorStats {
  private static final Logger log = LoggerFactory.getLogger(RowProcessorStats.class);

  private final Instant then = Instant.now();
  private final AtomicLong records = new AtomicLong(0);
  private final AtomicLong errors = new AtomicLong(0);
  private final AtomicLong published = new AtomicLong(0);
  private final Runtime runtime = Runtime.getRuntime();
  private final NumberFormat formatter = NumberFormat.getInstance();

  public void incRecords() {
    records.incrementAndGet();
  }

  public void incErrors() {
    errors.incrementAndGet();
  }

  public void incPublished() {
    published.incrementAndGet();
  }

  /** log stats. */
  public void logStats() {
    Instant now = Instant.now();
    Duration d = Duration.between(then, now);
    long count = records.get();
    long err = errors.get();
    long pub = published.get();
    log.debug(
        "Processed {} records [errors: {} / published: {}] over the past {}",
        formatter.format(count),
        err,
        pub,
        d);
    log.debug(
        "Memory: {}free / {}tot / {}max",
        formatter.format(runtime.freeMemory()),
        formatter.format(runtime.totalMemory()),
        formatter.format(runtime.maxMemory()));
  }
}
