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
  private long previouslyQueued = 0;
  private long previouslyPublished = 0;
  private long previouslyErrors = 0;
  private Instant lastLog = then;
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
    log.info(
        "Processed {} records [errors: {} / published: {}] over the past {}",
        formatter.format(count),
        err,
        pub,
        d);
    long queuedNow = count - previouslyQueued;
    long publishedNow = pub - previouslyPublished;
    long errorsNow = err - previouslyErrors;
    Duration logDuration = Duration.between(lastLog, now);
    Long inSeconds = logDuration.getSeconds();
    double recordsPerSecond = publishedNow / inSeconds.doubleValue();
    double queuedPerSecond = queuedNow / inSeconds.doubleValue();
    double errorsPerSecond = errorsNow / inSeconds.doubleValue();
    log.info(
        "Queued {} records over the past {} rate: {} records/sec",
        formatter.format(queuedNow),
        logDuration,
        queuedPerSecond);
    log.info(
        "Published {} records over the past {} rate: {} records/sec",
        formatter.format(publishedNow),
        logDuration,
        recordsPerSecond);
    log.info(
        "Encountered errors with {} records over the past {} rate: {} errors/sec",
        formatter.format(errorsNow),
        logDuration,
        errorsPerSecond);
    previouslyQueued = count;
    previouslyPublished = pub;
    previouslyErrors = err;
    lastLog = now;
    log.info(
        "Memory: {}free / {}tot / {}max",
        formatter.format(runtime.freeMemory()),
        formatter.format(runtime.totalMemory()),
        formatter.format(runtime.maxMemory()));
  }
}
