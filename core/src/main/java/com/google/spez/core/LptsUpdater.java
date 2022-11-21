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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.pubsub.v1.PubsubMessage;
import com.google.spez.common.ListenableFutureErrorHandler;
import com.google.spez.common.UsefulExecutors;
import com.google.spez.pubsub.PubSubConsumer;
import com.google.spez.pubsub.PubSubFactory;
import com.google.spez.pubsub.PubSubListener;
import com.google.spez.spanner.Database;
import com.google.spez.spanner.DatabaseFactory;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
class LptsUpdater implements PubSubListener {
  private static final Logger log = LoggerFactory.getLogger(LptsUpdater.class);

  static class SpannerUpdater {
    private static final String LPTS_COLUMN_NAME = "LastProcessedTimestamp";
    private final SpezConfig config;
    private final Database lptsDatabase;
    private final AtomicReference<String> largestTimestamp;
    private String lastTimestamp = "";

    SpannerUpdater(
        SpezConfig config, Database lptsDatabase, AtomicReference<String> largestTimestamp) {
      this.config = config;
      this.lptsDatabase = lptsDatabase;
      this.largestTimestamp = largestTimestamp;
    }

    public void close() {
      try {
        lptsDatabase.close();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    private String buildUpdateQuery(String timestamp) {
      return new StringBuilder()
          .append("UPDATE ")
          .append(config.getLpts().getTable())
          .append(" SET ")
          .append(LPTS_COLUMN_NAME)
          .append(" = '")
          .append(timestamp)
          .append("' WHERE instance = '")
          .append(config.getSink().getInstance())
          .append("' AND database = '")
          .append(config.getSink().getDatabase())
          .append("' AND table = '")
          .append(config.getSink().getTable())
          .append("' THEN RETURN *")
          .toString();
    }

    public void maybeUpdate() {
      try {
        var timestamp = largestTimestamp.get();
        log.debug("Maybe Update? timestamp {}", timestamp);
        if (timestamp.equals("")) {
          return;
        }
        log.debug("Comparing lastTimestamp {} to timestamp {}", lastTimestamp, timestamp);
        if (lastTimestamp.compareTo(timestamp) < 0) {
          var query = buildUpdateQuery(timestamp);
          log.debug("Updating lpts to timestamp {} with query {}", timestamp, query);
          var result = lptsDatabase.executeMutate(buildUpdateQuery(timestamp));
          // result.next();
          // log.info("result {}", result.getCurrentRow());
          lastTimestamp = timestamp;
        }
      } catch (Exception ex) {
        log.error("Caught exception while updating lpts", ex);
      }
    }
  }

  private final ListeningScheduledExecutorService scheduler;
  private final AtomicReference<String> largestTimestamp = new AtomicReference<String>("");
  private SpannerUpdater spannerUpdater;
  private PubSubConsumer consumer;

  private LptsUpdater(ListeningScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
  }

  private void setSpannerUpdater(SpannerUpdater spannerUpdater) {
    Preconditions.checkNotNull(spannerUpdater, "spannerUpdater can not be null");
    Preconditions.checkState(
        this.spannerUpdater == null, "setSpannerUpdater can only be called once");
    this.spannerUpdater = spannerUpdater;
  }

  private void setConsumer(PubSubConsumer consumer) {
    Preconditions.checkNotNull(consumer, "consumer can not be null");
    Preconditions.checkState(this.consumer == null, "setConsumer can only be called once");
    this.consumer = consumer;
  }

  public static LptsUpdater create(SpezConfig config, ListeningScheduledExecutorService scheduler) {
    LptsUpdater updater = new LptsUpdater(scheduler);
    PubSubConsumer consumer = PubSubFactory.createLedgerConsumer(config.getPubSub(), updater);
    Database lptsDatabase = DatabaseFactory.openLptsDatabase(config.getLpts());
    SpannerUpdater spannerUpdater =
        new SpannerUpdater(config, lptsDatabase, updater.largestTimestamp);
    updater.setSpannerUpdater(spannerUpdater);
    updater.setConsumer(consumer);
    return updater;
  }

  public static LptsUpdater create(SpezConfig config) {
    return create(config, UsefulExecutors.listeningScheduler(1));
  }

  @Override
  public void onMessage(PubsubMessage message) {
    messages.incrementAndGet();
    // get the timestamp
    var timestamp = message.getAttributesMap().get(SpezConfig.SINK_TIMESTAMP_KEY);
    // log.info("Got timestamp {}", timestamp);
    largestTimestamp.accumulateAndGet(
        timestamp,
        (String currentValue, String newValue) -> {
          if (currentValue.compareTo(newValue) < 0) {
            log.debug("Updating atomic lpts to timestamp {}", newValue);
            return newValue;
          }
          return currentValue;
        });
  }

  public void start() {
    var future =
        scheduler.scheduleAtFixedRate(
            spannerUpdater::maybeUpdate, 0, 30_000, TimeUnit.MILLISECONDS);
    ListenableFutureErrorHandler.create(
        scheduler,
        future,
        (throwable) -> {
          log.error("LptsUpdater scheduled task error", throwable);
        });
    consumer.open();
  }

  public void stop() {
    consumer.close();
    spannerUpdater.close();
  }

  private final Instant then = Instant.now();
  private final AtomicLong messages = new AtomicLong(0);
  private long previousMessages = 0;
  private Instant lastLog = then;
  private final Runtime runtime = Runtime.getRuntime();
  private final NumberFormat formatter = NumberFormat.getInstance();

  public void logStats() {
    var timestamp = largestTimestamp.get();
    Instant now = Instant.now();
    Duration d = Duration.between(then, now);
    long messagesTotal = messages.get();
    long messagesNow = messagesTotal - previousMessages;
    Duration logDuration = Duration.between(lastLog, now);
    Long inSeconds = logDuration.getSeconds();
    double messagesPerSecond = messagesNow / inSeconds.doubleValue();
    log.info(
        "Processed {} pubsub messages over the past {} rate: {} records/sec. Largest timestamp seen: {}",
        formatter.format(messagesNow),
        logDuration,
        messagesPerSecond,
        timestamp);
    previousMessages = messagesTotal;
  }
}
