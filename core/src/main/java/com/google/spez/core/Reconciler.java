package com.google.spez.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

public class Reconciler {
  private static final Logger log = LoggerFactory.getLogger(Reconciler.class);
  private final FutureCallback<Object> futureErrorLogger = new FutureCallback<Object>() {
    @Override
    public void onSuccess(Object result) {
    }

    @Override
    public void onFailure(Throwable t) {
      log.error("Reconcilier Exception: ", t);
    }
  };

  final static String EPOCH = "1970-01-01T00:00:00.000000Z";
  public static class Key implements Comparable<Key> {
    final String messageId;
    final String commitTimestamp;
    final String uuid;

    public Key(String messageId, String commitTimestamp, String uuid) {
      this.messageId = messageId;
      this.commitTimestamp = commitTimestamp;
      this.uuid = uuid;
    }

    @Override
    public int compareTo(Key other) {
      return ComparisonChain.start()
        .compare(commitTimestamp, other.commitTimestamp)
        .compare(uuid, other.uuid)
        .compare(messageId, other.messageId)
        .result();
    }
  }

  public static class Record {
    // TODO(pdex): maybe remove?
    final Instant created = Instant.now();
    int published = 0;
    int received = 0;

    public void incPublished() {
      published++;
    }

    public void incReceived() {
      received++;
    }

    public boolean completed() {
      return published > 0 && received > 0;
    }
  }

  private final ScheduledExecutorService _scheduler = Executors.newSingleThreadScheduledExecutor();
  private final ListeningScheduledExecutorService executor = MoreExecutors.listeningDecorator(_scheduler);
  private final SortedMap<Key, Record> bookkeeping = new TreeMap<>();
  private String lastProcessedTimestamp = EPOCH;

  private Record getOrDefault(Key key) {
    var record = bookkeeping.get(key);
    if (record == null) {
      record = new Record();
      bookkeeping.put(key, record);
    }
    return record;
  }

  @VisibleForTesting
  SortedMap<Key, Record> getBookkeeping() { return bookkeeping; }

  private void maybeUpdate(String messageId, String commitTimestamp, String uuid, String verb, Consumer<Record> increment) {
    if (!(commitTimestamp.compareTo(lastProcessedTimestamp) < 0)) {
      var key = new Key(messageId, commitTimestamp, uuid);
      Record record = getOrDefault(key);
      increment.accept(record);
      calculateLastProcessedTimestamp();
    } else {
      log.warn("Reconciler notified of {} message with commit timestamp '{}' that is earlier than last processed timestamp '{}'", verb, commitTimestamp, lastProcessedTimestamp);
    }
  }

  @VisibleForTesting
  void published(String messageId, String commitTimestamp, String uuid) {
    maybeUpdate(messageId, commitTimestamp, uuid, "published", (Record record) -> record.incPublished());
    /*
    // TODO(pdex): check commitTimestamp against lpts
    var key = new Key(messageId, commitTimestamp, uuid);
    Record record = getOrDefault(key);
    record.incPublished();
    //published.put(commitTimestamp, new ReconciliationSubscriber.ReconcileRecord(messageId, commitTimestamp, uuid));
    // reconcile
    calculateLastProcessedTimestamp();

     */
  }

  @VisibleForTesting
  void received(String messageId, String commitTimestamp, String uuid) {
    maybeUpdate(messageId, commitTimestamp, uuid, "received", (Record record) -> record.incReceived());
    /*
    // TODO(pdex): check commitTimestamp against lpts
    var key = new Key(messageId, commitTimestamp, uuid);
    Record record = getOrDefault(key);
    record.incReceived();
    //received.put(commitTimestamp, new ReconciliationSubscriber.ReconcileRecord(messageId, commitTimestamp, uuid));
    // reconcile
    calculateLastProcessedTimestamp();
     */
  }

  @VisibleForTesting
  String calculateLastProcessedTimestamp() {
    var completed = Lists.newArrayList();
    for (var entry : bookkeeping.entrySet()) {
      if (entry.getValue().completed()) {
        lastProcessedTimestamp = entry.getKey().commitTimestamp;
        completed.add(entry.getKey());
      } else {
        break;
      }
    }
    for (var key : completed) {
      bookkeeping.remove(key);
    }
    return lastProcessedTimestamp;
  }

  public void onPublish(String messageId, String commitTimestamp, String uuid) {
    var future = executor.submit(() -> this.published(messageId, commitTimestamp, uuid));
    Futures.addCallback(future, futureErrorLogger, MoreExecutors.directExecutor());
  }

  public void onReceive(String messageId, String commitTimestamp, String uuid) {
    var future = executor.submit(() -> received(messageId, commitTimestamp, uuid));
    Futures.addCallback(future, futureErrorLogger, MoreExecutors.directExecutor());
  }

  public String getLastProcessedTimestamp() {
    try {
      var result = executor.<String>submit(() -> calculateLastProcessedTimestamp());
      return result.get();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void stop() {
    executor.shutdown();
  }
}
