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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auto.value.AutoValue;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.*;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * This class creates and schedules the spanner event tailer
 *
 * <p>The {@code SpannerTailer} class creates your tailer and schedules it to tail the configured
 * spanner table based on your configuration options. For each row the tailer receives, it will
 * allow you to create a corresponding avro record and publish that record to the configured pub /
 * sub topic. This class assumes you have your Google Cloud credentials set up as described in the
 * {@code README.md} for this git repo.
 *
 * <p>As the tailer tails on a fixed interval, there is no mechanism for retry as the poller will
 * just process the records on its next poll attempt based on the last processed timestamp.
 *
 * <p>For the first run, the poller will try to configure the lastTimestamp based on the last record
 * published to the configured pub / sub topic and updated to the metadata Cloud Spanner table via
 * the lastProcessedTimestamp Cloud Function (if deployed).
 */
public class SpannerTailer {
  private static final Logger log = LoggerFactory.getLogger(SpannerTailer.class);

  private final ListeningExecutorService service;
  private final ScheduledExecutorService scheduler;
  private final HashFunction hasher;
  private final Funnel<Event> eventFunnel;
  private final BloomFilter<Event> bloomFilter;
  private final Map<HashCode, Timestamp> eventMap;

  private Spanner spanner;
  private String lastProcessedTimestamp;
  private boolean firstRun = true;

  public SpannerTailer(int threadPool, int maxEventCount) {
    this.scheduler = Executors.newScheduledThreadPool(threadPool);
    this.service =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                threadPool,
                new ThreadFactoryBuilder().setNameFormat("SpannerTailer Acceptor").build()));
    this.hasher = Hashing.murmur3_128();
    this.eventFunnel =
        (Funnel<Event>)
            (event, into) ->
                into.putString(event.uuid(), Charsets.UTF_8)
                    .putString(event.timestamp().toString(), Charsets.UTF_8);

    this.bloomFilter = BloomFilter.create(eventFunnel, maxEventCount, 0.01);
    this.eventMap = new ConcurrentHashMap<>(maxEventCount);
  }

  private Spanner getSpanner(String projectId, String instanceName, String dbName) {

    final RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setInitialRpcTimeout(Duration.ofSeconds(10L)) // TODO(XJDR): Move this to config file
            .setMaxRpcTimeout(Duration.ofSeconds(20L)) // TODO(XJDR): Move this to config file
            .setMaxAttempts(5) // TODO(XJDR): Move this to config file
            .setTotalTimeout(Duration.ofSeconds(30L)) // TODO(XJDR): Move this to config file
            .build();

    final SessionPoolOptions sessionPoolOptions =
        SessionPoolOptions.newBuilder()
            .setMinSessions(4) // TODO(XJDR): Move this to config file
            // Since we have no read-write transactions, we can set the write session fraction to
            // 0.
            .setWriteSessionsFraction(0)
            .build();

    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder()
            .setSessionPoolOption(sessionPoolOptions)
            .setNumChannels(4); // TODO(XJDR): Move this to config file
    try {
      builder
          .getSpannerStubSettingsBuilder()
          .applyToAllUnaryMethods(
              input -> {
                input.setRetrySettings(retrySettings);
                return null;
              });
    } catch (Exception e) {
      log.error("Error Creating Retry Setting");

      // TODO(xjdr): Do something better here
      // stop();
      // System.exit(1);
    }

    return builder.build().getService();
  }

  private DatabaseClient getDbClient(String projectId, String instanceName, String dbName) {
    if (spanner == null) {
      spanner = getSpanner(projectId, instanceName, dbName);
    } else if (spanner.isClosed()) {
      spanner = getSpanner(projectId, instanceName, dbName);
    }

    final DatabaseId db = DatabaseId.of(projectId, instanceName, dbName);
    final String clientProject = spanner.getOptions().getProjectId();

    if (!db.getInstanceId().getProject().equals(clientProject)) {
      log.error(
          "Invalid project specified. Project in the database id should match"
              + "the project name set in the environment variable GCLOUD_PROJECT. Expected: "
              + clientProject);

      // TODO(xjdr): Do something better here
      // stop();
      // System.exit(1);
    }

    final DatabaseClient dbClient = spanner.getDatabaseClient(db);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread("spannerShutdown") {
              @Override
              public void run() {
                System.out.println("System Runtime Shutdown Hook has been called");
                if (!spanner.isClosed()) {
                  spanner.close();
                }
              }
            });

    return dbClient;
  }

  /**
   * Parses the schema from the Cloud Spanner table and creates an Avro {@code SchemSet} to enable
   * you to dynamically serialize Cloud Spanner events as an Avro record for publshing to an event
   * ledger.
   *
   * @param projectId GCP Project Id
   * @param instanceName Cloud Spanner Instance Name
   * @param dbName Cloud Spanner Database Name
   * @param tableName Cloud Spanner Table Name
   */
  public SpannerToAvro.SchemaSet getSchema(
      String projectId, String instanceName, String dbName, String tableName) {
    Preconditions.checkNotNull(projectId);
    Preconditions.checkNotNull(instanceName);
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(tableName);

    final Statement schemaQuery =
        Statement.newBuilder(
                "SELECT COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=@tablename ORDER BY ORDINAL_POSITION")
            .bind("tablename")
            .to(tableName)
            .build();
    final DatabaseClient dbClient = getDbClient(projectId, instanceName, dbName);

    try (ReadOnlyTransaction readOnlyTransaction = dbClient.readOnlyTransaction();
        ResultSet resultSet = readOnlyTransaction.executeQuery(schemaQuery)) {
      log.debug("Processing Schema");
      return SpannerToAvro.GetSchema("test", "avroNamespace", resultSet);
    } catch (Exception e) {

    }

    return null;
  }

  private boolean uniq(Event e) {
    final HashCode eventHashCode = hasher.newHasher().putObject(e, eventFunnel).hash();

    if (bloomFilter.mightContain(e)) {
      if (eventMap.putIfAbsent(eventHashCode, e.timestamp()) == null) {
        bloomFilter.put(e);

        return true;
      }
    } else {
      bloomFilter.put(e);
      eventMap.put(eventHashCode, e.timestamp());

      return true;
    }

    return false;
  }

  /**
   * Starts the tailing process by scheduling a query at a configurable fixed delay with a read only
   * query. To maintain state to allow only new events to be processed, a {@code Timestamp} field is
   * required in the table and will be maintained here. As of now, that field is hardcoded as
   * `Timestamp`.
   *
   * @param handler SpannerEventHandler to be triggered on each new record for processing
   * @param bucketSize Size of the consistent hashing algorythm for thread poll managment
   * @param threadCount xx
   * @param pollRate Time delay in Milliseconds between polls
   * @param projectId GCP Project ID
   * @param instanceName Cloud Spanner Instance Name
   * @param dbName Cloud Spanner Database Name
   * @param tableName Cloud Spanner table Name
   * @param lptsTableName Cloud Spanner table name for lastProcessedTimestamp (as populated by the
   *     lastProcessedTimestamp Cloud Function
   * @param recordLimit The limit for the max number of records returned for a given query
   * @param vacuumRate xxx
   * @param eventCacheTTL xxx
   */
  public ScheduledFuture<?> start(
      SpannerEventHandler handler,
      int bucketSize,
      int threadCount,
      int pollRate,
      String projectId,
      String instanceName,
      String dbName,
      String tableName,
      String lptsTableName,
      String recordLimit,
      int vacuumRate,
      long eventCacheTTL) {

    Preconditions.checkNotNull(handler);
    Preconditions.checkArgument(bucketSize > 0);
    Preconditions.checkArgument(threadCount > 0);
    Preconditions.checkArgument(pollRate > 0);
    Preconditions.checkNotNull(projectId);
    Preconditions.checkNotNull(instanceName);
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(lptsTableName);
    Preconditions.checkNotNull(recordLimit);
    Preconditions.checkArgument(vacuumRate > 0);
    Preconditions.checkArgument(eventCacheTTL > 0);

    try {
      final ScheduledFuture<?> poller =
          scheduler.scheduleAtFixedRate(
              () -> {
                poll(
                    projectId,
                    instanceName,
                    dbName,
                    tableName,
                    lptsTableName,
                    recordLimit,
                    handler,
                    bucketSize);
              },
              0,
              500,
              TimeUnit.MILLISECONDS);
      return poller;

    } catch (Exception e) {

    }

    // TODO(xjdr): Don't do this.
    return null;
  }

  private void poll(
      String projectId,
      String instanceName,
      String dbName,
      String tableName,
      String lptsTableName,
      String recordLimit,
      SpannerEventHandler handler,
      int bucketSize) {

    final DatabaseClient dbClient = getDbClient(projectId, instanceName, dbName);
    final Statement lptsQuery = Statement.newBuilder("SELECT * FROM " + lptsTableName).build();

    if (firstRun) {

      try (ReadOnlyTransaction readOnlyTransaction = dbClient.readOnlyTransaction();
          ResultSet resultSet = readOnlyTransaction.executeQuery(lptsQuery)) {
        while (resultSet.next()) {
          String startingTimestamp = resultSet.getString("LastProcessedTimestamp");
          Timestamp tt = Timestamp.parseTimestamp(startingTimestamp);
          lastProcessedTimestamp = tt.toString();
        }
      }

      firstRun = false;
    }

    final Statement pollQuery =
        Statement.newBuilder(
                "SELECT * FROM "
                    + tableName
                    + " "
                    + "WHERE Timestamp > '"
                    + lastProcessedTimestamp
                    + "' "
                    + "ORDER BY Timestamp ASC LIMIT "
                    + recordLimit)
            .build();

    final ListenableFuture<ImmutableList<Struct>> resultSet =
        service.submit(
            new Callable<ImmutableList<Struct>>() {
              @Override
              public ImmutableList<Struct> call() {
                final List<Struct> sl = new ArrayList<>();
                try (ReadOnlyTransaction readOnlyTransaction = dbClient.readOnlyTransaction();
                    ResultSet resultSet = readOnlyTransaction.executeQuery(pollQuery)) {
                  while (resultSet.next()) {
                    sl.add(resultSet.getCurrentRowAsStruct());
                  }
                } catch (Exception e) {

                }

                return ImmutableList.copyOf(sl);
              }
            });

    final AsyncFunction<ImmutableList<Struct>, List<Boolean>> processEvent =
        new AsyncFunction<ImmutableList<Struct>, List<Boolean>>() {
          @Override
          public ListenableFuture<List<Boolean>> apply(ImmutableList<Struct> structList) {
            final List<ListenableFuture<Boolean>> fl = new ArrayList<>();
            structList.forEach(
                s -> {
                  fl.add(
                      service.submit(
                          new Callable<Boolean>() {
                            @Override
                            public Boolean call() {
                              return processRow(handler, s);
                            }
                          }));
                });
            return Futures.successfulAsList(fl);
          }
        };

    final ListenableFuture<ImmutableList<Struct>> resultSetTimeout =
        Futures.withTimeout(resultSet, 5, TimeUnit.SECONDS, scheduler);

    final ListenableFuture<List<Boolean>> pollingFuture =
        Futures.transformAsync(resultSet, processEvent, service);

    try {
      pollingFuture
          .get()
          .forEach(
              f -> {
                log.debug(
                    "Successfully Processed: "
                        + f
                        + " - "
                        + LocalTime.now(ZoneId.of("America/Los_Angeles")));
              });
    } catch (Exception e) {
      log.error("Polling Session Failed: " + e, e);
    }
  }

  private Boolean processRow(SpannerEventHandler handler, Struct struct) {
    final String uuid = Long.toString(struct.getLong("UUID"));
    final Timestamp ts = struct.getTimestamp("Timestamp");
    final Event e = Event.create(uuid, ts);

    if (uniq(e)) {
      final HashCode sortingKeyHashCode = hasher.newHasher().putBytes(uuid.getBytes(UTF_8)).hash();
      final int bucket = Hashing.consistentHash(sortingKeyHashCode, 12);

      handler.process(bucket, struct, ts.toString());
      lastProcessedTimestamp = ts.toString();
    }

    return true;
  }

  @AutoValue
  abstract static class Event {
    static Event create(String uuid, Timestamp timestamp) {
      return new AutoValue_SpannerTailer_Event(uuid, timestamp);
    }

    abstract String uuid();

    abstract Timestamp timestamp();
  }
}
