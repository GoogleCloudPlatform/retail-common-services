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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.spannerclient.Database;
import com.google.spannerclient.Query;
import com.google.spannerclient.QueryOptions;
import com.google.spannerclient.RowCursor;
import com.google.spannerclient.Spanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LastProcessedTimestamp {
  private static final Logger log = LoggerFactory.getLogger(LastProcessedTimestamp.class);
  private static final String LPTS_COLUMN_NAME = "LastProcessedTimestamp";

  static String parseLastProcessedTimestamp(RowCursor lptsCursor) {
    try {
      Timestamp timestamp = lptsCursor.getTimestamp(LPTS_COLUMN_NAME);
      return timestamp.toString();
    } catch (Exception e) {
      log.error("Couldn't retrieve " + LPTS_COLUMN_NAME, e);
      return null;
    }
  }

  /**
   * build the query to fetch the last processed timestamp.
   *
   * @param sink sink table config
   * @param lpts lpts table config
   * @return the query
   */
  public static String buildQuery(SpezConfig.SinkConfig sink, SpezConfig.LptsConfig lpts) {
    return new StringBuilder()
        .append("SELECT * FROM ")
        .append(lpts.getTable())
        .append(" WHERE instance = '")
        .append(sink.getInstance())
        .append("' AND database = '")
        .append(sink.getDatabase())
        .append("' AND table = '")
        .append(sink.getTable())
        .append("'")
        .toString();
  }

  /**
   * get the last processed timestamp for a given configuration.
   *
   * @param sink config for the table to retrieve
   * @param lpts config for where to find the last processed timestamp
   * @return the last processed timestamp as a string
   */
  public static String getLastProcessedTimestamp(
      SpezConfig.SinkConfig sink, SpezConfig.LptsConfig lpts) {
    final ListenableFuture<Database> dbFuture = Spanner.openDatabaseAsync(lpts.getSettings());

    Database lptsDatabase;
    try {
      log.info("waiting for lpts db");
      lptsDatabase = dbFuture.get(); // NOPMD
      log.info("LPTS Database returned!");
    } catch (Exception e) {
      log.error("Failed to get Database, throwing");
      throw new RuntimeException(e);
    }
    String lptsQuery = buildQuery(sink, lpts);

    log.info(
        "Looking for last processed timestamp with query {} against database {}",
        lptsQuery,
        lpts.databasePath());

    RowCursor lptsCursor = // NOPMD
        Spanner.execute(QueryOptions.DEFAULT(), lptsDatabase, Query.create(lptsQuery));

    if (lptsCursor == null) {
      throw new RuntimeException("Couldn't find lpts row");
    }

    boolean gotTimestamp = false;
    String timestamp = null;
    while (lptsCursor.next()) {
      if (gotTimestamp) {
        log.error(
            "Got more than one row from table '{}', using the first value {}",
            lpts.getTable(),
            timestamp);
        break;
      }
      timestamp = parseLastProcessedTimestamp(lptsCursor);
      if (timestamp != null) {
        gotTimestamp = true;
      }
    }

    if (timestamp == null) {
      throw new RuntimeException(LPTS_COLUMN_NAME + " was unavailable");
    }

    return timestamp;
  }
}
