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

import com.google.spez.core.internal.Database;
import com.google.spez.core.internal.RowCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerSchema {
  private static final Logger log = LoggerFactory.getLogger(SpannerSchema.class);
  private final Database database;
  private final SpezConfig.SinkConfig config;

  public SpannerSchema(Database database, SpezConfig.SinkConfig config) {
    this.database = database;
    this.config = config;
  }

  /**
   * Parses the schema from the Cloud Spanner table and creates an Avro {@code SchemSet} to enable
   * you to dynamically serialize Cloud Spanner events as an Avro record for publshing to an event
   * ledger.
   *
   * @return SchemaSet
   */
  public SchemaSet getSchema() {
    log.info(
        "querying schema for {}.{}.{}",
        config.getInstance(),
        config.getDatabase(),
        config.getTable());

    final String schemaQuery =
        "SELECT COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"
            + config.getTable()
            + "' ORDER BY ORDINAL_POSITION";
    final String pkQuery =
        "SELECT INDEX_NAME, INDEX_TYPE, COLUMN_NAME, IS_NULLABLE, SPANNER_TYPE FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME = '"
            + config.getTable()
            + "'";
    final String tsQuery =
        "SELECT COLUMN_NAME, OPTION_NAME, OPTION_TYPE, OPTION_VALUE FROM INFORMATION_SCHEMA.COLUMN_OPTIONS WHERE TABLE_NAME = '"
            + config.getTable()
            + "'";

    RowCursor schemaResult = database.execute(schemaQuery);
    RowCursor indexColumns = database.execute(pkQuery);
    RowCursor columnOptions = database.execute(tsQuery);

    TimestampColumnChecker timestampChecker = new TimestampColumnChecker(config);
    while (columnOptions.next()) {
      timestampChecker.checkRow(columnOptions);
    }
    timestampChecker.throwIfInvalid();

    UuidColumnChecker uuidChecker = new UuidColumnChecker(config);
    while (indexColumns.next()) {
      uuidChecker.checkRow(indexColumns);
    }
    uuidChecker.throwIfInvalid();

    var namespace = "avroNamespace";
    return SpannerToAvroSchema.getSchema(
        config.getTable(), namespace, schemaResult, config.getTimestampColumn());
  }
}
