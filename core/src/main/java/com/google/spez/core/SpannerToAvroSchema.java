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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.spez.core.internal.Row;
import com.google.spez.core.internal.RowCursor;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.LinkedHashMap;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerToAvroSchema {
  private static final Logger log = LoggerFactory.getLogger(SpannerToAvroSchema.class);
  private static final ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;

  private SpannerToAvroSchema() {}

  public static SchemaSet getSchema(
      String tableName, String avroNamespace, RowCursor resultSet, String tsColName) {

    final LinkedHashMap<String, String> spannerSchema = Maps.newLinkedHashMap();
    final SchemaBuilder.FieldAssembler<Schema> avroSchemaBuilder =
        SchemaBuilder.record(tableName).namespace(avroNamespace).fields();
    log.debug("Getting Schema");

    log.debug("Processing  Schema");
    while (resultSet.next()) {
      log.debug("Making Avro Schema");
      final Row currentRow = resultSet.getCurrentRow();
      final String name = currentRow.getString(0);
      final String type = currentRow.getString(1);
      final boolean nullable = currentRow.getString(2).equals("NO") ? false : true;

      spannerSchema.put(name, type);
      log.debug("Binding Avro Schema");
      switch (type) {
        case "ARRAY":
          log.debug("Made ARRAY");
          // Arrays cannot be nullable
          avroSchemaBuilder.name(name).type().array();
          break;
        case "BOOL":
          log.debug("Made BOOL");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().booleanType();
          } else {
            avroSchemaBuilder.name(name).type().booleanType().noDefault();
          }
          break;
        case "BYTES":
          log.debug("Made BYTES");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().bytesType();
          } else {
            avroSchemaBuilder.name(name).type().bytesType().noDefault();
          }
          break;
        case "DATE":
          // Date handled as String type
          log.debug("Made DATE");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().stringType();
          } else {
            avroSchemaBuilder.name(name).type().stringType().noDefault();
          }
          break;
        case "FLOAT64":
          log.debug("Made FLOAT64");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().doubleType();
          } else {
            avroSchemaBuilder.name(name).type().doubleType().noDefault();
          }
          break;
        case "INT64":
          log.debug("Made INT64");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().longType();
          } else {
            avroSchemaBuilder.name(name).type().longType().noDefault();
          }
          break;
        case "STRING(MAX)":
          log.debug("Made STRING(MAX)");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().stringType();
          } else {
            avroSchemaBuilder.name(name).type().stringType().noDefault();
          }
          break;
        case "TIMESTAMP":
          log.debug("Made TIMESTAMP");
          if (nullable) {
            avroSchemaBuilder.name(name).type().optional().stringType();
          } else {
            avroSchemaBuilder.name(name).type().stringType().noDefault();
          }
          break;
        default:
          if (type.contains("STRING")) {
            log.debug("Made STRING: " + type);
            if (nullable) {
              avroSchemaBuilder.name(name).type().optional().stringType();
            } else {
              avroSchemaBuilder.name(name).type().stringType().noDefault();
            }
          } else {
            log.error("Unknown Schema type when generating Avro Schema: " + type);
          }
          break;
      }
    }

    log.debug("Ending Avro Record");
    final Schema avroSchema = avroSchemaBuilder.endRecord();

    log.debug("Made Avro Schema");

    if (log.isDebugEnabled()) {
      final Set<String> keySet = spannerSchema.keySet();
      for (String k : keySet) {
        log.info("-------------------------- ColName: " + k + " Type: " + spannerSchema.get(k));
      }

      log.info("--------------------------- " + avroSchema.toString());
    }

    return SchemaSet.create(avroSchema, ImmutableMap.copyOf(spannerSchema), tsColName);
  }
}
