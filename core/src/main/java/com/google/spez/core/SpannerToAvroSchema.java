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
import com.google.spanner.v1.Type;
import com.google.spez.core.internal.Row;
import com.google.spez.core.internal.RowCursor;
import java.util.LinkedHashMap;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO(pdex): remove this class in preference of on demand schema
public class SpannerToAvroSchema {
  private static final Logger log = LoggerFactory.getLogger(SpannerToAvroSchema.class);

  private SpannerToAvroSchema() {}

  public static void addArrayField(
      Type type, SchemaBuilder.ArrayBuilder<SchemaBuilder.FieldAssembler<Schema>> schema) {
    var builder = schema.items();
    switch (type.getCode()) {
      case ARRAY:
        addArrayField(type.getArrayElementType(), builder.array());
        break;
      case BOOL:
        builder.booleanType();
        break;
      case BYTES:
        builder.bytesType();
        break;
      case DATE:
        builder.stringType();
        break;
      case FLOAT64:
        builder.doubleType();
        break;
      case INT64:
        builder.longType();
        break;
        // TODO(pdex): handle new NUMERIC type code
        // case NUMERIC:
        //   break;
      case STRING:
        builder.stringType();
        break;
      case STRUCT:
        break;
      case TIMESTAMP:
        builder.stringType();
        break;
      case TYPE_CODE_UNSPECIFIED:
      default:
        break;
    }
  }

  public static void addField(String name, Type type, SchemaBuilder.FieldAssembler<Schema> schema) {
    var builder = schema.name(name).type().optional();
    switch (type.getCode()) {
      case ARRAY:
        addArrayField(type.getArrayElementType(), builder.array());
        break;
      case BOOL:
        builder.booleanType();
        break;
      case BYTES:
        builder.bytesType();
        break;
      case DATE:
        builder.stringType();
        break;
      case FLOAT64:
        builder.doubleType();
        break;
      case INT64:
        builder.longType();
        break;
        // TODO(pdex): handle new NUMERIC type code
        // case NUMERIC:
        //   break;
      case STRING:
        builder.stringType();
        break;
      case STRUCT:
        break;
      case TIMESTAMP:
        builder.stringType();
        break;
      case TYPE_CODE_UNSPECIFIED:
      default:
        break;
    }
  }

  public static Schema buildSchema(String tableName, String avroNamespace, Row row) {
    SchemaBuilder.FieldAssembler<Schema> avroSchemaBuilder =
        SchemaBuilder.record(tableName).namespace(avroNamespace).fields();
    var struct = row.getType().getStructType();
    var fields = struct.getFieldsList();
    for (var field : fields) {
      addField(field.getName(), field.getType(), avroSchemaBuilder);
    }
    return avroSchemaBuilder.endRecord();
  }

  /**
   * builds a SchemaSet object.
   *
   * @param tableName table name
   * @param avroNamespace avro namespace
   * @param resultSet result set to iterate
   * @param tsColName timestamp column name
   * @return the SchemaSet representing
   */
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
      final boolean nullable = currentRow.getString(2).equals("NO") ? false : true; // NOPMD

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

    return SchemaSet.create(avroSchema, ImmutableMap.copyOf(spannerSchema));
  }
}
