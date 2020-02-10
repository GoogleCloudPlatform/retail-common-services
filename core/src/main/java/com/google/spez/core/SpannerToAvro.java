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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.spannerclient.Row;
import com.google.spannerclient.RowCursor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerToAvro {
  private static final Logger log = LoggerFactory.getLogger(SpannerToAvro.class);
  private static final ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;

  private SpannerToAvro() {}

  public static ListenableFuture<SchemaSet> GetSchemaAsync(
      String tableName, String avroNamespace, RowCursor resultSet, String tsColName) {
    final SettableFuture<SchemaSet> schemaFuture = SettableFuture.create();
    final ListeningExecutorService x =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                1, new ThreadFactoryBuilder().setNameFormat("Spanner To Avro Thread").build()));

    schemaFuture.set(GetSchema(tableName, avroNamespace, resultSet, tsColName));

    return schemaFuture;
  }

  public static SchemaSet GetSchema(
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

  public static Optional<ByteString> MakeRecord(SchemaSet schemaSet, Row resultSet) {
    //    final ByteBuf bb = Unpooled.directBuffer();
    final ByteBuf bb = alloc.directBuffer(1024); // fix this
    final Set<String> keySet = schemaSet.spannerSchema().keySet();
    final GenericRecord record = new GenericData.Record(schemaSet.avroSchema());

    log.debug("KeySet: " + keySet);
    log.debug("Record: " + record);

    keySet.forEach(
        x -> {
          log.debug("Column Name: " + x);
          log.debug("Data Type: " + schemaSet.spannerSchema().get(x));
          switch (schemaSet.spannerSchema().get(x)) {
            case "ARRAY":
              log.debug("Put ARRAY");

              final com.google.spanner.v1.Type columnType = resultSet.getColumnType(x);
              final String arrayTypeString = columnType.getArrayElementType().getCode().toString();

              log.debug("Type: " + columnType);
              log.debug("ArrayString: " + arrayTypeString);

              switch (arrayTypeString) {
                case "BOOL":
                  log.debug("Put BOOL");
                  try {
                    record.put(x, resultSet.getBooleanList(x));
                  } catch (NullPointerException e) {
                    record.put(x, null);
                  }

                  break;
                case "BYTES":
                  log.debug("Put BYTES");
                  try {
                    record.put(x, resultSet.getBytesList(x));
                  } catch (NullPointerException e) {
                    record.put(x, null);
                  }

                  break;
                case "DATE":
                  log.debug("Put DATE");
                  try {
                    record.put(x, resultSet.getStringList(x));
                  } catch (NullPointerException e) {
                    record.put(x, null);
                  }

                  break;
                case "FLOAT64":
                  log.debug("Put FLOAT64");
                  try {
                    record.put(x, resultSet.getDoubleList(x));
                  } catch (NullPointerException e) {
                    record.put(x, null);
                  }

                  break;
                case "INT64":
                  log.debug("Put INT64");
                  try {
                    record.put(x, resultSet.getLongList(x));
                  } catch (NullPointerException e) {
                    record.put(x, null);
                  }

                  break;
                case "STRING(MAX)":
                  log.debug("Put STRING");
                  try {
                    record.put(x, resultSet.getStringList(x));
                  } catch (NullPointerException e) {
                    record.put(x, null);
                  }

                  break;
                case "TIMESTAMP":
                  // Timestamp lists are not supported as of now
                  log.error("Cannot add Timestamp array list to avro record: " + arrayTypeString);
                  break;
                default:
                  log.error("Unknown Data type when generating Array Schema: " + arrayTypeString);
                  break;
              }

              break;
            case "BOOL":
              log.debug("Put BOOL");
              try {
                record.put(x, resultSet.getBoolean(x));
              } catch (NullPointerException e) {
                record.put(x, null);
              }

              break;
            case "BYTES":
              log.debug("Put BYTES");
              try {
                record.put(x, resultSet.getBytes(x));
              } catch (NullPointerException e) {
                record.put(x, null);
              }

              break;
            case "DATE":
              log.debug("Put DATE");
              try {
                record.put(x, resultSet.getDate(x).toString());
              } catch (NullPointerException e) {
                record.put(x, null);
              }

              break;
            case "FLOAT64":
              log.debug("Put FLOAT64");
              try {
                record.put(x, resultSet.getDouble(x));
              } catch (NullPointerException e) {
                record.put(x, null);
              }

              break;
            case "INT64":
              log.debug("Put INT64");
              try {
                record.put(x, resultSet.getLong(x));
                log.debug("INT64 OK");
              } catch (NullPointerException e) {
                record.put(x, null);
                log.debug("INT64 NULL OK");
              }

              break;
            case "STRING(MAX)":
              log.debug("Put STRING");
              try {
                record.put(x, resultSet.getString(x));
                log.debug("STRING(MAX) OK");
              } catch (NullPointerException e) {
                record.put(x, null);
                log.debug("STRING(MAX) NULL OK");
              }
              break;
            case "TIMESTAMP":
              log.debug("Put TIMESTAMP");
              try {
                record.put(x, resultSet.getTimestamp(x).toString());
              } catch (NullPointerException e) {
                record.put(x, null);
              }

              break;
            default:
              if (schemaSet.spannerSchema().get(x).contains("STRING")) {
                log.debug("Put STRING");
                try {
                  record.put(x, resultSet.getString(x));
                  log.debug("STRING OK");
                } catch (NullPointerException e) {
                  record.put(x, null);
                  log.debug("STRING NULL OK");
                }

              } else {
                log.error(
                    "Unknown Data type when generating Avro Record: "
                        + schemaSet.spannerSchema().get(x));
              }
              break;
          }
        });

    log.debug("Made Record");
    log.debug(record.toString());

    try (final ByteBufOutputStream outputStream = new ByteBufOutputStream(bb)) {
      final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      final DatumWriter<Object> writer = new GenericDatumWriter<>(schemaSet.avroSchema());

      log.debug("Serializing Record");
      writer.write(record, encoder);
      encoder.flush();
      outputStream.flush();
      log.debug("Adding serialized record to list");
      log.debug("--------------------------------- readableBytes " + bb.readableBytes());
      log.debug("--------------------------------- readerIndex " + bb.readerIndex());
      log.debug("--------------------------------- writerIndex " + bb.writerIndex());

      final ByteString message = ByteString.copyFrom(bb.nioBuffer());

      return Optional.of(message);

    } catch (IOException e) {
      log.error(
          "IOException while Serializing Spanner Stuct to Avro Record: " + record.toString(), e);
    } finally {
      bb.release();
    }

    return Optional.empty();
  }

  @AutoValue
  public abstract static class SchemaSet {
    static SchemaSet create(
        Schema avroSchema, ImmutableMap<String, String> spannerSchema, String tsColName) {
      return new AutoValue_SpannerToAvro_SchemaSet(avroSchema, spannerSchema, tsColName);
    }

    abstract Schema avroSchema();

    abstract ImmutableMap<String, String> spannerSchema();

    public abstract String tsColName();
  }
}
