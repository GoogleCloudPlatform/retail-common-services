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

import com.google.protobuf.ByteString;
import com.google.spez.core.internal.Row;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerToAvroRecord {
  private static final Logger log = LoggerFactory.getLogger(SpannerToAvroRecord.class);
  private static final ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;

  private SpannerToAvroRecord() {}

  /**
   * given a SchemaSet and a Row return an avro record.
   *
   * @param schemaSet schema of the row
   * @param resultSet row to encode
   * @return an avro record
   */
  public static Optional<ByteString> makeRecord(SchemaSet schemaSet, Row resultSet) {
    return makeRecord(schemaSet, resultSet, null);
  }

  public static void addArrayColumn(GenericRecord record, String columnName, Row resultSet, SchemaSet schemaSet) {
    log.debug("Put ARRAY");

    final com.google.spanner.v1.Type columnType = resultSet.getColumnType(columnName);
    final String arrayTypeString = columnType.getArrayElementType().getCode().toString();

    log.debug("Type: " + columnType);
    log.debug("ArrayString: " + arrayTypeString);

    switch (arrayTypeString) {
      case "BOOL":
        log.debug("Put BOOL");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
        } else {
          record.put(columnName, resultSet.getBooleanList(columnName));
        }

        break;
      case "BYTES":
        log.debug("Put BYTES");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
        } else {
          record.put(columnName, resultSet.getBytesList(columnName));
        }

        break;
      case "DATE":
        log.debug("Put DATE");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
        } else {
          record.put(columnName, resultSet.getStringList(columnName));
        }

        break;
      case "FLOAT64":
        log.debug("Put FLOAT64");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
        } else {
          record.put(columnName, resultSet.getDoubleList(columnName));
        }

        break;
      case "INT64":
        log.debug("Put INT64");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
        } else {
          record.put(columnName, resultSet.getLongList(columnName));
        }

        break;
      case "STRING(MAX)":
        log.debug("Put STRING");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
        } else {
          record.put(columnName, resultSet.getStringList(columnName));
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
  }

  public static void addColumn(GenericRecord record, String columnName, Row resultSet, SchemaSet schemaSet) {
    log.debug("Column Name: " + columnName);
    log.debug("Data Type: " + schemaSet.spannerSchema().get(columnName));
    switch (schemaSet.spannerSchema().get(columnName)) {
      case "ARRAY":
        addArrayColumn(record, columnName, resultSet, schemaSet);

        break;
      case "BOOL":
        log.debug("Put BOOL");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
        } else {
          record.put(columnName, resultSet.getBoolean(columnName));
        }

        break;
      case "BYTES":
        log.debug("Put BYTES");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
        } else {
          record.put(columnName, resultSet.getBytes(columnName));
        }

        break;
      case "DATE":
        log.debug("Put DATE");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
        } else {
          record.put(columnName, resultSet.getDate(columnName).toString());
        }

        break;
      case "FLOAT64":
        log.debug("Put FLOAT64");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
        } else {
          record.put(columnName, resultSet.getDouble(columnName));
        }

        break;
      case "INT64":
        log.debug("Put INT64");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
          log.debug("INT64 NULL OK");
        } else {
          record.put(columnName, resultSet.getLong(columnName));
          log.debug("INT64 OK");
        }

        break;
      case "STRING(MAX)":
        log.debug("Put STRING");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
          log.debug("STRING(MAX) NULL OK");
        } else {
          record.put(columnName, resultSet.getString(columnName));
          log.debug("STRING(MAX) OK");
        }
        break;
      case "TIMESTAMP":
        log.debug("Put TIMESTAMP");
        if (resultSet.isNull(columnName)) {
          record.put(columnName, null);
        } else {
          record.put(columnName, resultSet.getTimestamp(columnName).toString());
        }

        break;
      default:
        if (schemaSet.spannerSchema().get(columnName).contains("STRING")) {
          log.debug("Put STRING");
          if (resultSet.isNull(columnName)) {
            record.put(columnName, null);
            log.debug("STRING NULL OK");
          } else {
            record.put(columnName, resultSet.getString(columnName));
          }

        } else {
          log.error(
              "Unknown Data type when generating Avro Record: "
              + schemaSet.spannerSchema().get(columnName));
        }
        break;
    }
  }

  /**
   * given a SchemaSet and a Row return an avro record.
   *
   * @param schemaSet schema of the row
   * @param resultSet row to encode
   * @param syncMarker used to generate consistent records under test
   * @return an avro record
   */
  public static Optional<ByteString> makeRecord(
      SchemaSet schemaSet, Row resultSet, byte[] syncMarker) {
    //    final ByteBuf bb = Unpooled.directBuffer();
    final ByteBuf bb = alloc.directBuffer(1024); // fix this
    final Set<String> keySet = schemaSet.spannerSchema().keySet();
    final GenericRecord record = new GenericData.Record(schemaSet.avroSchema());

    log.debug("KeySet: " + keySet);
    log.debug("Record: " + record);

    keySet.forEach(
        columnName -> {
          addColumn(record, columnName, resultSet, schemaSet);
        });

    log.debug("Made Record");
    log.debug(record.toString());

    try (final ByteBufOutputStream outputStream = new ByteBufOutputStream(bb)) {
      log.debug("Serializing Record");
      // final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      /*
      var encoder = EncoderFactory.get().jsonEncoder(schemaSet.avroSchema(), outputStream);
      final DatumWriter<Object> writer = new GenericDatumWriter<>(schemaSet.avroSchema());

      writer.write(record, encoder);
      encoder.flush();
      */
      DatumWriter<GenericRecord> datumWriter =
          new GenericDatumWriter<GenericRecord>(schemaSet.avroSchema());
      DataFileWriter<GenericRecord> writer = // NOPMD
          new DataFileWriter<GenericRecord>(datumWriter)
              .create(schemaSet.avroSchema(), outputStream, syncMarker);
      writer.append(record);
      writer.close();

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
}
