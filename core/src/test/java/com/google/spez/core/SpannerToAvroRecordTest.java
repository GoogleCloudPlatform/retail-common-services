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

import static com.google.protobuf.TextFormat.escapeBytes;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.Timestamp;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import com.google.spez.core.internal.BothanRow;
import com.google.spez.core.internal.Row;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SpannerToAvroRecordTest implements WithAssertions {
  private static final Logger log = LoggerFactory.getLogger(SpannerToAvroRecordTest.class);
  private static String TABLE_NAME = "test_table";
  private static String NAMESPACE = "avroNamespace";

  public static Row of(String fieldName, boolean fieldValue) {
    var type = Type.newBuilder().setCode(TypeCode.BOOL).build();
    var name =
        ImmutableList.of(StructType.Field.newBuilder().setName(fieldName).setType(type).build());
    // var value = ImmutableList.of(Value.newBuilder().setBoolValue(fieldValue).build());
    var descriptor = Value.getDescriptor().findFieldByName("bool_value");
    var value = ImmutableList.of(Value.newBuilder().setField(descriptor, fieldValue).build());
    return new BothanRow(new com.google.spannerclient.Row(name, value));
  }

  public static Row of(String fieldName, Type fieldType, Value fieldValue) {
    var fields =
        ImmutableList.of(
            StructType.Field.newBuilder().setName(fieldName).setType(fieldType).build());
    var values = ImmutableList.of(fieldValue);
    var row = new com.google.spannerclient.Row(fields, values);
    return new BothanRow(row);
  }

  public static ByteString serialize(Schema schema, GenericData.Record record, byte[] syncMarker) {
    try (var outputStream = new ByteArrayOutputStream()) {
      try (var writer =
          new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))
              .create(schema, outputStream, syncMarker)) {
        writer.append(record);
        writer.close();
        outputStream.flush();
        return ByteString.copyFrom(outputStream.toByteArray());
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static GenericRecord deserialize(ByteString byteString) {
    try (var inputStream = new ByteArrayInputStream(byteString.toByteArray())) {
      try (var reader =
          new DataFileStream<GenericData.Record>(inputStream, new GenericDatumReader<>())) {
        return reader.next();
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static byte[] generateSync() {
    try {
      MessageDigest digester = MessageDigest.getInstance("MD5");
      long time = System.currentTimeMillis();
      digester.update((UUID.randomUUID() + "@" + time).getBytes(UTF_8));
      return digester.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private void logBytestrings(ByteString actualBytes, ByteString expectedBytes) {
    if (!actualBytes.equals(expectedBytes)) {
      log.error(
          "actual  : {}\n" + "does not match\n" + "expected: {}",
          escapeBytes(actualBytes.toByteArray()),
          escapeBytes(expectedBytes.toByteArray()));
    }
  }

  private SchemaBuilder.FieldTypeBuilder<Schema> schemaField(String fieldName) {
    return SchemaBuilder.record(TABLE_NAME).namespace(NAMESPACE).fields().name(fieldName).type();
  }

  private ByteString getExpectedBytes(
      Schema avroSchema, String fieldName, Object fieldValue, byte[] syncMarker) {
    var avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put(fieldName, fieldValue);
    return serialize(avroSchema, avroRecord, syncMarker);
  }

  private SchemaSet buildSchemaSet(Schema avroSchema, String fieldName, Type fieldType) {
    var spannerType = fieldType.getCode().name();
    var schemaMap = ImmutableMap.of(fieldName, spannerType);
    return SchemaSet.create(avroSchema, schemaMap, "");
  }

  @Test
  public void typeCodeNameMatchesSpannerType() {
    {
      var fieldName = "testBool"; // NOPMD
      var avroSchema = schemaField(fieldName).booleanType().noDefault().endRecord();
      var fieldType = Type.newBuilder().setCode(TypeCode.BOOL).build();
      var schemaSet = buildSchemaSet(avroSchema, fieldName, fieldType);
      assertThat(schemaSet.spannerSchema().get(fieldName)).isEqualTo("BOOL");
    }
    {
      var fieldName = "testInt64";
      var avroSchema = schemaField(fieldName).longType().noDefault().endRecord();
      var fieldType = Type.newBuilder().setCode(TypeCode.INT64).build();
      var schemaSet = buildSchemaSet(avroSchema, fieldName, fieldType);
      assertThat(schemaSet.spannerSchema().get(fieldName)).isEqualTo("INT64");
    }
    {
      var fieldName = "testFloat64";
      var avroSchema = schemaField(fieldName).doubleType().noDefault().endRecord();
      var fieldType = Type.newBuilder().setCode(TypeCode.FLOAT64).build();
      var schemaSet = buildSchemaSet(avroSchema, fieldName, fieldType);
      assertThat(schemaSet.spannerSchema().get(fieldName)).isEqualTo("FLOAT64");
    }
    {
      var fieldName = "testTimestamp";
      var avroSchema = schemaField(fieldName).stringType().noDefault().endRecord();
      var fieldType = Type.newBuilder().setCode(TypeCode.STRING).build();
      var schemaSet = buildSchemaSet(avroSchema, fieldName, fieldType);
      assertThat(schemaSet.spannerSchema().get(fieldName)).isEqualTo("STRING");
    }
    {
      var fieldName = "testDate";
      var avroSchema = schemaField(fieldName).stringType().noDefault().endRecord();
      var fieldType = Type.newBuilder().setCode(TypeCode.DATE).build();
      var schemaSet = buildSchemaSet(avroSchema, fieldName, fieldType);
      assertThat(schemaSet.spannerSchema().get(fieldName)).isEqualTo("DATE");
    }
    {
      var fieldName = "testString";
      var avroSchema = schemaField(fieldName).stringType().noDefault().endRecord();
      var fieldType = Type.newBuilder().setCode(TypeCode.STRING).build();
      var schemaSet = buildSchemaSet(avroSchema, fieldName, fieldType);
      assertThat(schemaSet.spannerSchema().get(fieldName)).isEqualTo("STRING");
    }
    {
      var fieldName = "testBytes";
      var avroSchema = schemaField(fieldName).stringType().noDefault().endRecord();
      var fieldType = Type.newBuilder().setCode(TypeCode.BYTES).build();
      var schemaSet = buildSchemaSet(avroSchema, fieldName, fieldType);
      assertThat(schemaSet.spannerSchema().get(fieldName)).isEqualTo("BYTES");
    }
    {
      var fieldName = "testArray";
      // TODO(pdex): fix this type
      var avroSchema = schemaField(fieldName).booleanType().noDefault().endRecord();
      var fieldType = Type.newBuilder().setCode(TypeCode.ARRAY).build();
      var schemaSet = buildSchemaSet(avroSchema, fieldName, fieldType);
      assertThat(schemaSet.spannerSchema().get(fieldName)).isEqualTo("ARRAY");
    }
    {
      var fieldName = "testStruct";
      // TODO(pdex): fix this type
      var avroSchema = schemaField(fieldName).booleanType().noDefault().endRecord();
      var fieldType = Type.newBuilder().setCode(TypeCode.STRUCT).build();
      var schemaSet = buildSchemaSet(avroSchema, fieldName, fieldType);
      assertThat(schemaSet.spannerSchema().get(fieldName)).isEqualTo("STRUCT");
    }
  }

  private void runTest(
      Schema avroSchema, String fieldName, Object fieldValue, Type rowType, Value rowValue) {
    var row = of(fieldName, rowType, rowValue);
    var syncMarker = generateSync();
    var schemaSet = buildSchemaSet(avroSchema, fieldName, rowType);
    Optional<ByteString> record = SpannerToAvroRecord.makeRecord(schemaSet, row, syncMarker);
    var expectedBytes = getExpectedBytes(avroSchema, fieldName, fieldValue, syncMarker);
    logBytestrings(record.get(), expectedBytes);

    var actualValue = deserialize(record.get()).get(fieldName);
    if (actualValue instanceof Utf8) {
      actualValue = actualValue.toString();
    }
    assertThat(actualValue).isInstanceOf(fieldValue.getClass()).isEqualTo(fieldValue);
    assertThat(record.get().toByteArray()).isEqualTo(expectedBytes.toByteArray());
  }

  // BOOL
  @Test
  void makeRecordWithRequiredBoolean() {
    var fieldName = "testBool"; // NOPMD
    var fieldValue = false;
    var avroSchema = schemaField(fieldName).booleanType().noDefault().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.BOOL).build();
    var rowValue = Value.newBuilder().setBoolValue(fieldValue).build();
    /*
    var row = of(fieldName, fieldType, rowValue);
    var syncMarker = generateSync();
    var schemaSet = buildSchemaSet(avroSchema, fieldName, fieldType);
    Optional<ByteString> record = SpannerToAvroRecord.makeRecord(schemaSet, row, syncMarker);
    var expectedBytes = getExpectedBytes(avroSchema, fieldName, fieldValue, syncMarker);
    logBytestrings(record.get(), expectedBytes);

    assertThat(deserialize(record.get()).get(fieldName)).isEqualTo(fieldValue);
    assertThat(record.get().toByteArray()).isEqualTo(expectedBytes.toByteArray());
    */
    runTest(avroSchema, fieldName, fieldValue, fieldType, rowValue);
  }

  @Test
  public void makeRecordWithOptionalBooleanSet() {
    var fieldName = "testBool"; // NOPMD
    var fieldValue = false;
    var avroSchema = schemaField(fieldName).optional().booleanType().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.BOOL).build();
    var rowValue = Value.newBuilder().setBoolValue(fieldValue).build();
    /*
    var row = of(fieldName, fieldType, rowValue);
    var syncMarker = generateSync();
    var schemaSet = buildSchemaSet(avroSchema, fieldName, fieldType);
    Optional<ByteString> record = SpannerToAvroRecord.makeRecord(schemaSet, row, syncMarker);
    var expectedBytes = getExpectedBytes(avroSchema, fieldName, fieldValue, syncMarker);
    logBytestrings(record.get(), expectedBytes);

    assertThat(deserialize(record.get()).get(fieldName)).isEqualTo(fieldValue);
    assertThat(record.get().toByteArray()).isEqualTo(expectedBytes.toByteArray());
     */
    runTest(avroSchema, fieldName, fieldValue, fieldType, rowValue);
  }

  @Test
  // TODO(pdex): this test won't work with BothanRow
  // BothanRow's underlying Row can't handle null values
  public void makeRecordWithOptionalBooleanNotSet() {
    var fieldName = "testBool"; // NOPMD
    Object fieldValue = null;
    var avroSchema = schemaField(fieldName).optional().booleanType().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.BOOL).build();
    var schemaSet = buildSchemaSet(avroSchema, fieldName, fieldType);
    var rowValue = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    var row = of(fieldName, fieldType, rowValue);
    var syncMarker = generateSync();
    Optional<ByteString> record = SpannerToAvroRecord.makeRecord(schemaSet, row, syncMarker);
    var expectedBytes = getExpectedBytes(avroSchema, fieldName, fieldValue, syncMarker);
    logBytestrings(record.get(), expectedBytes);

    assertThat(deserialize(record.get()).get(fieldName)).isEqualTo(fieldValue);
    assertThat(record.get().toByteArray()).isEqualTo(expectedBytes.toByteArray());
  }

  // INT64
  @Test
  void makeRecordWithRequiredLong() {
    var fieldName = "testLong";
    var fieldValue = 100L;
    var avroSchema = schemaField(fieldName).longType().noDefault().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.INT64).build();
    var rowValue = Value.newBuilder().setStringValue(Long.toString(fieldValue)).build();
    runTest(avroSchema, fieldName, fieldValue, fieldType, rowValue);
  }

  @Test
  public void makeRecordWithOptionalLongSet() {
    var fieldName = "testLong";
    var fieldValue = 100L;
    var avroSchema = schemaField(fieldName).optional().longType().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.INT64).build();
    var rowValue = Value.newBuilder().setStringValue(Long.toString(fieldValue)).build();
    runTest(avroSchema, fieldName, fieldValue, fieldType, rowValue);
  }

  // FLOAT64
  @Test
  void makeRecordWithRequiredDouble() {
    var fieldName = "testDouble";
    var fieldValue = 100.0d;
    var avroSchema = schemaField(fieldName).doubleType().noDefault().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.FLOAT64).build();
    var rowValue = Value.newBuilder().setNumberValue(fieldValue).build();
    runTest(avroSchema, fieldName, fieldValue, fieldType, rowValue);
  }

  @Test
  public void makeRecordWithOptionalDoubleSet() {
    var fieldName = "testDouble";
    var fieldValue = 100.0d;
    var avroSchema = schemaField(fieldName).optional().doubleType().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.FLOAT64).build();
    var rowValue = Value.newBuilder().setNumberValue(fieldValue).build();
    runTest(avroSchema, fieldName, fieldValue, fieldType, rowValue);
  }

  // TIMESTAMP
  @Test
  void makeRecordWithRequiredTimestamp() {
    var fieldName = "testTimestamp";
    var fieldValue = Timestamp.ofTimeSecondsAndNanos(0, 0).toString();
    System.out.println("fieldValue: " + fieldValue);
    var avroSchema = schemaField(fieldName).stringType().noDefault().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.TIMESTAMP).build();
    var rowValue = Value.newBuilder().setStringValue(fieldValue).build();
    runTest(avroSchema, fieldName, fieldValue, fieldType, rowValue);
  }

  @Test
  public void makeRecordWithOptionalTimestampSet() {
    var fieldName = "testTimestamp";
    var fieldValue = Timestamp.ofTimeSecondsAndNanos(0, 0).toString();
    var avroSchema = schemaField(fieldName).optional().stringType().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.TIMESTAMP).build();
    var rowValue = Value.newBuilder().setStringValue(fieldValue).build();
    runTest(avroSchema, fieldName, fieldValue, fieldType, rowValue);
  }

  // DATE
  @Test
  void makeRecordWithRequiredDate() {
    var fieldName = "testDate";
    var fieldValue = "1970-01-01";
    var avroSchema = schemaField(fieldName).stringType().noDefault().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.DATE).build();
    var rowValue = Value.newBuilder().setStringValue(fieldValue).build();
    runTest(avroSchema, fieldName, fieldValue, fieldType, rowValue);
  }

  @Test
  public void makeRecordWithOptionalDateSet() {
    var fieldName = "testDate";
    var fieldValue = "1970-01-01";
    var avroSchema = schemaField(fieldName).optional().stringType().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.DATE).build();
    var rowValue = Value.newBuilder().setStringValue(fieldValue).build();
    runTest(avroSchema, fieldName, fieldValue, fieldType, rowValue);
  }

  // STRING
  @Test
  void makeRecordWithRequiredString() {
    var fieldName = "testString";
    var fieldValue = "lorem ipsum";
    var avroSchema = schemaField(fieldName).stringType().noDefault().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.STRING).build();
    var rowValue = Value.newBuilder().setStringValue(fieldValue).build();
    runTest(avroSchema, fieldName, fieldValue, fieldType, rowValue);
  }

  @Test
  public void makeRecordWithOptionalStringSet() {
    var fieldName = "testString";
    var fieldValue = "lorem ipsum";
    var avroSchema = schemaField(fieldName).optional().stringType().endRecord();
    var fieldType = Type.newBuilder().setCode(TypeCode.STRING).build();
    var rowValue = Value.newBuilder().setStringValue(fieldValue).build();
    runTest(avroSchema, fieldName, fieldValue, fieldType, rowValue);
  }
}
