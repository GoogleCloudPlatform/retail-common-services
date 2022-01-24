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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import com.google.spez.spanner.internal.BothanRow;
import com.google.spez.spanner.Row;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SpannerToAvroSchemaTest implements WithAssertions {
  private static final Logger log = LoggerFactory.getLogger(SpannerToAvroSchemaTest.class);
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

  private SchemaBuilder.BaseTypeBuilder<SchemaBuilder.FieldAssembler<Schema>> schemaField(
      String fieldName) {
    return SchemaBuilder.record(TABLE_NAME)
        .namespace(NAMESPACE)
        .fields()
        .name(fieldName)
        .type()
        .optional();
  }

  private SchemaSet buildSchemaSet(Schema avroSchema, String fieldName, Type fieldType) {
    var spannerType = fieldType.getCode().name();
    var schemaMap = ImmutableMap.of(fieldName, spannerType);
    return SchemaSet.create(avroSchema, schemaMap);
  }

  private void runTest(
      Schema avroSchema, String fieldName, Object fieldValue, Type rowType, Value rowValue) {
    var row = of(fieldName, rowType, rowValue);
    // var syncMarker = generateSync();
    var schemaSet = buildSchemaSet(avroSchema, fieldName, rowType);
    // Optional<ByteString> record = SpannerToAvroRecord.makeRecord(schemaSet, row, syncMarker);
    // var expectedBytes = getExpectedBytes(avroSchema, fieldName, fieldValue, syncMarker);
    // logBytestrings(record.get(), expectedBytes);

    /*
    var actualValue = deserialize(record.get()).get(fieldName);
    if (actualValue instanceof Utf8) {
      actualValue = actualValue.toString();
    }
    assertThat(actualValue).isInstanceOf(fieldValue.getClass()).isEqualTo(fieldValue);
    assertThat(record.get().toByteArray()).isEqualTo(expectedBytes.toByteArray());
    */
  }

  // BEGIN: Simple test cases (primitive types)
  @Test
  void buildSchemaFromBoolean() {
    var fieldName = "testBool"; // NOPMD
    var fieldValue = false;
    var fieldType = Type.newBuilder().setCode(TypeCode.BOOL).build();
    var rowValue = Value.newBuilder().setBoolValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).booleanType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }

  @Test
  void buildSchemaFromBytes() {
    var fieldName = "testBytes"; // NOPMD
    var fieldValue = "bytes";
    var fieldType = Type.newBuilder().setCode(TypeCode.BYTES).build();
    var rowValue = Value.newBuilder().setStringValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).bytesType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }

  @Test
  void buildSchemaFromDate() {
    var fieldName = "testDate"; // NOPMD
    var fieldValue = "2021-01-01";
    var fieldType = Type.newBuilder().setCode(TypeCode.DATE).build();
    var rowValue = Value.newBuilder().setStringValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).stringType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }

  @Test
  void buildSchemaFromFloat64() {
    var fieldName = "testFloat64"; // NOPMD
    var fieldValue = 1.0;
    var fieldType = Type.newBuilder().setCode(TypeCode.FLOAT64).build();
    var rowValue = Value.newBuilder().setNumberValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).doubleType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }

  @Test
  void buildSchemaFromInt64() {
    var fieldName = "testInt64"; // NOPMD
    var fieldValue = 1;
    var fieldType = Type.newBuilder().setCode(TypeCode.INT64).build();
    var rowValue = Value.newBuilder().setNumberValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).longType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }

  @Test
  void buildSchemaFromString() {
    var fieldName = "testString"; // NOPMD
    var fieldValue = "string";
    var fieldType = Type.newBuilder().setCode(TypeCode.STRING).build();
    var rowValue = Value.newBuilder().setStringValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).stringType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }

  @Test
  void buildSchemaFromTimestamp() {
    var fieldName = "testTimestamp"; // NOPMD
    var fieldValue = "2021-01-01T00:00:00";
    var fieldType = Type.newBuilder().setCode(TypeCode.TIMESTAMP).build();
    var rowValue = Value.newBuilder().setStringValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).stringType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }
  // END: Simple test cases (primitive types)

  // BEGIN: Complex test cases (arrays, structs, etc)
  @Test
  void buildSchemaFromArrayBoolean() {
    var fieldName = "testArrayBoolean"; // NOPMD
    var fieldValue = ListValue.newBuilder().addValues(Value.newBuilder().setBoolValue(false));
    var fieldType =
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.BOOL))
            .build();
    var rowValue = Value.newBuilder().setListValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).array().items().booleanType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }

  @Test
  void buildSchemaFromArrayBytes() {
    var fieldName = "testArrayBytes"; // NOPMD
    var fieldValue = ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("bytes"));
    var fieldType =
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.BYTES))
            .build();
    var rowValue = Value.newBuilder().setListValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).array().items().bytesType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }

  @Test
  void buildSchemaFromArrayDate() {
    var fieldName = "testArrayDate"; // NOPMD
    var fieldValue =
        ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("2021-01-01"));
    var fieldType =
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.DATE))
            .build();
    var rowValue = Value.newBuilder().setListValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).array().items().stringType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }

  @Test
  void buildSchemaFromArrayFloat64() {
    var fieldName = "testArrayFloat64"; // NOPMD
    var fieldValue = ListValue.newBuilder().addValues(Value.newBuilder().setNumberValue(1.0));
    var fieldType =
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.FLOAT64))
            .build();
    var rowValue = Value.newBuilder().setListValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).array().items().doubleType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }

  @Test
  void buildSchemaFromArrayInt64() {
    var fieldName = "testArrayInt64"; // NOPMD
    var fieldValue = ListValue.newBuilder().addValues(Value.newBuilder().setNumberValue(1));
    var fieldType =
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.INT64))
            .build();
    var rowValue = Value.newBuilder().setListValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).array().items().longType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }

  @Test
  void buildSchemaFromArrayNumeric() {}

  @Test
  void buildSchemaFromArrayString() {
    var fieldName = "testArrayString"; // NOPMD
    var fieldValue = ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("string"));
    var fieldType =
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.STRING))
            .build();
    var rowValue = Value.newBuilder().setListValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).array().items().stringType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }

  @Test
  void buildSchemaFromArrayTimestamp() {
    var fieldName = "testArrayTimestamp"; // NOPMD
    var fieldValue =
        ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("2021-01-01T00:00:00"));
    var fieldType =
        Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(Type.newBuilder().setCode(TypeCode.TIMESTAMP))
            .build();
    var rowValue = Value.newBuilder().setListValue(fieldValue).build();
    var row = of(fieldName, fieldType, rowValue);
    var avroSchema = SpannerToAvroSchema.buildSchema(TABLE_NAME, NAMESPACE, row);
    var expectedAvroSchema = schemaField(fieldName).array().items().stringType().endRecord();
    assertThat(avroSchema).isEqualTo(expectedAvroSchema);
  }
  // END: Complex test cases (arrays, structs, etc)

}
