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

package com.google.spez.spanner.internal;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import com.google.spez.spanner.Row;
import java.util.ArrayList;
import java.util.List;

public class GaxRow implements Row {
  private final Struct rowAsStruct;

  public GaxRow(Struct rowAsStruct) {
    this.rowAsStruct = rowAsStruct;
  }

  @Override
  public boolean isNull(String columnName) {
    return rowAsStruct.isNull(columnName);
  }

  @Override
  public String getString(int columnIndex) {
    return rowAsStruct.getString(columnIndex);
  }

  @Override
  public String getString(String columnName) {
    return rowAsStruct.getString(columnName);
  }

  private List<StructType.Field> convert(
      List<com.google.cloud.spanner.Type.StructField> structFields) {
    var result = new ArrayList<Field>();
    for (var structField : structFields) {
      result.add(
          StructType.Field.newBuilder()
              .setName(structField.getName())
              .setType(convert(structField.getType()))
              .build());
    }
    return result;
  }

  private Type convert(com.google.cloud.spanner.Type type) {
    switch (type.getCode()) {
      case ARRAY:
        return Type.newBuilder()
            .setCode(TypeCode.ARRAY)
            .setArrayElementType(convert(type.getArrayElementType()))
            .build();
      case BOOL:
        return Type.newBuilder().setCode(TypeCode.BOOL).build();
      case BYTES:
        return Type.newBuilder().setCode(TypeCode.BYTES).build();
      case DATE:
        return Type.newBuilder().setCode(TypeCode.DATE).build();
      case FLOAT64:
        return Type.newBuilder().setCode(TypeCode.FLOAT64).build();
      case INT64:
        return Type.newBuilder().setCode(TypeCode.INT64).build();
      case JSON:
        return Type.newBuilder().setCode(TypeCode.JSON).build();
      case NUMERIC:
        return Type.newBuilder().setCode(TypeCode.NUMERIC).build();
      case STRING:
        return Type.newBuilder().setCode(TypeCode.STRING).build();
      case STRUCT:
        return Type.newBuilder()
            .setCode(TypeCode.STRUCT)
            .setStructType(
                StructType.newBuilder().addAllFields(convert(type.getStructFields())).build())
            .build();
      case TIMESTAMP:
        return Type.newBuilder().setCode(TypeCode.TIMESTAMP).build();
      default:
        return Type.newBuilder().setCode(TypeCode.TYPE_CODE_UNSPECIFIED).build();
    }
  }

  @Override
  public Type getColumnType(String columnName) {
    return convert(rowAsStruct.getColumnType(columnName));
  }

  @Override
  public List<Boolean> getBooleanList(String columnName) {
    return rowAsStruct.getBooleanList(columnName);
  }

  @Override
  public List<ByteArray> getBytesList(String columnName) {
    return rowAsStruct.getBytesList(columnName);
  }

  @Override
  public List<String> getStringList(String columnName) {
    return rowAsStruct.getStringList(columnName);
  }

  @Override
  public List<Double> getDoubleList(String columnName) {
    return rowAsStruct.getDoubleList(columnName);
  }

  @Override
  public List<Long> getLongList(String columnName) {
    return rowAsStruct.getLongList(columnName);
  }

  @Override
  public boolean getBoolean(String columnName) {
    return rowAsStruct.getBoolean(columnName);
  }

  @Override
  public ByteArray getBytes(String columnName) {
    return rowAsStruct.getBytes(columnName);
  }

  @Override
  public Date getDate(String columnName) {
    return rowAsStruct.getDate(columnName);
  }

  @Override
  public List<Date> getDateList(String columnName) {
    return rowAsStruct.getDateList(columnName);
  }

  @Override
  public double getDouble(String columnName) {
    return rowAsStruct.getDouble(columnName);
  }

  @Override
  public long getLong(String columnName) {
    return rowAsStruct.getLong(columnName);
  }

  @Override
  public Timestamp getTimestamp(String columnName) {
    return rowAsStruct.getTimestamp(columnName);
  }

  @Override
  public long getSize() {
    // cloud.spanner doesn't provide access to the underlying proto object
    // so we can't call getSerializedSize on the row elements to calculate
    // a row size. Just return 0.
    return 0;
  }

  @Override
  public List<Timestamp> getTimestampList(String columnName) {
    return rowAsStruct.getTimestampList(columnName);
  }

  @Override
  public Type getType() {
    return convert(rowAsStruct.getType());
  }
}
