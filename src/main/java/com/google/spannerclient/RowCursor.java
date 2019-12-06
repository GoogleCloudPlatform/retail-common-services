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
package com.google.spannerclient;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.MessageLite;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.util.List;

public class RowCursor implements RowBase, AutoCloseable {
  private final ImmutableList<StructType.Field> fields;
  private final ImmutableList<ListValue> rows;
  private int index = -1;

  private RowCursor(ImmutableList<StructType.Field> fields, ImmutableList<ListValue> rows) {
    this.fields = fields;
    this.rows = rows;
  }

  public static RowCursor of(
      ImmutableList<StructType.Field> fields, ImmutableList<ListValue> rows) {
    Preconditions.checkNotNull(fields);
    Preconditions.checkNotNull(rows);

    return new RowCursor(fields, rows);
  }

  public boolean next() {
    return ++index < rows.size();
  }

  private Row getCurrentRow() {
    Preconditions.checkState(index >= 0, "Must be preceded by a next() call");
    Preconditions.checkElementIndex(index, rows.size(), "All rows have been yielded");
    return Row.of(fields, ImmutableList.copyOf(rows.get(index).getValuesList()));
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#string()}.
   *
   * @param columnIndex
   */
  @Override
  public String getString(int columnIndex) {
    return getCurrentRow().getString(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#string()}.
   *
   * @param columnName
   */
  @Override
  public String getString(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().getString(columnIndex);
  }

  /**
   * Parse the column value into a protocol message of the given type.
   *
   * @throws IllegalStateException if parsing failed state check
   * @throws UninitializedMessageException if the parsed proto is not initialized
   * @throws NullPointerException if the column value is null
   * @throws IllegalArgumentException if given class is not a protocol message type
   * @param columnIndex
   */
  @Override
  public <T extends MessageLite> T getProto(int columnIndex, Class<T> clazz) {
    return getCurrentRow().getProto(columnIndex, clazz);
  }

  /**
   * Parse the column value into a protocol message of the given type.
   *
   * @throws IllegalStateException if parsing failed state check
   * @throws UninitializedMessageException if the parsed proto is not initialized
   * @throws NullPointerException if the column value is null
   * @throws IllegalArgumentException if given class is not a protocol message type
   * @param columnName
   */
  @Override
  public <T extends MessageLite> T getProto(String columnName, Class<T> clazz) {
    return getCurrentRow().getProto(columnName, clazz);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#bytes()}.
   *
   * @param columnIndex
   */
  @Override
  public ByteArray getBytes(int columnIndex) {
    return getCurrentRow().getBytes(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#bytes()}.
   *
   * @param columnName
   */
  @Override
  public ByteArray getBytes(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().getBytes(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#timestamp()}.
   *
   * @param columnIndex
   */
  @Override
  public Timestamp getTimestamp(int columnIndex) {
    return getCurrentRow().getTimestamp(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#timestamp()}.
   *
   * @param columnName
   */
  @Override
  public Timestamp getTimestamp(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().getTimestamp(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#date()}.
   *
   * @param columnIndex
   */
  @Override
  public Date getDate(int columnIndex) {
    return getCurrentRow().getDate(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#date()}.
   *
   * @param columnName
   */
  @Override
  public Date getDate(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().getDate(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.bool())}.
   *
   * @param columnIndex
   * @throws NullPointerException if any element of the array value is {@code NULL}. If the array
   *     may contain {@code NULL} values, use {@link #getBooleanList(int)} instead.
   */
  @Override
  public boolean[] getBooleanArray(int columnIndex) {
    return getCurrentRow().getBooleanArray(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.bool())}.
   *
   * @param columnName
   * @throws NullPointerException if any element of the array value is {@code NULL}. If the array
   *     may contain {@code NULL} values, use {@link #getBooleanList(String)} instead.
   */
  @Override
  public boolean[] getBooleanArray(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().getBooleanArray(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.bool())}.
   *
   * @param columnIndex
   */
  @Override
  public List<Boolean> getBooleanList(int columnIndex) {
    return getCurrentRow().getBooleanList(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.bool())}.
   *
   * @param columnName
   */
  @Override
  public List<Boolean> getBooleanList(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().getBooleanList(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.int64())}.
   *
   * @param columnIndex
   * @throws NullPointerException if any element of the array value is {@code NULL}. If the array
   *     may contain {@code NULL} values, use {@link #getLongList(int)} instead.
   */
  @Override
  public long[] getLongArray(int columnIndex) {
    return getCurrentRow().getLongArray(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.int64())}.
   *
   * @param columnName
   * @throws NullPointerException if any element of the array value is {@code NULL}. If the array
   *     may contain {@code NULL} values, use {@link #getLongList(String)} instead.
   */
  @Override
  public long[] getLongArray(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().getLongArray(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.int64())}.
   *
   * @param columnIndex
   */
  @Override
  public List<Long> getLongList(int columnIndex) {
    return getCurrentRow().getLongList(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.int64())}.
   *
   * @param columnName
   */
  @Override
  public List<Long> getLongList(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().getLongList(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.float64())}.
   *
   * @param columnIndex
   * @throws NullPointerException if any element of the array value is {@code NULL}. If the array
   *     may contain {@code NULL} values, use {@link #getDoubleList(int)} instead.
   */
  @Override
  public double[] getDoubleArray(int columnIndex) {
    return new double[0];
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.float64())}.
   *
   * @param columnName
   * @throws NullPointerException if any element of the array value is {@code NULL}. If the array
   *     may contain {@code NULL} values, use {@link #getDoubleList(String)} instead.
   */
  @Override
  public double[] getDoubleArray(String columnName) {
    return new double[0];
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.float64())}.
   *
   * @param columnIndex
   */
  @Override
  public List<Double> getDoubleList(int columnIndex) {
    return null;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.float64())}.
   *
   * @param columnName
   */
  @Override
  public List<Double> getDoubleList(String columnName) {
    return null;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.string())}.
   *
   * @param columnIndex
   */
  @Override
  public List<String> getStringList(int columnIndex) {
    return null;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.string())}.
   *
   * @param columnName
   */
  @Override
  public List<String> getStringList(String columnName) {
    return null;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.bytes())}.
   *
   * @param columnIndex
   */
  @Override
  public List<ByteArray> getBytesList(int columnIndex) {
    return null;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.bytes())}.
   *
   * @param columnName
   */
  @Override
  public List<ByteArray> getBytesList(String columnName) {
    return null;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.timestamp())}.
   *
   * @param columnIndex
   */
  @Override
  public List<Timestamp> getTimestampList(int columnIndex) {
    return null;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.timestamp())}.
   *
   * @param columnName
   */
  @Override
  public List<Timestamp> getTimestampList(String columnName) {
    return null;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.date())}.
   *
   * @param columnIndex
   */
  @Override
  public List<Date> getDateList(int columnIndex) {
    return null;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.date())}.
   *
   * @param columnName
   */
  @Override
  public List<Date> getDateList(String columnName) {
    return null;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.struct(...))}.
   *
   * @param columnIndex
   */
  @Override
  public List<Row> getRowList(int columnIndex) {
    return null;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.struct(...))}.
   *
   * @param columnName
   */
  @Override
  public List<Row> getRowList(String columnName) {
    return null;
  }

  /**
   * Returns the type of the underlying data. This will always be a {@code STRUCT} type, with fields
   * corresponding to the data's columns. For the result of a read or query, this will always match
   * the columns passed to the {@code read()} call or named in the query text, in order.
   */
  @Override
  public Type getType() {
    return null;
  }

  /**
   * Returns the number of columns in the underlying data. This includes any columns with {@code
   * NULL} values.
   */
  @Override
  public int getColumnCount() {
    return fields.size();
  }

  /**
   * Returns the index of the column named {@code columnName}.
   *
   * @param columnName
   * @throws IllegalArgumentException if there is not exactly one element of {@code
   *     type().structFields()} with {@link Type.StructField#getName()} equal to {@code columnName}
   */
  @Override
  public int getColumnIndex(String columnName) {
    for (int i = 0; i < fields.size() - 1; i++) {
      if (fields.get(i).getName().equals(columnName)) {
        return i;
      }
    }

    throw new IllegalArgumentException();
  }

  /**
   * Returns the type of a column.
   *
   * @param columnIndex
   */
  @Override
  public Type getColumnType(int columnIndex) {
    return getCurrentRow().getColumnType(columnIndex);
  }

  /**
   * Returns the type of a column.
   *
   * @param columnName
   */
  @Override
  public Type getColumnType(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().getColumnType(columnIndex);
  }

  /**
   * Returns {@code true} if a column contains a {@code NULL} value.
   *
   * @param columnIndex
   */
  @Override
  public boolean isNull(int columnIndex) {
    return getCurrentRow().isNull(columnIndex);
  }

  /**
   * Returns {@code true} if a column contains a {@code NULL} value.
   *
   * @param columnName
   */
  @Override
  public boolean isNull(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().isNull(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#bool()}.
   *
   * @param columnIndex
   */
  @Override
  public boolean getBoolean(int columnIndex) {
    return getCurrentRow().getBoolean(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#bool()}.
   *
   * @param columnName
   */
  @Override
  public boolean getBoolean(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().getBoolean(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link TypeCode#int64()}.
   *
   * @param columnIndex
   */
  @Override
  public long getLong(int columnIndex) {
    return getCurrentRow().getLong(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#int64()}.
   *
   * @param columnName
   */
  @Override
  public long getLong(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().getLong(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#float64()}.
   *
   * @param columnIndex
   */
  @Override
  public double getDouble(int columnIndex) {
    return getCurrentRow().getDouble(columnIndex);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#float64()}.
   *
   * @param columnName
   */
  @Override
  public double getDouble(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    return getCurrentRow().getDouble(columnIndex);
  }

  @Override
  public void close() throws Exception {}
}
