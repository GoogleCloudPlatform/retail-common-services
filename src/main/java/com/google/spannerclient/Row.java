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
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Value;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class Row implements RowBase {

  private final ImmutableList<StructType.Field> fields;
  private final ImmutableList<Value> values;

  public Row(ImmutableList<StructType.Field> fields, ImmutableList<Value> values) {

    this.fields = fields;
    this.values = values;
  }

  public static Row of(ImmutableList<StructType.Field> fields, ImmutableList<Value> values) {
    Preconditions.checkNotNull(fields);
    Preconditions.checkNotNull(values);

    return new Row(fields, values);
  }

  /**
   * Returns the type of the underlying data. This will always be a {@code STRUCT} type, with fields
   * corresponding to the data's columns. For the result of a read or query, this will always match
   * the columns passed to the {@code read()} call or named in the query text, in order.
   */
  @Override
  public Type getType() {
    // TODO(xjdr): Is this even needed?
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
    Preconditions.checkState(columnIndex <= fields.size() - 1);

    return fields.get(columnIndex).getType();
  }

  /**
   * Returns the type of a column.
   *
   * @param columnName
   */
  @Override
  public Type getColumnType(String columnName) {
    final int columnIndex = getColumnIndex(columnName);

    return fields.get(columnIndex).getType();
  }

  /**
   * Returns {@code true} if a column contains a {@code NULL} value.
   *
   * @param columnIndex
   */
  @Override
  public boolean isNull(int columnIndex) {
    Preconditions.checkState(columnIndex <= values.size() - 1);

    return values.get(columnIndex).getKindCase().equals(Value.KindCase.NULL_VALUE);
  }

  /**
   * Returns {@code true} if a column contains a {@code NULL} value.
   *
   * @param columnName
   */
  @Override
  public boolean isNull(String columnName) {
    final int columnIndex = getColumnIndex(columnName);

    return values.get(columnIndex).getKindCase().equals(Value.KindCase.NULL_VALUE);
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#bool()}.
   *
   * @param columnIndex
   */
  @Override
  public boolean getBoolean(int columnIndex) {
    Preconditions.checkState(columnIndex <= values.size() - 1);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.BOOL));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.BOOL_VALUE));

    return values.get(columnIndex).getBoolValue();
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#bool()}.
   *
   * @param columnName
   */
  @Override
  public boolean getBoolean(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.BOOL));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.BOOL_VALUE));

    return values.get(columnIndex).getBoolValue();
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#int64()}.
   *
   * @param columnIndex
   */
  @Override
  public long getLong(int columnIndex) {
    Preconditions.checkState(columnIndex <= values.size() - 1);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.INT64));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    return Longs.tryParse(values.get(columnIndex).getStringValue());
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#int64()}.
   *
   * @param columnName
   */
  @Override
  public long getLong(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.INT64));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    return Longs.tryParse(values.get(columnIndex).getStringValue());
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#float64()}.
   *
   * @param columnIndex
   */
  @Override
  public double getDouble(int columnIndex) {
    Preconditions.checkState(columnIndex <= values.size() - 1);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.FLOAT64));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    return Doubles.tryParse(values.get(columnIndex).getStringValue());
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#float64()}.
   *
   * @param columnName
   */
  @Override
  public double getDouble(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.FLOAT64));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    return Doubles.tryParse(values.get(columnIndex).getStringValue());
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
    Preconditions.checkState(columnIndex <= values.size() - 1);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.STRING));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    try {
      final Method m = clazz.getMethod("getDefaultInstance");
      final T t = (T) m.invoke(null);
      final MessageLite.Builder messageBuilder = t.newBuilderForType();

      return clazz.cast(messageBuilder.build());
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      throw new IllegalArgumentException("Invalid proto class " + clazz.getCanonicalName(), e);
    }
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
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(columnIndex <= values.size() - 1);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.STRING));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    try {
      final Method m = clazz.getMethod("getDefaultInstance");
      final T t = (T) m.invoke(null);
      final MessageLite.Builder messageBuilder = t.newBuilderForType();

      return clazz.cast(messageBuilder.build());
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      throw new IllegalArgumentException("Invalid proto class " + clazz.getCanonicalName(), e);
    }
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#string()}.
   *
   * @param columnIndex
   */
  @Override
  public String getString(int columnIndex) {
    Preconditions.checkState(columnIndex <= values.size() - 1);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.STRING));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    return values.get(columnIndex).getStringValue();
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#string()}.
   *
   * @param columnName
   */
  @Override
  public String getString(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.STRING));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    return values.get(columnIndex).getStringValue();
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#bytes()}.
   *
   * @param columnIndex
   */
  @Override
  public ByteArray getBytes(int columnIndex) {
    Preconditions.checkState(columnIndex <= values.size() - 1);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.BYTES));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    return ByteArray.copyFrom(values.get(columnIndex).getStringValue());
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#bytes()}.
   *
   * @param columnName
   */
  @Override
  public ByteArray getBytes(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.BYTES));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    return ByteArray.copyFrom(values.get(columnIndex).getStringValue());
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#timestamp()}.
   *
   * @param columnIndex
   */
  @Override
  public Timestamp getTimestamp(int columnIndex) {
    Preconditions.checkState(columnIndex <= values.size() - 1);
    Preconditions.checkState(
        fields.get(columnIndex).getType().getCode().equals(TypeCode.TIMESTAMP));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    return Timestamp.parseTimestamp(values.get(columnIndex).getStringValue());
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#timestamp()}.
   *
   * @param columnName
   */
  @Override
  public Timestamp getTimestamp(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(
        fields.get(columnIndex).getType().getCode().equals(TypeCode.TIMESTAMP));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    return Timestamp.parseTimestamp(values.get(columnIndex).getStringValue());
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#date()}.
   *
   * @param columnIndex
   */
  @Override
  public Date getDate(int columnIndex) {
    Preconditions.checkState(columnIndex <= values.size() - 1);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.DATE));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    return Date.parseDate(values.get(columnIndex).getStringValue());
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@link Type#date()}.
   *
   * @param columnName
   */
  @Override
  public Date getDate(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.DATE));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.STRING_VALUE));

    return Date.parseDate(values.get(columnIndex).getStringValue());
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
    final List<Boolean> bl = getBooleanList(columnIndex);
    final boolean[] ba = new boolean[bl.size()];
    final int[] i = new int[1];

    i[0] = 0;

    bl.forEach(
        x -> {
          ba[i[0]] = x;
          i[0]++;
        });

    return ba;
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
    final List<Boolean> bl = getBooleanList(columnName);
    final boolean[] ba = new boolean[bl.size()];
    final int[] i = new int[1];

    i[0] = 0;

    bl.forEach(
        x -> {
          ba[i[0]] = x;
          i[0]++;
        });

    return ba;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.bool())}.
   *
   * @param columnIndex
   */
  @Override
  public List<Boolean> getBooleanList(int columnIndex) {
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));
    final List<Boolean> booleanList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.BOOL_VALUE));
              booleanList.add(v.getBoolValue());
            });

    return booleanList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.bool())}.
   *
   * @param columnName
   */
  @Override
  public List<Boolean> getBooleanList(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<Boolean> booleanList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.BOOL_VALUE));
              booleanList.add(v.getBoolValue());
            });

    return booleanList;
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
    final List<Long> ll = getLongList(columnIndex);
    final long[] la = new long[ll.size()];
    final int[] i = new int[1];

    i[0] = 0;

    ll.forEach(
        x -> {
          la[i[0]] = x;
          i[0]++;
        });

    return la;
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
    final List<Long> ll = getLongList(columnName);
    final long[] la = new long[ll.size()];
    final int[] i = new int[1];

    i[0] = 0;

    ll.forEach(
        x -> {
          la[i[0]] = x;
          i[0]++;
        });

    return la;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.int64())}.
   *
   * @param columnIndex
   */
  @Override
  public List<Long> getLongList(int columnIndex) {
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<Long> longList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              longList.add(Longs.tryParse(v.getStringValue()));
            });

    return longList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.int64())}.
   *
   * @param columnName
   */
  @Override
  public List<Long> getLongList(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<Long> longList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              longList.add(Longs.tryParse(v.getStringValue()));
            });

    return longList;
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
    final List<Double> dl = getDoubleList(columnIndex);
    final double[] da = new double[dl.size()];
    final int[] i = new int[1];

    i[0] = 0;

    dl.forEach(
        x -> {
          da[i[0]] = x;
          i[0]++;
        });

    return da;
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
    final List<Double> dl = getDoubleList(columnName);
    final double[] da = new double[dl.size()];
    final int[] i = new int[1];

    i[0] = 0;

    dl.forEach(
        x -> {
          da[i[0]] = x;
          i[0]++;
        });

    return da;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.float64())}.
   *
   * @param columnIndex
   */
  @Override
  public List<Double> getDoubleList(int columnIndex) {
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<Double> doubleList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              doubleList.add(Doubles.tryParse(v.getStringValue()));
            });

    return doubleList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.float64())}.
   *
   * @param columnName
   */
  @Override
  public List<Double> getDoubleList(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<Double> doubleList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              doubleList.add(Doubles.tryParse(v.getStringValue()));
            });

    return doubleList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.string())}.
   *
   * @param columnIndex
   */
  @Override
  public List<String> getStringList(int columnIndex) {
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<String> stringList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              stringList.add(v.getStringValue());
            });

    return stringList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.string())}.
   *
   * @param columnName
   */
  @Override
  public List<String> getStringList(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<String> stringList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              stringList.add(v.getStringValue());
            });

    return stringList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.bytes())}.
   *
   * @param columnIndex
   */
  @Override
  public List<ByteArray> getBytesList(int columnIndex) {
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<ByteArray> baList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              baList.add(ByteArray.copyFrom(v.getStringValue()));
            });

    return baList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.bytes())}.
   *
   * @param columnName
   */
  @Override
  public List<ByteArray> getBytesList(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<ByteArray> baList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              baList.add(ByteArray.copyFrom(v.getStringValue()));
            });

    return baList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.timestamp())}.
   *
   * @param columnIndex
   */
  @Override
  public List<Timestamp> getTimestampList(int columnIndex) {
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<Timestamp> timestampList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              timestampList.add(Timestamp.parseTimestamp(v.getStringValue()));
            });

    return timestampList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.timestamp())}.
   *
   * @param columnName
   */
  @Override
  public List<Timestamp> getTimestampList(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<Timestamp> timestampList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              timestampList.add(Timestamp.parseTimestamp(v.getStringValue()));
            });

    return timestampList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.date())}.
   *
   * @param columnIndex
   */
  @Override
  public List<Date> getDateList(int columnIndex) {
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<Date> dateList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              dateList.add(Date.parseDate(v.getStringValue()));
            });

    return dateList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.date())}.
   *
   * @param columnName
   */
  @Override
  public List<Date> getDateList(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<Date> dateList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              dateList.add(Date.parseDate(v.getStringValue()));
            });

    return dateList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.struct(...))}.
   *
   * @param columnIndex
   */
  @Override
  public List<Row> getRowList(int columnIndex) {
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<Row> rowList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              // TODO(xjdr): Finish the impl here
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              // rowList.add(Row.of());
            });

    return rowList;
  }

  /**
   * Returns the value of a non-{@code NULL} column with type {@code Type.array(Type.struct(...))}.
   *
   * @param columnName
   */
  @Override
  public List<Row> getRowList(String columnName) {
    final int columnIndex = getColumnIndex(columnName);
    Preconditions.checkState(fields.get(columnIndex).getType().getCode().equals(TypeCode.ARRAY));
    Preconditions.checkState(
        values.get(columnIndex).getKindCase().equals(Value.KindCase.LIST_VALUE));

    final List<Row> rowList = new ArrayList<>();

    values
        .get(columnIndex)
        .getListValue()
        .getValuesList()
        .forEach(
            v -> {
              // TODO(xjdr): Finish the impl here
              Preconditions.checkState(v.getKindCase().equals(Value.KindCase.STRING_VALUE));
              // rowList.add(Row.of());
            });

    return rowList;
  }
}
