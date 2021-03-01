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

package com.google.spez.core.internal;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.spanner.v1.Type;
import java.util.List;

public interface Row {
  boolean isNull(String columnName);

  String getString(int columnIndex);

  String getString(String columnName);

  Type getColumnType(String columnName);

  List<Boolean> getBooleanList(String columnName);

  List<ByteArray> getBytesList(String columnName);

  List<String> getStringList(String columnName);

  List<Double> getDoubleList(String columnName);

  List<Long> getLongList(String columnName);

  boolean getBoolean(String columnName);

  ByteArray getBytes(String columnName);

  Date getDate(String columnName);

  List<Date> getDateList(String columnName);

  double getDouble(String columnName);

  long getLong(String columnName);

  Timestamp getTimestamp(String columnName);

  long getSize();

  List<Timestamp> getTimestampList(String columnName);

  Type getType();

  /*
   int getColumnCount();
   int getColumnIndex(String var1);
   Type getColumnType(int var1);
   Type getColumnType(String var1);
   boolean isNull(int var1);
   boolean isNull(String var1);
   boolean getBoolean(int var1);
   boolean getBoolean(String var1);
   long getLong(int var1);
   long getLong(String var1);
   double getDouble(int var1);
   double getDouble(String var1);
   String getString(int var1);
   String getString(String var1);
   <T extends MessageLite> T getProto(int var1, Class<T> var2);
   <T extends MessageLite> T getProto(String var1, Class<T> var2);
   ByteArray getBytes(int var1);
   ByteArray getBytes(String var1);
   Timestamp getTimestamp(int var1);
   Timestamp getTimestamp(String var1);
   Date getDate(int var1);
   Date getDate(String var1);
   boolean[] getBooleanArray(int var1);
   boolean[] getBooleanArray(String var1);
   List<Boolean> getBooleanList(int var1);
   List<Boolean> getBooleanList(String var1);
   long[] getLongArray(int var1);
   long[] getLongArray(String var1);
   List<Long> getLongList(int var1);
   List<Long> getLongList(String var1);
   double[] getDoubleArray(int var1);
   double[] getDoubleArray(String var1);
   List<Double> getDoubleList(int var1);
   List<Double> getDoubleList(String var1);
   List<String> getStringList(int var1);
   List<String> getStringList(String var1);
   List<ByteArray> getBytesList(int var1);
   List<ByteArray> getBytesList(String var1);
   List<Timestamp> getTimestampList(int var1);
   List<Date> getDateList(int var1);
   List<Row> getRowList(int var1);
   List<Row> getRowList(String var1);
  */
}
