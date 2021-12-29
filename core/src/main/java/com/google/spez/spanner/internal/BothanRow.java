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
import com.google.spanner.v1.Type;
import com.google.spez.spanner.Row;
import java.util.List;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class BothanRow implements Row {
  private final com.google.spannerclient.Row row;

  public BothanRow(com.google.spannerclient.Row row) {
    this.row = row;
  }

  @Override
  public boolean isNull(String columnName) {
    return row.isNull(columnName);
  }

  @Override
  public String getString(int columnIndex) {
    return row.getString(columnIndex);
  }

  @Override
  public String getString(String columnName) {
    return row.getString(columnName);
  }

  @Override
  public Type getColumnType(String columnName) {
    return row.getColumnType(columnName);
  }

  @Override
  public List<Boolean> getBooleanList(String columnName) {
    return row.getBooleanList(columnName);
  }

  @Override
  public List<ByteArray> getBytesList(String columnName) {
    return row.getBytesList(columnName);
  }

  @Override
  public List<String> getStringList(String columnName) {
    return row.getStringList(columnName);
  }

  @Override
  public List<Double> getDoubleList(String columnName) {
    return row.getDoubleList(columnName);
  }

  @Override
  public List<Long> getLongList(String columnName) {
    return row.getLongList(columnName);
  }

  @Override
  public boolean getBoolean(String columnName) {
    return row.getBoolean(columnName);
  }

  @Override
  public ByteArray getBytes(String columnName) {
    return row.getBytes(columnName);
  }

  @Override
  public Date getDate(String columnName) {
    return row.getDate(columnName);
  }

  @Override
  public List<Date> getDateList(String columnName) {
    return row.getDateList(columnName);
  }

  @Override
  public double getDouble(String columnName) {
    return row.getDouble(columnName);
  }

  @Override
  public long getLong(String columnName) {
    return row.getLong(columnName);
  }

  @Override
  public Timestamp getTimestamp(String columnName) {
    return row.getTimestamp(columnName);
  }

  @Override
  public long getSize() {
    return row.getSize();
  }

  @Override
  public List<Timestamp> getTimestampList(String columnName) {
    return row.getTimestampList(columnName);
  }

  @Override
  public Type getType() {
    return row.getType();
  }
}
