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

import com.google.cloud.Timestamp;
import com.google.spez.spanner.Row;
import com.google.spez.spanner.RowCursor;

public class GaxRowCursor implements RowCursor {
  private final com.google.cloud.spanner.ResultSet cursor;

  public GaxRowCursor(com.google.cloud.spanner.ResultSet cursor) {
    this.cursor = cursor;
  }

  @Override
  public String getString(String columnName) {
    return cursor.getString(columnName);
  }

  @Override
  public Timestamp getTimestamp(String columnName) {
    return cursor.getTimestamp(columnName);
  }

  @Override
  public Row getCurrentRow() {
    return new GaxRow(cursor.getCurrentRowAsStruct());
  }

  @Override
  public boolean next() {
    return cursor.next();
  }
}
