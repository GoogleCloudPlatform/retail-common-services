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

import com.google.spez.spanner.RowCursor;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
class UuidColumnChecker {
  private final SpezConfig.SinkConfig config;
  private boolean tableExists = false;
  private boolean columnExists = false;
  private boolean columnIsPrimaryKey = false;
  private boolean columnIsStringMax = false;

  public UuidColumnChecker(SpezConfig.SinkConfig config) {
    this.config = config;
  }

  public void checkRow(RowCursor rc) {
    tableExists = true;
    if (rc.getString("COLUMN_NAME").equals(config.getUuidColumn())) {
      columnExists = true;
      if (rc.getString("INDEX_TYPE").equals("PRIMARY_KEY")) {
        columnIsPrimaryKey = true;
      }
      if (rc.getString("SPANNER_TYPE").equals("STRING(MAX)")) {
        columnIsStringMax = true;
      }
    }
  }

  public void throwIfInvalid() {
    if (tableExists && columnExists && columnIsPrimaryKey && columnIsStringMax) {
      return;
    }

    String tableDescription = (tableExists ? "it does" : "it doesn't");
    String columnDescription = (columnExists ? "it does" : "it doesn't");
    String pkDescription = (columnIsPrimaryKey ? "it is" : "it is not");
    String typeDescription = (columnIsStringMax ? "it is" : "it is not");
    throw new IllegalStateException(
        "Spanner table '"
            + config.tablePath()
            + "' must exist ("
            + tableDescription
            + ") and contain a column named '"
            + config.getUuidColumn()
            + "' ("
            + columnDescription
            + ") which is a PRIMARY_KEY ("
            + pkDescription
            + ") and is of type STRING(MAX) ("
            + typeDescription
            + ")");
  }
}
