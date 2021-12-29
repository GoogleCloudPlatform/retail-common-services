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

import com.google.spez.spanner.Row;
import com.google.spez.spanner.RowCursor;

public class GaxRowCursor implements RowCursor {
  @Override
  public String getString(String columnName) {
    return null;
  }

  @Override
  public Row getCurrentRow() {
    return null;
  }

  @Override
  public boolean next() {
    return false;
  }
}
