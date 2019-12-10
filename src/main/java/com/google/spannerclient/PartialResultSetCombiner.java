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

import com.google.protobuf.Value;
import com.google.spanner.v1.PartialResultSet;

// TODO(pdex): consider using the builder pattern here
class PartialResultSetCombiner {

  public static PartialResultSet combine(PartialResultSet a, PartialResultSet b) {
    PartialResultSet.Builder combined = a.toBuilder();
    if (a.getChunkedValue()) {
      int lastValue = combined.getValuesCount() - 1;
      Value.Builder value = combined.getValuesBuilder(lastValue);
      if (value.getKindCase().equals(Value.KindCase.STRING_VALUE)) {
        value.setStringValue(value.getStringValue() + b.getValues(0).getStringValue());
      }
      combined.setChunkedValue(b.getChunkedValue());
    }
    return combined.build();
  }
}
