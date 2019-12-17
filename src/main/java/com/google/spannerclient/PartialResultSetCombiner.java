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

import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSet;
import java.util.List;
import java.util.Map;

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

  public static void appendToList(ListValue.Builder into, ListValue from, int start) {
    assert start <= from.getValuesCount();
    for (int i = start; i < from.getValuesCount(); i++) {
      into.addValues(from.getValues(i));
    }
  }

  public static void mergeChunk(ListValue.Builder chunkedValue, ListValue value) {
    if (value.getValuesCount() == 0) {
      return;
    }

    if (chunkedValue.getValuesCount() == 0) {
      chunkedValue.addAllValues(value.getValuesList());
      return;
    }

    Value.Builder lastValue = chunkedValue.getValuesBuilder(chunkedValue.getValuesCount() - 1);
    if (lastValue.getKindCase() != value.getValues(0).getKindCase()) {
      appendToList(chunkedValue, value, 0);
    } else if (value.getValues(0).getKindCase() == Value.KindCase.LIST_VALUE) {
      // [['a', 'b']]
      //      +
      // [['c', 'd']]
      //      =
      // [['a', 'bc', 'd']]
      mergeChunk(lastValue.getListValueBuilder(), value.getValues(0).getListValue());
      appendToList(chunkedValue, value, 1);
    } else if (value.getValues(0).getKindCase() == Value.KindCase.STRING_VALUE) {
      // ['a', 'b']
      //     +
      // ['c', 'd']
      //     =
      // ['a', 'bc', 'd']
      lastValue.setStringValue(
          lastValue.getStringValue().concat(value.getValues(0).getStringValue()));
      appendToList(chunkedValue, value, 1);
    } else {
      appendToList(chunkedValue, value, 0);
    }
  }

  public static void mergeChunk(Struct.Builder chunkedValue, Struct value) {
    if (value.getFieldsCount() == 0) {
      return;
    }

    if (chunkedValue.getFieldsCount() == 0) {
      chunkedValue.putAllFields(value.getFieldsMap());
      return;
    }

    for (Map.Entry<String, Value> entry : value.getFieldsMap().entrySet()) {
      if (chunkedValue.containsFields(entry.getKey())) {
        // key already exists in chunk append v to existing kv pair
        Value.Builder existing = chunkedValue.getFieldsOrThrow(entry.getKey()).toBuilder();
        if (existing.getKindCase() != entry.getValue().getKindCase()) {
          // error condition?
        } else if (existing.getKindCase() == Value.KindCase.LIST_VALUE) {
          mergeChunk(existing.getListValueBuilder(), entry.getValue().getListValue());
        } else if (existing.getKindCase() == Value.KindCase.STRING_VALUE) {
          existing.setStringValue(
              existing.getStringValue().concat(entry.getValue().getStringValue()));
        } else if (existing.getKindCase() == Value.KindCase.STRUCT_VALUE) {
          mergeChunk(existing.getStructValueBuilder(), entry.getValue().getStructValue());
        }
        chunkedValue.putFields(entry.getKey(), existing.build());
      } else {
        // key doesn't exist in chunk put kv pair
        chunkedValue.putFields(entry.getKey(), entry.getValue());
      }
    }
  }

  public static ResultSet combine(List<PartialResultSet> partials) {
    return combine(partials, 1, 0);
  }

  public static ResultSet combine(
      List<PartialResultSet> resultSet, int columnsPerRow, int startIndex) {
    ResultSet.Builder out = ResultSet.newBuilder();
    ListValue.Builder currentRow = null;
    Value.Builder chunkedValue = Value.newBuilder();
    for (int i = startIndex; i < resultSet.size(); i++) {
      PartialResultSet part = resultSet.get(i);
      if (part.hasMetadata()) {
        out.setMetadata(part.getMetadata());
      }
      if (part.hasStats()) {
        out.setStats(part.getStats());
      }
      for (int idx = 0; idx < part.getValuesCount(); idx++) {
        Value value = part.getValues(idx);
        // only the last value may be chunked
        // accumulate chunks into chunkedValue
        if (part.getChunkedValue() && idx == part.getValuesCount() - 1) {
          if (value.hasListValue()) {
            assert value.getListValue().getValuesCount() > 0;
            mergeChunk(chunkedValue.getListValueBuilder(), value.getListValue());
          } else if (value.hasStructValue()) {
            assert value.getStructValue().getFieldsCount() > 0;
            mergeChunk(chunkedValue.getStructValueBuilder(), value.getStructValue());
          } else {
            assert value.getKindCase() == Value.KindCase.STRING_VALUE;
            chunkedValue.setStringValue(
                chunkedValue.getStringValue().concat(value.getStringValue()));
          }
        } else {
          if (currentRow == null || currentRow.getValuesCount() == columnsPerRow) {
            currentRow = out.addRowsBuilder();
          }
          // first value in a parts list must be merged with the chunk
          if (chunkedValue.getKindCase() != Value.KindCase.KIND_NOT_SET) {
            assert idx == 0;
            if (chunkedValue.hasListValue()) {
              assert value.getListValue().getValuesCount() > 0;
              mergeChunk(chunkedValue.getListValueBuilder(), value.getListValue());
            } else if (chunkedValue.hasStructValue()) {
              assert value.getStructValue().getFieldsCount() > 0;
              mergeChunk(chunkedValue.getStructValueBuilder(), value.getStructValue());
            } else {
              assert value.getKindCase() == Value.KindCase.STRING_VALUE;
              chunkedValue.setStringValue(
                  chunkedValue.getStringValue().concat(value.getStringValue()));
            }
            currentRow.addValues(chunkedValue);
            chunkedValue.clear();
          } else {
            currentRow.addValues(value);
          }
        }
      }
    }
    return out.build();
  }
}
