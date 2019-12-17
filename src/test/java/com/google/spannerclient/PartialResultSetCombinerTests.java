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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSet;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class PartialResultSetCombinerTests {

  @Test
  void stringCombine() {
    /*
    # Strings are concatenated.
    "foo", "bar" => "foobar"
        */
    PartialResultSet a =
        PartialResultSet.newBuilder()
            .addValues(Value.newBuilder().setStringValue("foo").build())
            .setChunkedValue(true)
            .build();
    PartialResultSet b =
        PartialResultSet.newBuilder()
            .addValues(Value.newBuilder().setStringValue("bar").build())
            .setChunkedValue(false)
            .build();
    ResultSet r = PartialResultSetCombiner.combine(Arrays.asList(a, b));

    assertEquals("foobar", r.getRows(0).getValues(0).getStringValue());
  }

  @Test
  void integerCombine() {
    /*
    # Lists of non-strings are concatenated.
    [2, 3], [4] => [2, 3, 4]
        */
    PartialResultSet a =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setNumberValue(2))
                            .addValues(Value.newBuilder().setNumberValue(3))))
            .setChunkedValue(true)
            .build();
    PartialResultSet b =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder().addValues(Value.newBuilder().setNumberValue(4))))
            .setChunkedValue(false)
            .build();
    ResultSet r = PartialResultSetCombiner.combine(Arrays.asList(a, b));
    assertEquals(
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setNumberValue(2))
            .addValues(Value.newBuilder().setNumberValue(3))
            .addValues(Value.newBuilder().setNumberValue(4))
            .build(),
        r.getRows(0).getValues(0).getListValue());
  }

  @Test
  void stringListCombine() {
    /*
    # Lists are concatenated, but the last and first elements are merged
    # because they are strings.
    ["a", "b"], ["c", "d"] => ["a", "bc", "d"]
        */
    PartialResultSet a =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("a"))
                            .addValues(Value.newBuilder().setStringValue("b"))))
            .setChunkedValue(true)
            .build();
    PartialResultSet b =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("c"))
                            .addValues(Value.newBuilder().setStringValue("d"))))
            .setChunkedValue(false)
            .build();
    ResultSet r = PartialResultSetCombiner.combine(Arrays.asList(a, b));
    assertEquals(
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("a"))
            .addValues(Value.newBuilder().setStringValue("bc"))
            .addValues(Value.newBuilder().setStringValue("d"))
            .build(),
        r.getRows(0).getValues(0).getListValue());
  }

  @Test
  void stringListAndSublistCombine() {
    /*
    # Lists are concatenated, but the last and first elements are merged
    # because they are lists. Recursively, the last and first elements
    # of the inner lists are merged because they are strings.
    ["a", ["b", "c"]], [["d"], "e"] => ["a", ["b", "cd"], "e"]
        */
    PartialResultSet a =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("a"))
                            .addValues(
                                Value.newBuilder()
                                    .setListValue(
                                        ListValue.newBuilder()
                                            .addValues(Value.newBuilder().setStringValue("b"))
                                            .addValues(Value.newBuilder().setStringValue("c"))))))
            .setChunkedValue(true)
            .build();
    PartialResultSet b =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(
                                Value.newBuilder()
                                    .setListValue(
                                        ListValue.newBuilder()
                                            .addValues(Value.newBuilder().setStringValue("d"))))
                            .addValues(Value.newBuilder().setStringValue("e"))))
            .setChunkedValue(false)
            .build();
    ResultSet r = PartialResultSetCombiner.combine(Arrays.asList(a, b));
    assertEquals(
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("a"))
            .addValues(
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("b"))
                            .addValues(Value.newBuilder().setStringValue("cd"))))
            .addValues(Value.newBuilder().setStringValue("e"))
            .build(),
        r.getRows(0).getValues(0).getListValue());
  }

  @Test
  void objectFieldsCombine() {
    /*
    # Non-overlapping object fields are combined.
    {"a": "1"}, {"b": "2"} => {"a": "1", "b": 2"}
        */
    PartialResultSet a =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setStructValue(
                        Struct.newBuilder()
                            .putFields("a", Value.newBuilder().setStringValue("1").build())))
            .setChunkedValue(true)
            .build();
    PartialResultSet b =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setStructValue(
                        Struct.newBuilder()
                            .putFields("b", Value.newBuilder().setStringValue("2").build())))
            .setChunkedValue(false)
            .build();
    ResultSet r = PartialResultSetCombiner.combine(Arrays.asList(a, b));
    assertEquals(
        Struct.newBuilder()
            .putFields("a", Value.newBuilder().setStringValue("1").build())
            .putFields("b", Value.newBuilder().setStringValue("2").build())
            .build(),
        r.getRows(0).getValues(0).getStructValue());
  }

  @Test
  void objectFieldsOverlappingCombine() {
    /*
    # Overlapping object fields are merged.
    {"a": "1"}, {"a": "2"} => {"a": "12"}
        */
    PartialResultSet a =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setStructValue(
                        Struct.newBuilder()
                            .putFields("a", Value.newBuilder().setStringValue("1").build())))
            .setChunkedValue(true)
            .build();
    PartialResultSet b =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setStructValue(
                        Struct.newBuilder()
                            .putFields("a", Value.newBuilder().setStringValue("2").build())))
            .setChunkedValue(false)
            .build();
    ResultSet r = PartialResultSetCombiner.combine(Arrays.asList(a, b));
    assertEquals(
        Struct.newBuilder().putFields("a", Value.newBuilder().setStringValue("12").build()).build(),
        r.getRows(0).getValues(0).getStructValue());
  }

  @Test
  void objectFieldsOverlappingSublistCombine() {
    /*
    # Examples of merging objects containing lists of strings.
    {"a": ["1"]}, {"a": ["2"]} => {"a": ["12"]}
        */
    PartialResultSet a =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setStructValue(
                        Struct.newBuilder()
                            .putFields(
                                "a",
                                Value.newBuilder()
                                    .setListValue(
                                        ListValue.newBuilder()
                                            .addValues(Value.newBuilder().setStringValue("1")))
                                    .build())))
            .setChunkedValue(true)
            .build();
    PartialResultSet b =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setStructValue(
                        Struct.newBuilder()
                            .putFields(
                                "a",
                                Value.newBuilder()
                                    .setListValue(
                                        ListValue.newBuilder()
                                            .addValues(Value.newBuilder().setStringValue("2")))
                                    .build())))
            .setChunkedValue(false)
            .build();
    ResultSet r = PartialResultSetCombiner.combine(Arrays.asList(a, b));
    assertEquals(
        Struct.newBuilder()
            .putFields(
                "a",
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("12")))
                    .build())
            .build(),
        r.getRows(0).getValues(0).getStructValue());
  }

  @Test
  void aMoreCompleteExampleCombine() {
    /*
    For a more complete example, suppose a streaming SQL query is yielding a result set whose rows contain a single string field. The following PartialResultSets might be yielded:

    {
      "metadata": { ... }
      "values": ["Hello", "W"]
      "chunkedValue": true
      "resumeToken": "Af65..."
    }
    {
      "values": ["orl"]
      "chunkedValue": true
      "resumeToken": "Bqp2..."
    }
    {
      "values": ["d"]
      "resumeToken": "Zx1B..."
    }
    This sequence of PartialResultSets encodes two rows, one containing the field value "Hello", and a second containing the field value "World" = "W" + "orl" + "d".
        */
    PartialResultSet a =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder()
                            .addValues(Value.newBuilder().setStringValue("Hello"))
                            .addValues(Value.newBuilder().setStringValue("W"))))
            .setChunkedValue(true)
            .build();
    PartialResultSet b =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("orl"))))
            .setChunkedValue(true)
            .build();
    PartialResultSet c =
        PartialResultSet.newBuilder()
            .addValues(
                Value.newBuilder()
                    .setListValue(
                        ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("d"))))
            .setChunkedValue(false)
            .build();
    ResultSet r = PartialResultSetCombiner.combine(Arrays.asList(a, b, c));
    assertEquals(
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setStringValue("Hello"))
            .addValues(Value.newBuilder().setStringValue("World"))
            .build(),
        r.getRows(0).getValues(0).getListValue());
  }
}
