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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;

@AutoValue
public abstract class SchemaSet {
  static SchemaSet create(Schema avroSchema, ImmutableMap<String, String> spannerSchema) {
    return new AutoValue_SchemaSet(avroSchema, spannerSchema);
  }

  abstract Schema avroSchema();

  abstract ImmutableMap<String, String> spannerSchema();
}
