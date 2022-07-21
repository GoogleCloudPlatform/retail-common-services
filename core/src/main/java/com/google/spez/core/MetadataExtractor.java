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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.spez.spanner.Row;
import java.util.Map;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class MetadataExtractor {

  private final SpezConfig config;
  private final ImmutableMap<String, String> base;

  public MetadataExtractor(SpezConfig config) {
    this.config = config;
    this.base = ImmutableMap.copyOf(config.getBaseMetadata());
  }

  /**
   * extract metadata for a given row.
   *
   * @param row metadata is retrieved from this
   * @return a map of metadata key value pairs
   */
  public Map<String, String> extract(Row row) {
    Map<String, String> metadata = Maps.newHashMap(base);

    String uuid;
    String type = config.getSink().getUuidColumnType();
    // Spez currently only supports UUIDs of type STRING(MAX) or INT64
    if (type.equals("INT64")) {
      uuid = String.valueOf(row.getLong(config.getSink().getUuidColumn()));
    } else {
      uuid = row.getString(config.getSink().getUuidColumn());
    }
    String commitTimestamp = row.getTimestamp(config.getSink().getTimestampColumn()).toString();

    metadata.put(config.SINK_UUID_KEY, uuid);
    metadata.put(config.SINK_TIMESTAMP_KEY, commitTimestamp);

    return metadata;
  }
}
