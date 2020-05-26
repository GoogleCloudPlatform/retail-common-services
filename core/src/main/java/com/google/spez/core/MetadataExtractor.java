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
import com.google.spannerclient.Row;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataExtractor {

  private static final Logger log = LoggerFactory.getLogger(MetadataExtractor.class);

  private final SpezConfig config;
  private final ImmutableMap base;

  public MetadataExtractor(SpezConfig config) {
    this.config = config;
    this.base = ImmutableMap.copyOf(config.getBaseMetadata());
  }

  public Map<String, String> extract(Row row) {
    var metadata = Maps.newHashMap(base);

    String uuid = Long.toString(row.getLong(config.getSpannerDb().getUuidColumn()));
    String commitTimestamp =
        row.getTimestamp(config.getSpannerDb().getTimestampColumn()).toString();

    metadata.put(config.SPANNERDB_UUID_KEY, uuid);
    metadata.put(config.SPANNERDB_TIMESTAMP_KEY, uuid);

    return metadata;
  }
}
