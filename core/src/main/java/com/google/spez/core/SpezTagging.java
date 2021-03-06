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

import io.opencensus.tags.TagContext;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagMetadata;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tagger;
import io.opencensus.tags.Tags;

public class SpezTagging {
  private static final Tagger tagger = Tags.getTagger();
  // frontendKey allows us to break down the recorded data.
  public static final TagKey TAILER_TABLE_KEY = TagKey.create("spez_tailer_table");

  /**
   * Created a tag for a given tableName.
   *
   * @param tableName name of the table
   * @return a tag
   */
  public static TagContext tagForTable(String tableName) {
    return tagger
        .currentBuilder()
        .put(
            TAILER_TABLE_KEY,
            TagValue.create(tableName),
            TagMetadata.create(TagMetadata.TagTtl.UNLIMITED_PROPAGATION))
        .build();
  }
}
