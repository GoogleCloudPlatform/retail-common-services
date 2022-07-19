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

import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SpannerTailerTest implements WithAssertions {

  /*
   * SpannerTailer's last processed timestamp relies on records being processed IN ORDER
   * from oldest to newest. If this test fails, it means you've broken an assumption
   * that the entire system is built upon. Do not pass go, do not collect $200. You
   * are going to have a very bad day.
   */
  @Test
  public void shouldUseOrderByClause() {
    String query =
        SpannerTailer.buildLptsTableQuery("sink_table", "timestamp", "2019-08-08T20:30:39.802644Z");
    assertThat(query).isNotNull();
    assertThat(query)
        .isEqualTo(
            "SELECT * FROM sink_table WHERE timestamp"
                + " > '2019-08-08T20:30:39.802644Z' ORDER BY timestamp DESC LIMIT "
                + String.valueOf(950 * 12)
                + " ");
  }
}
