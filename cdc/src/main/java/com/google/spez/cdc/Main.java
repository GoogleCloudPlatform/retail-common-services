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
package com.google.spez.cdc;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.ByteString;
import com.google.spez.core.EventPublisher;
import com.google.spez.core.SpannerEventHandler;
import com.google.spez.core.SpannerTailer;
import com.google.spez.core.SpannerToAvro;
import com.google.spez.core.Spez;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    final List<ListeningExecutorService> l =
        Spez.ServicePoolGenerator(32, "Spanner Tailer Event Worker");

    final SpannerTailer tailer = new SpannerTailer(32, 200000000);
    final EventPublisher publisher = new EventPublisher("{project_id}", "test-topic");
    final Map<String, String> metadata = new HashMap<>();

    // Populate CDC Metadata
    metadata.put("test_key1", "test_val1");
    metadata.put("test_key2", "test_val2");

    final SpannerToAvro.SchemaSet schemaSet =
        tailer.getSchema("{project_id}", "test-db", "test", "test");

    final SpannerEventHandler handler =
        (bucket, s, timestamp) -> {
          Optional<ByteString> record = SpannerToAvro.MakeRecord(schemaSet, s);
          // TODO(xjdr): Throw if empty optional
          publisher.publish(record.get(), metadata, timestamp);
          log.info("Published: " + record.get().toString() + " " + timestamp);

          return Boolean.TRUE;
        };

    tailer.start(
        handler,
        l.size(),
        2,
        500,
        "{project_id}",
        "test-db",
        "test",
        "test",
        "lpts_table",
        "2000",
        500,
        500);
  }
}
