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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Spez {
  private static final Logger log = LoggerFactory.getLogger(Spez.class);

  private Spez() {}

  public static List<ListeningExecutorService> ServicePoolGenerator(int poolSize, String name) {
    final List<ListeningExecutorService> l = new ArrayList<>();
    IntStream.range(0, poolSize)
        .forEach(
            i -> {
              l.add(
                  MoreExecutors.listeningDecorator(
                      Executors.newSingleThreadExecutor(
                          new ThreadFactoryBuilder().setNameFormat(name).build())));
            });

    return l;
  }
}
