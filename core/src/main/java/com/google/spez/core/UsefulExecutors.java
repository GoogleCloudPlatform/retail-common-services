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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

// TODO(pdex): move this class into wingwalker-common
public class UsefulExecutors {

  public static ListeningScheduledExecutorService listeningScheduler(int numThreads) {
    Preconditions.checkArgument(numThreads >= 1, "numThreads must be greater than or equal to 1");
    return MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(numThreads));
  }

  public static ListeningScheduledExecutorService listeningScheduler() {
    return listeningScheduler(1);
  }

  public static void submit(
      ListeningExecutorService executor, Runnable runnable, Consumer<Throwable> callback) {
    ListenableFutureErrorHandler.create(executor, executor.submit(runnable), callback);
  }
}
