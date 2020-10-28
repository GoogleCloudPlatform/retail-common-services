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

package com.google.spez.common;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

// TODO(pdex): move this class into wingwalker-common
public class ListenableFutureErrorHandler {

  private final Executor executor;
  private final ListenableFuture future;
  private final Consumer<Throwable> callback;

  private ListenableFutureErrorHandler(
      Executor executor, ListenableFuture future, Consumer<Throwable> callback) {
    this.executor = executor;
    this.future = future;
    this.callback = callback;
  }

  public static void create(
      Executor executor, ListenableFuture future, Consumer<Throwable> callback) {
    Preconditions.checkNotNull(executor, "executor must not be null");
    Preconditions.checkNotNull(future, "future must not be null");
    Preconditions.checkNotNull(callback, "callback must not be null");

    new ListenableFutureErrorHandler(executor, future, callback).setup();
  }

  private void setup() {
    Futures.addCallback(
        future,
        new FutureCallback() {
          @Override
          public void onFailure(Throwable t) {
            callback.accept(t);
          }

          @Override
          public void onSuccess(Object result) {}
        },
        executor);
  }
}
