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
package com.google.spez.core;

import com.google.spannerclient.Row;

/** Interface to specific a call back method that is for each new event. */
public interface SpannerEventHandler {

  /**
   * Processes the event from the {@code SpannerTailer} in the form of a method callback.
   *
   * @param bucket Consistent Hash Table bucket id to aid in construction of method specific thread
   *     pooling strategies
   * @param event The Cloud Spanner {@code Struct} to create an immutable representation of the
   *     event.
   * @param timestamp the Cloud Spanner Commit Timestamp for the event
   */
  Boolean process(int bucket, Row event, String timestamp);
}
