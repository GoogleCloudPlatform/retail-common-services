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

import com.google.protobuf.ByteString;
import com.google.spez.core.internal.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventState {
  private static final Logger log = LoggerFactory.getLogger(EventState.class);

  public enum WorkStage {
    RowRead,
    QueuedForConversion,
    ConvertedToMessage,
    QueuedForPublishing,
    MessagePublishRequested,
    MessagePublished
  }

  private WorkStage stage;
  final Row row;
  // TODO(pdex): add reference to polling span
  ByteString message;
  String publishId;

  public EventState(Row row) {
    stage = WorkStage.RowRead;
    this.row = row;
  }

  public void queued() {
    stage = WorkStage.QueuedForConversion;
  }

  public void convertedToMessage(ByteString message) {
    stage = WorkStage.ConvertedToMessage;
    this.message = message;
  }

  public void queuedForPublishing() {
    stage = WorkStage.QueuedForPublishing;
  }

  public void messagePublishRequested() {
    stage = WorkStage.MessagePublishRequested;
  }

  public void mesesagePublished(String publishId) {
    stage = WorkStage.MessagePublished;
    this.publishId = publishId;
  }
}
