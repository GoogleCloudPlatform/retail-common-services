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
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class EventState {
  private static final Tracer tracer = Tracing.getTracer();
  private static final Logger log = LoggerFactory.getLogger(EventState.class);

  public enum WorkStage {
    RowRead,
    QueuedForConversion,
    ConvertedToMessage,
    QueuedForPublishing,
    MessagePublishRequested,
    MessagePublished
  }

  private static Span createChildSpan(String name, Span parentSpan) {
    Span span = tracer.spanBuilderWithExplicitParent(name, parentSpan).startSpan();
    span.putAttribute("name", AttributeValue.stringAttributeValue(name));
    return span;
  }

  private void transitionStage(WorkStage newStage) {
    stage = newStage;
    eventSpan.addAnnotation(stage.toString());
  }

  private WorkStage stage;
  final String tableName;
  final Span eventSpan;
  Row row;
  ByteString message;
  String publishId;

  public EventState(Span pollingSpan, String tableName) {
    eventSpan = createChildSpan("Spez Event", pollingSpan);
    this.tableName = tableName;
  }

  public void uuid(String uuid) {
    eventSpan.putAttribute("uuid", AttributeValue.stringAttributeValue(uuid));
  }

  // state transitions
  public void rowRead(Row row) {
    this.row = row;
    transitionStage(WorkStage.RowRead);
    eventSpan.putAttribute("rowSize", AttributeValue.longAttributeValue(row.getSize()));
  }

  public void queued() {
    transitionStage(WorkStage.QueuedForConversion);
  }

  public void convertedToMessage(ByteString message) {
    transitionStage(WorkStage.ConvertedToMessage);
    this.message = message;
    eventSpan.putAttribute("messageSize", AttributeValue.longAttributeValue(message.size()));
  }

  public void queuedForPublishing(long bufferSize) {
    transitionStage(WorkStage.QueuedForPublishing);
    eventSpan.putAttribute("bufferSizeWhenQueued", AttributeValue.longAttributeValue(bufferSize));
  }

  public void messagePublishRequested() {
    transitionStage(WorkStage.MessagePublishRequested);
  }

  public void messagePublished(String publishId) {
    transitionStage(WorkStage.MessagePublished);
    eventSpan.putAttribute("publishId", AttributeValue.stringAttributeValue(publishId));
    eventSpan.end();
    this.publishId = publishId;
  }
}
