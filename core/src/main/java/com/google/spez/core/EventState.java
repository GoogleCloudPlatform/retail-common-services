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

import io.opencensus.trace.BlankSpan;
import com.google.protobuf.ByteString;
import com.google.common.collect.Maps;
import com.google.spez.core.internal.Row;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

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

  private Span createChildSpan(String name) {
    return tracer.spanBuilderWithExplicitParent(name, pollingSpan).startSpan();
  }

  private void transitionStage(WorkStage newStage) {
    childSpans.getOrDefault(stage, BlankSpan.INSTANCE).end();
    stage = newStage;
    childSpans.put(stage, createChildSpan(stage.toString()));
  }

  private WorkStage stage;
  final Row row;
  final Span pollingSpan;
  Map<WorkStage, Span> childSpans = Maps.newEnumMap(WorkStage.class);
  ByteString message;
  String publishId;

  public EventState(Row row, Span pollingSpan) {
    stage = WorkStage.RowRead;
    this.row = row;
    this.pollingSpan = pollingSpan;
  }

  public void queued() {
    stage = WorkStage.QueuedForConversion;
    childSpans.put(stage, createChildSpan(stage.toString()));
  }

  public void convertedToMessage(ByteString message) {
    transitionStage(WorkStage.ConvertedToMessage);
    this.message = message;
  }

  public void queuedForPublishing() {
    transitionStage(WorkStage.QueuedForPublishing);
  }

  public void messagePublishRequested() {
    transitionStage(WorkStage.MessagePublishRequested);
  }

  public void messagePublished(String publishId) {
    childSpans.getOrDefault(stage, BlankSpan.INSTANCE).end();
    stage = WorkStage.MessagePublished;
    this.publishId = publishId;
  }
}
