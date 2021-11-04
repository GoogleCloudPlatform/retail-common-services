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
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
public class EventState {
  private static final Logger log = LoggerFactory.getLogger(EventState.class);

  public enum WorkStage {
    Unknown,
    RowRead,
    QueuedForConversion,
    ConvertedToMessage,
    QueuedForPublishing,
    MessagePublishRequested,
    MessagePublished
  }

  private void initialStage(WorkStage newStage) {
    stage = newStage;
    nanosNow = System.nanoTime();
  }

  /*
  private Span currentSpan() {
    return childSpans.getOrDefault(stage, BlankSpan.INSTANCE);
  }
  */

  private void transitionStage(WorkStage newStage) {
    var nanosThen = nanosNow;
    nanosNow = System.nanoTime();

    stage = newStage;
    long duration = nanosNow - nanosThen;
    switch (stage) {
      case RowRead:
        statsCollector.addRowReadDuration(duration);
        break;
      case QueuedForConversion:
        statsCollector.addQueuedForConversionDuration(duration);
        break;
      case ConvertedToMessage:
        statsCollector.addConvertedToMessageDuration(duration);
        break;
      case QueuedForPublishing:
        statsCollector.addQueuedForPublishingDuration(duration);
        break;
      case MessagePublishRequested:
        statsCollector.addMessagePublishedDuration(duration);
        break;
      case MessagePublished:
        statsCollector.addMessagePublishedDuration(duration);
        break;
      case Unknown:
      default:
        break;
    }
  }

  private WorkStage stage;
  private long nanosNow;
  private final Span pollingSpan;
  private final StatsCollector statsCollector;
  final Map<String, AttributeValue> attributes = new HashMap<>();
  private Row row;
  ByteString message;
  private String uuid;

  public EventState(Span pollingSpan, StatsCollector statsCollector) {
    this.stage = WorkStage.Unknown;
    this.pollingSpan = pollingSpan;
    this.statsCollector = statsCollector;
  }

  public Row getRow() {
    return row;
  }

  public Span getPollingSpan() {
    return pollingSpan;
  }

  public void uuid(String uuid) {
    this.uuid = uuid;
    attributes.put("uuid", AttributeValue.stringAttributeValue(uuid));
    statsCollector.attachUuid(uuid);
  }

  // state transitions
  public void rowRead(Row row) {
    this.row = row;
    initialStage(WorkStage.RowRead);
    statsCollector.addRowSize(row.getSize());
  }

  public void uuid(String uuid) {
    eventSpan.putAttribute("uuid", AttributeValue.stringAttributeValue(uuid));
  }

  // state transitions
  public void queued() {
    transitionStage(WorkStage.QueuedForConversion);
  }

  public void convertedToMessage(ByteString message) {
    transitionStage(WorkStage.ConvertedToMessage);
    this.message = message;
    statsCollector.addMessageSize(message.size());
  }

  public void queuedForPublishing(long bufferSize) {
    transitionStage(WorkStage.QueuedForPublishing);
    statsCollector.addBufferSizeWhenQueued(bufferSize);
    statsCollector.incrementReceived();
  }

  public void messagePublishRequested() {
    transitionStage(WorkStage.MessagePublishRequested);
  }

  public void messagePublished(String publishId) {
    transitionStage(WorkStage.MessagePublished);
    attributes.put("publishId", AttributeValue.stringAttributeValue(publishId));
    pollingSpan.addAnnotation("Event " + uuid + " published", attributes);
    statsCollector.attachPublishId(publishId);
    statsCollector.incrementPublished();
    statsCollector.collect();
  }

  public void error(Throwable throwable) {
    log.error("Aborted processing event {} due to error", uuid, throwable);
    statsCollector.collect();
  }
}
