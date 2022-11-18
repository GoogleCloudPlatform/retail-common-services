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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.testing.TestingExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.google.spannerclient.Publisher;
import com.google.spez.common.UsefulExecutors;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
@ExtendWith(MockitoExtension.class)
public class EventPublisherTest implements WithAssertions {

  @Mock private ListeningScheduledExecutorService scheduler;

  @Mock private Publisher publisher;

  static String EVENT = "event";
  static String UUID = "uuid-value";

  @Test
  public void shouldBufferMessage(@Mock StatsCollector statsCollector) {
    var eventState = new EventState(null, statsCollector);
    // Mockito.when(publisher.getTopicPath()).thenReturn("");
    // buffer size 100 will not publish
    EventPublisher eventPublisher = new EventPublisher("", scheduler, publisher, 100, 100);

    var event = ByteString.copyFromUtf8(EVENT);
    var attributes = Map.of(SpezConfig.SINK_UUID_KEY, UUID);
    var future = eventPublisher.publish(event, attributes, eventState);
    assertThat(eventPublisher.bufferSize).hasValue(1);
    assertThat(future).isNotNull();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldStartScheduler(@Mock ListenableScheduledFuture future) {
    EventPublisher eventPublisher = new EventPublisher("", scheduler, publisher, 100, 100);
    Mockito.when(
            scheduler.scheduleAtFixedRate(
                eventPublisher.runPublishBuffer, 0, 100, TimeUnit.MILLISECONDS))
        .thenReturn(future);

    eventPublisher.start();

    Mockito.verify(scheduler, Mockito.times(1))
        .scheduleAtFixedRate(eventPublisher.runPublishBuffer, 0, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void scheduledTaskErrorsAreLogged() {
    var scheduler = UsefulExecutors.listeningScheduler();
    EventPublisher eventPublisher = // NOPMD
        new EventPublisher(
            "",
            scheduler,
            publisher,
            100,
            1,
            () -> {
              throw new RuntimeException("TEST: scheduled task failure");
            });

    eventPublisher.start();
  }

  @Test
  public void createCallsStart(
      @Mock(answer = Answers.RETURNS_DEEP_STUBS) SpezConfig config, @Mock GoogleCredentials creds) {
    Mockito.when(config.getAuth().getCredentials()).thenReturn(creds);
    Mockito.when(config.getPubSub().getProjectId()).thenReturn("project-id");
    Mockito.when(config.getPubSub().getTopic()).thenReturn("topic-id");
    try (MockedConstruction cons = Mockito.mockConstruction(EventPublisher.class)) {
      EventPublisher eventPublisher = EventPublisher.create(config);
      Mockito.verify(eventPublisher, Mockito.times(1)).start();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void publishBufferUpdatesBufferSize(
      @Mock StatsCollector statsCollector,
      @Mock ListenableFuture submitFuture,
      @Mock ListenableFuture<PublishResponse> publishFuture) {
    var eventState = new EventState(null, statsCollector);
    Mockito.when(publisher.getTopicPath()).thenReturn("");
    // buffer size 1 will publish
    var scheduler = TestingExecutors.noOpScheduledExecutor();
    EventPublisher eventPublisher = new EventPublisher("", scheduler, publisher, 1, 100);

    Mockito.when(publisher.publish(Mockito.any(), Mockito.any())).thenReturn(publishFuture);
    var event = ByteString.copyFromUtf8(EVENT);
    var attributes = Map.of(SpezConfig.SINK_UUID_KEY, UUID);
    var future = eventPublisher.publish(event, attributes, eventState);
    assertThat(eventPublisher.bufferSize).hasValue(1);
    assertThat(future).isNotNull();

    eventPublisher.publishBuffer();
    assertThat(eventPublisher.bufferSize).hasValue(0);
  }

  @Test
  void callbackShouldAttachMessageId(@Mock PublishResponse response)
      throws ExecutionException, InterruptedException {
    var eventState = new EventState(null, null);
    var event = ByteString.copyFromUtf8(EVENT);
    var message =
        PubsubMessage.newBuilder()
            .setData(event)
            .setOrderingKey("")
            .putAttributes(SpezConfig.SINK_UUID_KEY, UUID)
            .build();
    SettableFuture<String> future = SettableFuture.create();
    var sink = List.of(new EventPublisher.BufferPayload(message, future, eventState));
    var callback = new EventPublisher.PublishCallback(sink, "", null);

    Mockito.when(response.getMessageIdsCount()).thenReturn(sink.size());
    Mockito.when(response.getMessageIds(0)).thenReturn("published-message-id");
    callback.onSuccess(response);

    assertThat(future).isDone();
    assertThat(future).isNotCancelled();
    assertThat(future.get()).isEqualTo("published-message-id");
  }
}
