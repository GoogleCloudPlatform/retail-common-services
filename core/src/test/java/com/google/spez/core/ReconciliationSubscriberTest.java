package com.google.spez.core;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.google.spannerclient.Publisher;
import com.google.spez.common.UsefulExecutors;
import io.opencensus.common.Scope;
import io.opencensus.trace.Span;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
@ExtendWith(MockitoExtension.class)
public class ReconciliationSubscriberTest implements WithAssertions {
  @Mock private Publisher publisher;
  private String lastProcessedTimestamp = "2021-02-18T16:31:47.675987Z";

  @Test
  public void shouldBufferMessage() {

    // Mockito.when(publisher.getTopicPath()).thenReturn("");
    // buffer size 100 will not publish
    /*
    EventPublisher eventPublisher = new EventPublisher(scheduler, publisher, 100, 100);

    var event = ByteString.copyFromUtf8(EVENT);
    var attributes = Map.of(SpezConfig.SINK_UUID_KEY, UUID);
    var future = eventPublisher.publish(event, attributes, parent);
    assertThat(eventPublisher.bufferSize).hasValue(1);
    assertThat(future).isNotNull();
     */
  }
}


