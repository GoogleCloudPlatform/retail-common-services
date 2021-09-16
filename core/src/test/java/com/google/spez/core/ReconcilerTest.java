package com.google.spez.core;

import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("PMD.BeanMembersShouldSerialize")
@ExtendWith(MockitoExtension.class)
class ReconcilerTest implements WithAssertions {

  @Test
  void publishedAndNotReceived() {
    var reconciler = new Reconciler();
    reconciler.published("messageId", "commitTimestamp", "uuid");
    var expectedKey = new Reconciler.Key("messageId", "commitTimestamp", "uuid");
    var bookkeeping = reconciler.getBookkeeping();
    assertThat(bookkeeping).hasSize(1);
    assertThat(bookkeeping).containsKey(expectedKey);
    var record = bookkeeping.get(expectedKey);
    assertThat(record.published).isEqualTo(1);
    assertThat(record.received).isEqualTo(0);
    assertThat(record.completed()).isFalse();
    assertThat(reconciler.getLastProcessedTimestamp()).isEqualTo(Reconciler.EPOCH);
  }

  @Test
  void receivedAndNotPublished() {
    var reconciler = new Reconciler();
    reconciler.received("messageId", "commitTimestamp", "uuid");
    var expectedKey = new Reconciler.Key("messageId", "commitTimestamp", "uuid");
    var bookkeeping = reconciler.getBookkeeping();
    assertThat(bookkeeping).hasSize(1);
    assertThat(bookkeeping).containsKey(expectedKey);
    var record = bookkeeping.get(expectedKey);
    assertThat(record.published).isEqualTo(0);
    assertThat(record.received).isEqualTo(1);
    assertThat(record.completed()).isFalse();
    assertThat(reconciler.getLastProcessedTimestamp()).isEqualTo(Reconciler.EPOCH);
  }

  @Test
  void publishedAndReceived() {
    var reconciler = new Reconciler();
    var timestamp = "2020-01-01T00:00:00.000000Z";
    reconciler.published("messageId", timestamp, "uuid");
    var expectedKey = new Reconciler.Key("messageId", timestamp, "uuid");
    var bookkeeping = reconciler.getBookkeeping();
    assertThat(bookkeeping).hasSize(1);
    assertThat(bookkeeping).containsKey(expectedKey);
    var record = bookkeeping.get(expectedKey);
    assertThat(record.published).isEqualTo(1);
    assertThat(record.received).isEqualTo(0);
    assertThat(record.completed()).isFalse();
    assertThat(reconciler.getLastProcessedTimestamp()).isEqualTo(Reconciler.EPOCH);
    reconciler.received("messageId", timestamp, "uuid");
    assertThat(bookkeeping).hasSize(0);
    assertThat(record.published).isEqualTo(1);
    assertThat(record.received).isEqualTo(1);
    assertThat(record.completed()).isTrue();
    assertThat(reconciler.getLastProcessedTimestamp()).isEqualTo(timestamp);
  }

  @Test
  void receivedAndPublished() {
    var reconciler = new Reconciler();
    var timestamp = "2020-01-01T00:00:00.000000Z";
    reconciler.received("messageId", timestamp, "uuid");
    var expectedKey = new Reconciler.Key("messageId", timestamp, "uuid");
    var bookkeeping = reconciler.getBookkeeping();
    assertThat(bookkeeping).hasSize(1);
    assertThat(bookkeeping).containsKey(expectedKey);
    var record = bookkeeping.get(expectedKey);
    assertThat(record.published).isEqualTo(0);
    assertThat(record.received).isEqualTo(1);
    assertThat(record.completed()).isFalse();
    assertThat(reconciler.getLastProcessedTimestamp()).isEqualTo(Reconciler.EPOCH);
    reconciler.published("messageId", timestamp, "uuid");
    assertThat(bookkeeping).hasSize(0);
    assertThat(record.published).isEqualTo(1);
    assertThat(record.received).isEqualTo(1);
    assertThat(record.completed()).isTrue();
    assertThat(reconciler.getLastProcessedTimestamp()).isEqualTo(timestamp);
  }

  @Test
  void publishedTooLate() {
    var reconciler = new Reconciler();
    var timestamp = "2020-01-01T00:00:00.000000Z";
    reconciler.published("messageId", timestamp, "uuid");
    var expectedKey = new Reconciler.Key("messageId", timestamp, "uuid");
    var bookkeeping = reconciler.getBookkeeping();
    assertThat(bookkeeping).hasSize(1);
    assertThat(bookkeeping).containsKey(expectedKey);
    var record = bookkeeping.get(expectedKey);
    assertThat(record.published).isEqualTo(1);
    assertThat(record.received).isEqualTo(0);
    assertThat(record.completed()).isFalse();
    assertThat(reconciler.getLastProcessedTimestamp()).isEqualTo(Reconciler.EPOCH);
    reconciler.received("messageId", timestamp, "uuid");
    assertThat(bookkeeping).hasSize(0);
    assertThat(record.published).isEqualTo(1);
    assertThat(record.received).isEqualTo(1);
    assertThat(record.completed()).isTrue();
    assertThat(reconciler.getLastProcessedTimestamp()).isEqualTo(timestamp);
    reconciler.published("messageId", Reconciler.EPOCH, "uuid");
    assertThat(bookkeeping).hasSize(0);
  }
}
