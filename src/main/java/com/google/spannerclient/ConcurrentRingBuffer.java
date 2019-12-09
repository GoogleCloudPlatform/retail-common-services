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
package com.google.spannerclient;

import com.google.common.base.Preconditions;
import com.google.spanner.v1.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple, lock-free, non-blocking ring buffer.
 *
 * <p>'''Note:''' For very high-rate usages, sizing buffers by powers of two may be advantageous.
 *
 * <p>'''Caveats:''' References are kept to old entries until they are overwritten again. You can
 * get around this by storing nullable references (though this would require another allocation for
 * each item).
 */
class ConcurrentRingBuffer<T> {
  private final AtomicLong nextRead = new AtomicLong(0);
  private final AtomicLong nextWrite = new AtomicLong(0);
  private final AtomicLong publishedWrite = new AtomicLong(-1);
  private final int capacity;
  private final T[] ring;

  ConcurrentRingBuffer(int capacity) {
    Preconditions.checkArgument(capacity > 0);

    this.capacity = capacity;
    this.ring = (T[]) new Object[capacity];
  }

  void publish(long which) {
    while (publishedWrite.get() != which) {}
    boolean ok = publishedWrite.compareAndSet(which - 1, which);
    assert (ok);
  }

  /**
   * Try to get an item out of the ConcurrentRingBuffer. Returns no item only when the buffer is
   * empty.
   */
  Optional<T> tryGet() {
    final long w = publishedWrite.get();
    final long r = nextRead.get();

    if (w < r) {
      return Optional.empty();
    }

    final T el = ring[Math.toIntExact(r % capacity)];

    if (nextRead.compareAndSet(r, r + 1)) {
      Optional.of(el);
    } else {
      return tryGet();
    }

    return Optional.empty();
  }

  /**
   * Returns the next element without changing the read position.
   *
   * @return the next element or None if buffer is empty
   */
  Optional<T> tryPeek() {
    final long w = publishedWrite.get();
    final long r = nextRead.get();

    if (w < r) {
      return Optional.empty();
    } else {
      final T el = ring[Math.toIntExact(r % capacity)];
      return Optional.of(el);
    }
  }

  /** Attempt to put an item into the buffer. If it is full, the operation fails. */
  boolean tryPut(T el) {
    final long w = publishedWrite.get();
    final long r = nextRead.get();

    if (w - r >= capacity) {
      return false;
    }

    if (!nextWrite.compareAndSet(w, w + 1)) {
      tryPut(el);
    } else {
      ring[Math.toIntExact(w % capacity)] = el;
      publish(w);
      return true;
    }

    return false;
  }

  /**
   * Current size of the buffer.
   *
   * <p>The returned value is only quiescently consistent; treat it as a fast approximation.
   */
  int size() {
    return Math.toIntExact(nextWrite.get() - nextRead.get());
  }

  List<Session> getSessionList() {
    return new ArrayList<>();
  }
}
