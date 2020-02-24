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
package com.google.spannerclient;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.spanner.v1.Session;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

class SessionPool {
  private final ScheduledExecutorService scheduler;
  private final int maxPoolSize;
  private final ConcurrentLinkedDeque<Session> pool;
  private final Instant lastChecked;
  private final SessionStaleHandler staleHandler;
  private final NewSessionHandler newSessionHandler;

  SessionPool(
      int maxPoolSize, SessionStaleHandler staleHandler, NewSessionHandler newSessionHandler) {
    Preconditions.checkArgument(maxPoolSize > 1);
    Preconditions.checkNotNull(staleHandler);
    Preconditions.checkNotNull(newSessionHandler);

    this.maxPoolSize = maxPoolSize;
    this.staleHandler = staleHandler;
    this.newSessionHandler = newSessionHandler;
    this.pool = new ConcurrentLinkedDeque<>();
    this.lastChecked = Instant.now();
    this.scheduler = Executors.newScheduledThreadPool(2);

    staleCheck();
  }

  void staleCheck() {
    scheduler.scheduleAtFixedRate(
        () -> {
          final Instant now = Instant.now();
          final Duration d = Duration.between(lastChecked, now);

          if (d.toMinutes() >= 30) {
            pool.forEach(
                s -> {
                  //               if (staleHandler.checkSession(s)) {
                  // pool.addLast(newSessionHandler.newSession());
                  // }
                });
          }
        },
        30,
        30,
        TimeUnit.MINUTES);
  }

  int getMaxPoolSize() {
    return maxPoolSize;
  }

  void addSession(Session session) {
    pool.addLast(session);
  }

  public Optional<Session> getSession() {
    if (pool.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(pool.peekFirst());
    }
  }

  ImmutableList<Session> getSessionList() {
    final List<Session> sl = new ArrayList<>();

    if (!pool.isEmpty()) {
      pool.iterator()
          .forEachRemaining(
              new Consumer() {
                @Override
                public void accept(Object o) {
                  sl.add((Session) o);
                }
              });
    }

    return ImmutableList.copyOf(sl);
  }

  void removeSession(Session s) {
    Preconditions.checkNotNull(s);

    // pool.remove(s);
  }
}
