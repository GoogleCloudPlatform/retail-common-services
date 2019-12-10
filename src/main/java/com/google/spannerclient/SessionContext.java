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

import com.google.spanner.v1.Session;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

class SessionContext {

  private final Session session;
  private final AtomicBoolean inUse;

  private Instant lastUsed;
  private boolean expired;

  public SessionContext(Session session) {
    this.session = session;
    this.inUse = new AtomicBoolean(false);
    this.expired = false;
  }

  void markExpired() {
    expired = true;
  }

  boolean isLocked() {
    return inUse.get();
  }

  Duration lastUsed() {
    Instant now = Instant.now();

    return Duration.between(lastUsed, now);
  }

  boolean lock() {
    if (expired == true) {
      return false;
    }

    if (inUse.compareAndSet(false, true)) {
      lastUsed = Instant.now();
      return true;
    }

    return false;
  }

  void unlock() {
    inUse.compareAndSet(true, false);
  }

  Session getSession() {
    return session;
  }

  String getName() {
    return session.getName();
  }
}
