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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

class SessionContext {

  private final Session session;
  private final AtomicBoolean inUse;

  public SessionContext(Session session) {
    this.session = session;
    this.inUse = new AtomicBoolean(false);
  }

  boolean isLocked() {
    return inUse.get();
  }

  boolean lock() {
    return inUse.compareAndSet(false, true);
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

  List<Session> getSessionList() {
    return new ArrayList<>();
  }
}
