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

import com.google.spanner.v1.Session;
import java.time.Duration;
import java.time.Instant;
import javax.annotation.Nullable;

/**
 * Context is an immutable object that carries per-request info.
 *
 * <p>RPC frameworks like gRPC have their own Context implementations that allow propagation of
 * deadlines, cancellation, and end-user credentials across RPC boundaries (between client and
 * server). Since these framework-specific Context implementations are not compatible with one
 * another, we provide our own Context class that wraps common features.
 *
 * <p>In gRPC and other frameworks, the current Context is maintained in thread-local storage, so
 * it's implicitly available to any method that needs it. In this Spanner client library, we pass
 * Context as an explicit parameter to methods that need it. This allows us to defer enforcement of
 * the specified request constraints until the request reaches the underlying framework-specific
 * Spanner client implementation, at which point the native Context class can be used.
 */
public class Context {

  private static final Context DEFAULT_CONTEXT = new Context();
  private Instant deadline;
  private Session session;

  private Context() {}

  private Context(Instant deadline, Session session) {
    this.deadline = deadline;
    this.session = session;
  }

  // getDefault returns an empty context.
  public static Context getDefault() {
    return DEFAULT_CONTEXT;
  }

  // withDeadline returns a derived context with the specified maximum deadline.
  public Context withDeadline(Instant deadline) {
    if (this.deadline != null && this.deadline.isBefore(deadline)) {
      // You can't make a derived context with a later deadline than the parent.
      return this;
    }
    return new Context(deadline, session);
  }

  /**
   * withDeadlineAfter returns a derived context with a maximum deadline specified relative to the
   * current time.
   */
  public Context withDeadlineAfter(Duration duration) {
    return withDeadline(Instant.now().plus(duration));
  }

  // withCallerId returns a derived context with the specified callerId.
  public Context withCallerId(Session session) {
    if (this.session != null && this.session.equals(session)) {
      // Nothing changed.
      return this;
    }
    return new Context(deadline, session);
  }

  @Nullable
  public Instant getDeadline() {
    return deadline;
  }

  @Nullable
  public Duration getTimeout() {
    if (deadline == null) {
      return null;
    }
    return Duration.ofSeconds(deadline.getEpochSecond());
  }

  @Nullable
  public String getSession() {
    return session.getName();
  }

  public void setSession(Session session_) {
    this.session = session_;
  }
}
