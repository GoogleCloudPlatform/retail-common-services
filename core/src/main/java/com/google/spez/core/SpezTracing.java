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

import io.opencensus.common.Scope;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

public class SpezTracing {
  private static final Tracer tracer = Tracing.getTracer();

  @SuppressWarnings("MustBeClosedChecker")
  public Scope processRowScope() {
    return tracer.spanBuilder("SpannerTailer.processRow").startScopedSpan();
  }

  public Span currentSpan() {
    return tracer.getCurrentSpan();
  }
}
