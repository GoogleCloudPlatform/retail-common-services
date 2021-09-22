#!/usr/bin/env python3
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from google.cloud import pubsub_v1

from avro.datafile import DataFileReader
from avro.io import DatumReader
import io
import logging

from opencensus.common.transports.async_ import AsyncTransport
from opencensus.ext.stackdriver import trace_exporter as stackdriver_exporter
from opencensus.trace.tracer import Tracer
from opencensus.trace.samplers import AlwaysOnSampler
from opencensus.trace.span_context import SpanContext
from opencensus.trace.trace_options import TraceOptions


logging.basicConfig(level=logging.DEBUG)

project_id = 'spanner-event-exporter'

topic = 'spez-ledger-topic'

sub = 'listen'

TRACE_KEY = 'googclient_OpenCensusTraceContextKey'

subscriber = pubsub_v1.SubscriberClient()
topic_name = f'projects/{project_id}/topics/{topic}'
subscription_name = f'projects/{project_id}/subscriptions/{sub}'

try:
  existing = subscriber.get_subscription(subscription_name)
except:
  subscriber.create_subscription(name=subscription_name, topic=topic_name)

def get_context(attributes):
  encoded = attributes.get(TRACE_KEY)
  version, trace_id, span_id, options = encoded.split('-')
  trace_options = TraceOptions(trace_options_byte=options)
  context = SpanContext(trace_id=trace_id, span_id=span_id, trace_options=trace_options, from_header=True)


exporter = stackdriver_exporter.StackdriverExporter(project_id=project_id, transport=AsyncTransport)

def callback(message):
  trace_id = message.attributes.get(TRACE_KEY)
  context = get_context(message.attributes)
  tracer = Tracer(span_context=context, exporter=exporter, sampler=AlwaysOnSampler())
  with tracer.span(name='listener_ack') as span:
    payload = []
    try:
      reader = DataFileReader(io.BytesIO(message.data), DatumReader())
      payload = [msg for msg in reader]
      reader.close()
    except:
      logging.exception("got exception")
    logging.debug("message %s data %s payload %s", message, message.data, payload)
    message.ack()

future = subscriber.subscribe(subscription_name, callback)

future.result()
