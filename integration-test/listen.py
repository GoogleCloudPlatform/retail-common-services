#!/usr/bin/env python3

from google.cloud import pubsub_v1

from avro.datafile import DataFileReader
from avro.io import DatumReader
import io

from opencensus.common.transports.async_ import AsyncTransport
from opencensus.ext.stackdriver import trace_exporter as stackdriver_exporter
from opencensus.trace.tracer import Tracer
from opencensus.trace.samplers import AlwaysOnSampler
from opencensus.trace.span_context import SpanContext


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


exporter = stackdriver_exporter.StackdriverExporter(project_id=project_id, transport=AsyncTransport)

def callback(message):
  print("message", message)
  print("attributes", message.attributes)
  print("message_id", message.message_id)
  print("publish_time", message.publish_time)
  print("ordering_key", message.ordering_key)
  trace_id = message.attributes.get(TRACE_KEY)
  context = SpanContext(trace_id=trace_id, from_header=True)
  tracer = Tracer(exporter=exporter, sampler=AlwaysOnSampler())
  '''
  reader = DataFileReader(io.BytesIO(message.data), DatumReader())
  for msg in reader:
    print(msg)
  reader.close()
  '''
  message.ack()

future = subscriber.subscribe(subscription_name, callback)

future.result()
