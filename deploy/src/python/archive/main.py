###
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
###

import base64
import os
import random

from google.cloud import storage
from opencensus.common.transports.async_ import AsyncTransport
from opencensus.ext.stackdriver.trace_exporter import StackdriverExporter
from opencensus.trace.samplers import AlwaysOnSampler
from opencensus.trace.span_context import SpanContext
from opencensus.trace.tracer import Tracer


def archive(event, context):
    version, trace_id, span_id, options = event["attributes"]["googclient_OpenCensusTraceContextKey"].split("-")
    tracer = setup_tracing(trace_id, span_id)
    client = storage.Client()
    bucket_name = os.environ.get("BUCKET")

    sink_instance = event["attributes"]["spez.sink.instance"]
    sink_database = event["attributes"]["spez.sink.database"]
    sink_table = event["attributes"]["spez.sink.table"]
    sink_uuid = event["attributes"]["spez.sink.uuid"]
    sink_commit_timestamp = event["attributes"]["spez.sink.commit_timestamp"]
    o_path = sink_instance + "/" + sink_database + "/" + sink_table +"/"
    o_name = sink_uuid + "_" + sink_commit_timestamp + "#" + random_appendix()
    o = o_path + o_name

    with tracer.span(name="archive") as span:
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(o)
        span.add_annotation("invoking archive")
        try:
            blob.upload_from_string(base64.b64decode(event['data']), content_type="avro/bytes")
        except:
            pass

def setup_tracing(trace_id, span_id):
    span_context = SpanContext(trace_id=trace_id, span_id=span_id)
    
    exporter = StackdriverExporter(
                    project_id=os.environ.get("GCP_PROJECT_ID"),
                    transport=AsyncTransport
                )

    tracer = Tracer(span_context=span_context, exporter=exporter, sampler=AlwaysOnSampler())
    return tracer

def random_appendix():
    letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    appendix = ""
    i = 0
    while i < 4:
        appendix = appendix + letters[random.randint(0, 51)]
        i += 1
    return appendix
