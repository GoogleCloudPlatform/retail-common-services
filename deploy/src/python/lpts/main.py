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

import os

from google.cloud import spanner

def lpts(event, context):
    project = os.environ.get("PROJECT")
    lpts_instance = os.environ.get("INSTANCE")
    lpts_database = os.environ.get("DATABASE")
    lpts_table = os.environ.get("TABLE")
    lpts_id = "projects/" + project + "/instances/" + lpts_instance + "/databases/" + lpts_database

    sink_instance = event["attributes"]["spez.sink.instance"]
    sink_database = event["attributes"]["spez.sink.database"]
    sink_table = event["attributes"]["spez.sink.table"]
    sink_commit_timestamp = event["attributes"]["spez.sink.commit_timestamp"]

    client = spanner.Client()
    instance = client.instance(lpts_instance)
    database = instance.database(lpts_database)

    with database.batch() as batch:
        batch.insert(
            table = lpts_table,
            columns = ('instance', 'database', 'table', 'LastProcessedTimestamp', 'CommitTimestamp'),
            values = [(sink_instance, sink_database, sink_table, sink_commit_timestamp, spanner.COMMIT_TIMESTAMP)]
        )
