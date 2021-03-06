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


from google.cloud import spanner
from google.cloud.spanner_v1 import COMMIT_TIMESTAMP
import sys

# Instantiate a client.
spanner_client = spanner.Client()

# Your Cloud Spanner instance ID.
instance_id = 'example-event-sink-instance'

# Get a Cloud Spanner instance by ID.
instance = spanner_client.instance(instance_id)

# Your Cloud Spanner database ID.
database_id = 'example-event-sink-database'

table_name = 'example'
uuid_column = 'uuid'

# Get a Cloud Spanner database by ID.
database = instance.database(database_id)

def insert_row(transaction, uuid):
  values=[uuid, COMMIT_TIMESTAMP]
  transaction.insert(
    table_name,
    columns=[uuid_column, 'CommitTimestamp'],
    values=[values],
  )

def insert_rows(offset, num=10000):
  for i in range(1, num+1):
    print(f"{i}/{num}", end="\r")
    uuid = offset + i
    database.run_in_transaction(insert_row, uuid)
  print(f"\ninserted rows with uuid {offset}..{offset+num}")

offset = int(sys.argv[1])

if offset == -1:
  with database.snapshot() as snapshot:
    result = snapshot.execute_sql(f"select max({uuid_column}) as max_id from {table_name}").one()
    offset = result[0]
    print("new offset: ", offset)

insert_rows(offset)
