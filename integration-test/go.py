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
instance_id = 'spez-test-instance'

# Get a Cloud Spanner instance by ID.
instance = spanner_client.instance(instance_id)

# Your Cloud Spanner database ID.
database_id = 'spez-test-database'

# Get a Cloud Spanner database by ID.
database = instance.database(database_id)

def insert_rows(transaction, offset, num=100):
  values=[]
  for i in range(1, num+1):
    values.append([offset + i, COMMIT_TIMESTAMP])
  transaction.insert(
    'test',
    columns=['Id', 'CommitTimestamp'],
    values=values,
  )

offset = int(sys.argv[1])

if offset == -1:
  with database.snapshot() as snapshot:
    result = snapshot.execute_sql("select max(Id) as max_id from test").one()
    offset = result[0]
    print("new offset: ", offset)

database.run_in_transaction(insert_rows, offset)
