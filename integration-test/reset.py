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

# Instantiate a client.
spanner_client = spanner.Client()

# Your Cloud Spanner instance ID.
instance_id = 'spez-lpts-instance'

# Get a Cloud Spanner instance by ID.
instance = spanner_client.instance(instance_id)

# Your Cloud Spanner database ID.
database_id = 'spez-lpts-database'

# Get a Cloud Spanner database by ID.
database = instance.database(database_id)

def reset_timestamp(transaction, timestamp):
  transaction.insert_or_update(
    'lpts',
    columns=['CommitTimestamp', 'LastProcessedTimestamp', 'instance', 'database', 'table'],
    values=[
        [COMMIT_TIMESTAMP, '1970-01-01T00:00:00.000000Z', 'spez-test-instance', 'spez-test-database', 'test'],
    ],
  )


offset = 10

database.run_in_transaction(reset_timestamp, offset)

