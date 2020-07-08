#!/usr/bin/env python3

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

