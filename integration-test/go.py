#!/usr/bin/env python3

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

def insert_rows(transaction, offset):
  transaction.insert(
    'test',
    columns=['Id', 'CommitTimestamp'],
    values=[
        [offset + 1 , COMMIT_TIMESTAMP],
        [offset + 2 , COMMIT_TIMESTAMP],
        [offset + 3 , COMMIT_TIMESTAMP],
        [offset + 4 , COMMIT_TIMESTAMP],
        [offset + 5 , COMMIT_TIMESTAMP],
        [offset + 6 , COMMIT_TIMESTAMP],
        [offset + 7 , COMMIT_TIMESTAMP],
        [offset + 8 , COMMIT_TIMESTAMP],
        [offset + 9 , COMMIT_TIMESTAMP],
        [offset + 10, COMMIT_TIMESTAMP],
    ],
  )

offset = int(sys.argv[1])

database.run_in_transaction(insert_rows, offset)
