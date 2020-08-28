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
