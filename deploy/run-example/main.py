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


from avro.datafile import DataFileReader
from avro.io import DatumReader
from google.cloud import pubsub_v1
from google.cloud import spanner
from google.cloud.spanner_v1 import COMMIT_TIMESTAMP
import io
import logging
import sys

# Instantiate a client.
spanner_client = spanner.Client()

# Your Cloud Spanner instance ID.
sink_instance_id = 'example-event-sink-instance'

# Get a Cloud Spanner instance by ID.
sink_instance = spanner_client.instance(sink_instance_id)

# Your Cloud Spanner database ID.
sink_database_id = 'example-event-sink-database'

sink_table_name = 'example'
uuid_column = 'uuid'

ledger_topic = 'spez-ledger-topic'

# Get a Cloud Spanner database by ID.
sink_database = sink_instance.database(sink_database_id)

def insert_row(transaction, uuid):
  values=[uuid, COMMIT_TIMESTAMP]
  transaction.insert(
    table_name,
    columns=[uuid_column, 'CommitTimestamp'],
    values=[values],
  )


def reset_lpts(sink_instance, sink_database, sink_table):
  def do_reset(transaction, sink_instance, sink_database, sink_table):
    transaction.insert_or_update(
      'lpts',
      columns=['CommitTimestamp', 'LastProcessedTimestamp', 'instance', 'database', 'table'],
      values=[
          [COMMIT_TIMESTAMP, '1970-01-01T00:00:00.000000Z', sink_instance, sink_database, sink_table],
      ],
    )
  # Cloud Spanner lpts instance name
  instance_id = 'spez-lpts-instance'
  # Cloud Spanner lpts database name
  database_id = 'spez-lpts-database'

  spanner_client = spanner.Client()
  instance = spanner_client.instance(instance_id)
  database = instance.database(database_id)
  database.run_in_transaction(do_reset, sink_instance, sink_database, sink_table)


def setup_subscriber(project_id):
  ledger_topic = 'spez-ledger-topic'
  ledger_subscription = 'listen'

  subscriber = pubsub_v1.SubscriberClient()
  topic_name = f'projects/{project_id}/topics/{ledger_topic}'
  subscription_name = f'projects/{project_id}/subscriptions/{ledger_subscription}'
  try:
    existing = subscriber.get_subscription(subscription_name)
  except:
    subscriber.create_subscription(name=subscription_name, topic=topic_name)
  return subscription_name, subscriber


def callback(message):
  payload = []
  try:
    reader = DataFileReader(io.BytesIO(message.data), DatumReader())
    payload = [msg for msg in reader]
    reader.close()
  except:
    logging.exception("got exception")
  logging.debug("message %s data %s payload %s", message, message.data, payload)
  message.ack()


def wait_for_message(project_id):
  subscription_name, subscriber = setup_subscriber(project_id)
  future = subscriber.subscribe(subscription_name, callback)
  future.result()


def main(project_id):
  logging.basicConfig(level=logging.DEBUG)
  logging.debug('Reseting LPTS timestamp for %s/%s/%s', sink_instance_id, sink_database_id, sink_table_name)
  reset_lpts(sink_instance_id, sink_database_id, sink_table_name)
  uuid = '000000001'
  logging.debug('Storing single event with uuid %s', uuid)
  sink_database.run_in_transaction(insert_row, uuid)
  logging.debug('Waiting for event to appear on pubsub')
  wait_for_message(project_id)


if __name__ == '__main__':
  main(sys.argv[1])
