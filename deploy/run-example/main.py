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
from google.api_core import retry
from google.cloud import pubsub_v1
from google.cloud import spanner
from google.cloud.spanner_v1 import COMMIT_TIMESTAMP
import io
import logging
import sys
import uuid

# Instantiate a client.
spanner_client = spanner.Client()

# Your Cloud Spanner instance ID.
sink_instance_id = 'event-sink-instance'

# Get a Cloud Spanner instance by ID.
sink_instance = spanner_client.instance(sink_instance_id)

# Your Cloud Spanner database ID.
sink_database_id = 'event-sink-database'

# You Table info
sink_table_name = 'example'
uuid_column = 'uuid'
uuid_value = uuid.uuid4().hex

ledger_topic = 'spez-ledger-topic'

# Get a Cloud Spanner database by ID.
sink_database = sink_instance.database(sink_database_id)

def insert_row(transaction, uuid):
  values=[uuid, COMMIT_TIMESTAMP]
  transaction.insert(
    sink_table_name,
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
  subscription_path = subscriber.subscription_path(project_id, ledger_subscription)
  try:
    existing = subscriber.get_subscription(subscription=subscription_path)
  except:
    subscriber.create_subscription(name=subscription_path, topic=topic_name)
  return subscription_path, subscriber


def validate_message(message):
  valid = False
  payload = []
  try:
    reader = DataFileReader(io.BytesIO(message.data), DatumReader())
    payload = [msg for msg in reader]
    reader.close()
  except:
    logging.exception("got exception")
  logging.debug(f"message {message} data {message.data} payload {payload}")
  for m in payload:
    this_uuid = m[uuid_column]
    if this_uuid == uuid_value:
      logging.info(f"Received uuid {this_uuid} Spez is working correctly")
      valid = True
    else:
      logging.warn("Received uuid {this_uuid} this may happen if you have stale data or multiple writers, waiting for more messages...")
  return valid


def wait_for_message(project_id):
  subscription_path, subscriber = setup_subscriber(project_id)
  with subscriber:
    msg_not_found = True
    while msg_not_found:
      response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": 10},
        retry=retry.Retry(deadline=300),
      )

      ack_ids = []
      for received_message in response.received_messages:
        logging.debug(f"Received: {received_message.message.data}.")
        ack_ids.append(received_message.ack_id)
        if msg_not_found and validate_message(received_message.message):
          msg_not_found = False
        # Acknowledges the received messages so they will not be sent again.
        subscriber.acknowledge(
          request={"subscription": subscription_path, "ack_ids": ack_ids}
        )


def main(project_id):
  logging.basicConfig(level=logging.INFO)
  logging.info('Reseting LPTS timestamp for %s/%s/%s', sink_instance_id, sink_database_id, sink_table_name)
  reset_lpts(sink_instance_id, sink_database_id, sink_table_name)
  logging.info('Storing single event with uuid %s', uuid_value)
  sink_database.run_in_transaction(insert_row, uuid_value)
  logging.info('Waiting for event to appear on pubsub')
  wait_for_message(project_id)


if __name__ == '__main__':
  main(sys.argv[1])
