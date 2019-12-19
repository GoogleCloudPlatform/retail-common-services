/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.spannerclient;

public class Queue {
  private final Database database;
  private final String queueName;

  Queue(Database database, String queueName) {
    this.database = database;
    this.queueName = queueName;
  }

  public RowCursor query(String sql) {
    return null;
  }

  public boolean send(String topic, String messageKey, String data) {
    return true;
  }

  public long createSubscription(String topic) {
    return 0; // returns subscriptionId
  }

  public boolean recieve(
      String topic, long subscriptionId, ReceiveOptions receiveOptions, MessageCallback handler) {
    return true;
  }

  public Database getDatabase() {
    return database;
  }

  public String getQueueName() {
    return queueName;
  }
}
