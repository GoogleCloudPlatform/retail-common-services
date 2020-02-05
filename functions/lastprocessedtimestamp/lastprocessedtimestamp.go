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

package retail


import (
        "cloud.google.com/go/pubsub"
        "cloud.google.com/go/spanner"
        "context"
        "log"
	"os"
)

var projectID = os.Getenv("GCP_PROJECT")
var instanceName = os.Getenv("INSTANCE_NAME")
var databaseName = os.Getenv("DATABASE_NAME")
var tableName = os.Getenv("TABLE_NAME")

var databaseID = "projects/" + projectID + "/instances/" + instanceName + "/databases/" + databaseName 

// PubSubMessage is the payload of a Pub/Sub event.
//type PubSubMessage struct {
	// This field is read-only.
	//ID string

	// Data is the actual data in the message.
	//Data []byte

	// Attributes represents the key-value pairs the current message
	// is labelled with.
	//Attributes map[string]string

	// The time at which the message was published.
	// This is populated by the server for Messages obtained from a subscription.
	// This field is read-only.
	//PublishTime time.Time
	// contains filtered or unexported fields
//}


func LastProcessedTimestamp(ctx context.Context, msg *pubsub.Message) error {

        client, err := spanner.NewClient(ctx, databaseID)
        if err != nil {
                log.Fatal(err)
        }

        columns := []string{"Id", "LastProcessedTimestamp", "CommitTimestamp"}

	      m := []*spanner.Mutation{
                        spanner.InsertOrUpdate(tableName, columns, []interface{}{1, msg.Attributes["Timestamp"], spanner.CommitTimestamp,}),
                }

                go func(client *spanner.Client, ctx context.Context, m []*spanner.Mutation,  msg *pubsub.Message) {
                        _, err1 := client.Apply(ctx, m)
                        if err1 != nil {
                                log.Print(err1)
                        } else {
                            msg.Ack()
                        }
                }(client, ctx, m, msg)
    return nil
}
