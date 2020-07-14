/*
 * Copyright 2020 Google LLC
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
 *
 */

// Package p contains a Pub/Sub Cloud Function.
package p

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
)


func LastProcessedTimestamp(ctx context.Context, msg *pubsub.Message) error {
	project := os.Getenv("PROJECT")
	lptsInstance := os.Getenv("INSTANCE")
	lptsDatabase := os.Getenv("DATABASE")
	lptsTable := os.Getenv("TABLE")
	lptsId := "projects/" + project + "/instances/" + lptsInstance + "/databases/" + lptsDatabase

	sinkInstance := msg.Attributes["spez.sink.instance"]
	sinkDatabase := msg.Attributes["spez.sink.database"]
	sinkTable := msg.Attributes["spez.sink.table"]

	//TODO: We should do a more robust check of the timestamp.  time.Parse makes
	//		assumptions when values from the string being parsed are missing
	//		or the pattern otherwise doesn't match.  This could be *very* bad
	//		if not caught and an unexpected lpts (like the beginning of time)
	//		is written to the database.
	sinkCommitTimestamp, err := time.Parse(time.RFC3339Nano, msg.Attributes["spez.sink.commit_timestamp"])
	if err != nil {
		log.Println(err)
		msg.Nack()
		panic(err)
	}

	client, err := spanner.NewClient(ctx, lptsId)
	if err != nil {
		log.Println(err)
		msg.Nack()
		panic(err)
	}
	defer client.Close()

	columns := []string{"instance", "database", "table", "LastProcessedTimestamp", "CommitTimestamp"}

	mutation := []*spanner.Mutation{spanner.InsertOrUpdate(lptsTable, columns, []interface{}{sinkInstance, sinkDatabase, sinkTable, sinkCommitTimestamp, spanner.CommitTimestamp})}

	_, err = client.Apply(ctx, mutation)
	if err != nil {
		log.Println(err)
		panic(err)
	}

	fmt.Printf("LPTS updated: %v, %v, %v, %v\n", sinkInstance, sinkDatabase, sinkTable, sinkCommitTimestamp)
	msg.Ack()
	return nil
}
