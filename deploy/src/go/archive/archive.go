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
	"math/rand"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
)


func Archive(ctx context.Context, msg *pubsub.Message) error {
	bucket := os.Getenv("BUCKET")

	sinkInstance := msg.Attributes["spez.sink.instance"]
	sinkDatabase := msg.Attributes["spez.sink.database"]
	sinkTable := msg.Attributes["spez.sink.table"]
	sinkUuid := msg.Attributes["spez.sink.uuid"]
	sinkCommitTimestamp := msg.Attributes["spez.sink.commit_timestamp"]

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Println(err)
	}

	objectPath := sinkInstance + "/" + sinkDatabase + "/" + sinkTable + "/"
	objectName := sinkUuid + "_" + sinkCommitTimestamp + "#"
	appendix := randomAppendix()
	object := objectPath + objectName + appendix

	writer := client.Bucket(bucket).Object(object).NewWriter(ctx)
	writer.ContentType = "protobuf/bytes"
        writer.Metadata = map[string]string{
		"spez.sink.instance": msg.Attributes["spez.sink.instance"],
		"spez.sink.database": msg.Attributes["spez.sink.database"],
		"spez.sink.table": msg.Attributes["spez.sink.table"],
		"spez.sink.uuid": msg.Attributes["spez.sink.uuid"],
		"spez.sink.commit_timestamp": msg.Attributes["spez.sink.commit_timestamp"],
	}

	_, err = writer.Write(msg.Data)
	if err != nil {
		log.Println(err)
	}

	if err := writer.Close(); err != nil {
		log.Println(err)
	}

	fmt.Printf("Archived event: %v\n", object)
	msg.Ack()
	return nil
}

func randomAppendix() string {
	appendix := make([]rune, 4)
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	for i := range appendix {
		rand.Seed(time.Now().UnixNano())
		appendix[i] = letters[rand.Intn(len(letters))]
	}
	return string(appendix)
}
