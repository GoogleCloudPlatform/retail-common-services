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
	"context"
	"fmt"
	"math/rand"
	"strings"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
)


// Archiver consumes a Pub/Sub message and writes it to an GCS Object.
func Archiver(ctx context.Context, msg pubsub.Message) error {
	if msg.Attributes["Replay"] == "True" {
		return nil
	}

	client, err := createStorageClient(ctx)
	if err != nil {
		return err
	}

	// This matches the Spanner table name.
	bucketName := strings.ToLower(msg.Attributes["Topic"])
	fileName := msg.Attributes["Timestamp"] + "#" + constructRandomAppendix()
	err = writeMessageToStorage(ctx, bucketName, client, fileName, msg)
	if err != nil {
		return err
	}

	return nil
}

func createStorageClient(ctx context.Context) (*storage.Client, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("Failed to create client: %v", err)
	}
	return client, nil
}

func writeMessageToStorage(
	ctx context.Context,
	bucketName string,
	client *storage.Client,
	fileName string,
	msg pubsub.Message,
) error {
	writer := client.Bucket(bucketName).Object(fileName).NewWriter(ctx)
	writer.ContentType = "protobuf/bytes"
	writer.Metadata = map[string]string{
	//	"EffectiveDate": msg.Attributes["EffectiveDate"],
	//	"ItemNumber":    msg.Attributes["ItemNumber"],
		"Timestamp":     msg.Attributes["Timestamp"],
	}
	if _, err := writer.Write(msg.Data); err != nil {
		return fmt.Errorf("Failed to write data to bucket %q, file %q: %v", bucketName, fileName, err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("Failed to close bucket %q, file %q: %v", bucketName, fileName, err)
	}
	return nil
}

// constructRandomAppendix concatenates 4 random letters.
func constructRandomAppendix() string {
	appendix := make([]rune, 4)
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	for i := range appendix {
		appendix[i] = letters[rand.Intn(len(letters))]
	}
	return string(appendix)
}
