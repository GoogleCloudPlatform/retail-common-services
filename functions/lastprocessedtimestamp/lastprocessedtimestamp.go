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
	"os"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
)

// GCP_PROJECT is automatically set in the GCF environment.
var project = os.Getenv("GCP_PROJECT")
var databaseName = "projects/your-project-id/instances/your-instance-id/databases/your-database-id"

// LastProcessedTimestamp records the last processed timestamp to bootstrap the tailer.
func LastProcessedTimestamp(ctx context.Context, msg pubsub.Message) error {
	if msg.Attributes["Replay"] == "True" {
		return nil
	}

	client, err := createSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	mutations := constructMutations(msg)
	err = applyMutations(ctx, client, mutations)
	if err != nil {
		return err
	}

	return nil
}

func createSpannerClient(ctx context.Context) (*spanner.Client, error) {
	client, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		return nil, fmt.Errorf("Failed to create Spanner client: %v", err)
	}
	return client, nil
}

func constructMutations(msg pubsub.Message) []*spanner.Mutation {
	return []*spanner.Mutation{
		spanner.InsertOrUpdate(
			"LastProcessedTimestamp",
			[]string{
				"Id",
				"LastProcessedTimestamp",
				"CommitTimestamp",
			},
			[]interface{}{
				1,
				msg.Attributes["Timestamp"],
				spanner.CommitTimestamp,
			},
		),
	}
}

func applyMutations(
	ctx context.Context,
	client *spanner.Client,
	mutations []*spanner.Mutation,
) error {
	_, err := client.Apply(ctx, mutations)
	if err != nil {
		return fmt.Errorf("Failed to apply row mutation: %v", err)
	}
	return nil
}
