/**
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
 */

# <lpts table>
resource "google_spanner_instance" "spez-lpts-instance" {
  depends_on = [
    google_project_service.enabled
  ]
  name   = "spez-lpts-instance"
  config = join("-", ["regional", var.region])

  display_name = "spez-lpts-instance"
  num_nodes    = 1
}

resource "google_spanner_database" "spez-lpts-database" {
  depends_on = [
    google_project_service.enabled
  ]
  name     = "spez-lpts-database"
  instance = google_spanner_instance.spez-lpts-instance.name

  ddl = [
    "CREATE TABLE lpts (instance STRING(MAX), database STRING(MAX), table STRING(MAX), LastProcessedTimestamp TIMESTAMP, CommitTimestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY(instance, database, table)"
  ]
}
# </lpts table>

# <lpts function>
resource "google_service_account" "spez-lpts-function-sa" {
  account_id   = "spez-lpts-function-sa"
  display_name = "Spez Last Processed Timestamp Function Service Account"
}

resource "google_project_iam_member" "spez-lpts-function-sa-role" {
  depends_on = [
    google_project_service.enabled
  ]
  for_each = toset( [
    "roles/pubsub.subscriber",
    "roles/spanner.databaseUser"
    ] )
  project  = var.project
  role     = each.key
  member   = "serviceAccount:${google_service_account.spez-lpts-function-sa.email}"
}

data "archive_file" "local_lpts_source" {
  type        = "zip"
  source_dir  = "../../src/python/lpts/"
  output_path = "../../build/lpts_source.zip"
}

resource "google_storage_bucket_object" "gcs-lpts-source" {
  depends_on = [
    google_project_service.enabled
  ]
  name   = "lpts_source.zip"
  bucket = google_storage_bucket.spez-function-source.name
  source = data.archive_file.local_lpts_source.output_path
}

resource "google_cloudfunctions_function" "spez-lpts-function" {
  depends_on = [
    google_project_service.enabled
  ]
  name        = "spez-lpts-function"
  description = "Spez Last Processed Timestamp Cluster"
  runtime     = "python37"

  entry_point = "lpts"
  environment_variables = {
    PROJECT  = var.project
    INSTANCE = var.lpts_instance 
    DATABASE = var.lpts_database
    TABLE    = var.lpts_table
  }
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.spez-ledger-topic.id
  }
  service_account_email = google_service_account.spez-lpts-function-sa.email
  source_archive_bucket = google_storage_bucket.spez-function-source.name
  source_archive_object = google_storage_bucket_object.gcs-lpts-source.name
}
# </lpts function>
