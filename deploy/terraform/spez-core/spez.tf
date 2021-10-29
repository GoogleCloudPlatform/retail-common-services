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

terraform {
  required_version = ">= 0.12.0"
}

provider "google" {
  //credentials = file(var.terraform_credentials_file)

  project = var.project
  region  = var.region
}

provider "archive" {}

# <lpts table>
resource "google_spanner_instance" "spez-lpts-instance" {
  name   = "spez-lpts-instance"
  config = join("-", ["regional", var.region])

  display_name = "spez-lpts-instance"
  num_nodes    = 1
}

resource "google_spanner_database" "spez-lpts-database" {
  name     = "spez-lpts-database"
  instance = google_spanner_instance.spez-lpts-instance.name

  ddl = [
    "CREATE TABLE lpts (instance STRING(MAX), database STRING(MAX), table STRING(MAX), LastProcessedTimestamp TIMESTAMP, CommitTimestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY(instance, database, table)"
  ]
}
# </lpts table>

#<ledger>
resource "google_pubsub_topic" "spez-ledger-topic" {
  name = "spez-ledger-topic"

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }
}
#</ledger>

#<source bucket>
resource "google_storage_bucket" "spez-function-source" {
  name     = join("-", ["spez", var.project, "function-source"])
  location = var.region
  uniform_bucket_level_access = true
}
#</source bucket>

#<archive>
resource "google_storage_bucket" "spez-event-archive" {
  name     = join("-", ["spez", var.project, "event-archive"])
  location = var.region
  uniform_bucket_level_access = true
}

resource "google_service_account" "spez-archive-function-sa" {
  account_id   = "spez-archive-function-sa"
  display_name = "Spez Archive Function Service Account"
}
resource "google_project_iam_member" "spez-archive-function-sa-project-iam-member-storage" {
  project = var.project
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.spez-archive-function-sa.email}"
}
resource "google_project_iam_member" "spez-archive-function-sa-project-iam-member-pubsub" {
  project = var.project
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.spez-archive-function-sa.email}"
}

data "archive_file" "local_archive_source" {
  type        = "zip"
  source_dir  = "../../src/python/archive/"
  output_path = "../../build/archive_source.zip"
}
resource "google_storage_bucket_object" "gcs-archive-source" {
  name   = "archive_source.zip"
  bucket = google_storage_bucket.spez-function-source.name
  source = data.archive_file.local_archive_source.output_path
  metadata = {
    package = "retail-common-services"
  }
}
resource "google_cloudfunctions_function" "spez-archive-function" {
  name        = "spez-archive-function"
  description = "Spez Archive Function"
  runtime     = "python37"

  entry_point = "archive"
  environment_variables = {
    BUCKET  = google_storage_bucket.spez-event-archive.name
  }
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.spez-ledger-topic.id
  }
  service_account_email = google_service_account.spez-archive-function-sa.email
  source_archive_bucket = google_storage_bucket.spez-function-source.name
  source_archive_object = google_storage_bucket_object.gcs-archive-source.name
  labels = {
    package = "retail-common-services"
  }
}
#</archive>


#<lpts>
resource "google_service_account" "spez-lpts-function-sa" {
  account_id   = "spez-lpts-function-sa"
  display_name = "Spez Last Processed Timestamp Function Service Account"
}
resource "google_project_iam_member" "spez-lpts-function-sa-project-iam-member-spanner" {
  project = var.project
  role    = "roles/spanner.databaseUser"
  member  = "serviceAccount:${google_service_account.spez-lpts-function-sa.email}"
}
resource "google_project_iam_member" "spez-lpts-function-sa-project-iam-member-pubsub" {
  project = var.project
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.spez-lpts-function-sa.email}"
}

data "archive_file" "local_lpts_source" {
  type        = "zip"
  source_dir  = "../../src/python/lpts/"
  output_path = "../../build/lpts_source.zip"
}

resource "google_storage_bucket_object" "gcs-lpts-source" {
  name   = "lpts_source.zip"
  bucket = google_storage_bucket.spez-function-source.name
  source = data.archive_file.local_lpts_source.output_path
}

resource "google_cloudfunctions_function" "spez-lpts-function" {
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
#</lpts>
