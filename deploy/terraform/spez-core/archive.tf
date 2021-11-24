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

# <archive function>
resource "google_storage_bucket" "spez-event-archive" {
  depends_on = [
    google_project_service.storage-component
  ]
  name     = join("-", ["spez", var.project, "event-archive"])
  location = var.region
  uniform_bucket_level_access = true
}

resource "google_service_account" "spez-archive-function-sa" {
  account_id   = "spez-archive-function-sa"
  display_name = "Spez Archive Function Service Account"
}

resource "google_project_iam_member" "spez-archive-function-sa-role" {
  depends_on = [
    google_project_service.cloudresourcemanager,
    google_project_service.iam
  ]
  for_each = toset( [
    "roles/pubsub.subscriber",
    "roles/storage.admin"
    ] )
  project  = var.project
  role     = each.key
  member   = "serviceAccount:${google_service_account.spez-archive-function-sa.email}"
}

data "archive_file" "local_archive_source" {
  type        = "zip"
  source_dir  = "../../src/python/archive/"
  output_path = "../../build/archive_source.zip"
}

resource "google_storage_bucket_object" "gcs-archive-source" {
  depends_on = [
    google_project_service.storage-component
  ]
  name   = "archive_source.zip"
  bucket = google_storage_bucket.spez-function-source.name
  source = data.archive_file.local_archive_source.output_path
  metadata = {
    package = "retail-common-services"
  }
}

resource "google_cloudfunctions_function" "spez-archive-function" {
  depends_on = [
    google_project_service.cloudfunctions
  ]
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
# </archive function>
