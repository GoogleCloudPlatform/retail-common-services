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

resource "google_service_account" "node_sa" {
  account_id   = "spanner-event-exporter-node-sa"
  display_name = "Spanner Event Exporter Node"
}

resource "google_project_iam_member" "node_sa_role" {
  for_each = toset( [
    "roles/cloudtrace.agent",
    "roles/monitoring.metricWriter",
    "roles/pubsub.publisher",
    "roles/spanner.databaseUser"
    ] )
  project  = var.project
  role     = each.key
  member   = "serviceAccount:${google_service_account.node_sa.email}"
}

resource "google_container_cluster" "primary" {
  name               = "spanner-event-exporter"
  location           = var.region
  initial_node_count = 1
  node_config {
    machine_type       = "n1-standard-8"
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    service_account = google_service_account.node_sa.email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
  timeouts {
    create = "30m"
    update = "40m"
  }
}
