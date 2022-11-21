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
  labels = {
    goog-packaged-solution = "retail-common-services"
  }
}

resource "google_spanner_database" "spez-lpts-database" {
  depends_on = [
    google_project_service.enabled
  ]
  name     = "spez-lpts-database"
  instance = google_spanner_instance.spez-lpts-instance.name
  deletion_protection = false

  ddl = [
    "CREATE TABLE lpts (instance STRING(MAX), database STRING(MAX), table STRING(MAX), LastProcessedTimestamp TIMESTAMP, CommitTimestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY(instance, database, table)"
  ]
}
# </lpts table>
