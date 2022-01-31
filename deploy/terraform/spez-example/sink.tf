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

resource "google_spanner_instance" "event-sink-instance" {
  name   = var.sink_instance
  config = join("-", ["regional", var.region])

  display_name = "event-sink-instance"
  num_nodes    = 1
}

locals {
  sink_ddl = <<-EOT
    CREATE TABLE ${var.sink_table} (
      uuid STRING(MAX) NOT NULL,
      CommitTimestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
    ) primary key (uuid)
  EOT
  setup_lpts = <<-EOT
    echo "Configuring LPTS entry";
    gcloud spanner databases execute-sql spez-lpts-database --instance=spez-lpts-instance --sql="INSERT INTO lpts (instance, database, table, CommitTimestamp, LastProcessedTimestamp) VALUES('${var.sink_instance}', '${var.sink_database}', '${var.sink_table}', PENDING_COMMIT_TIMESTAMP(), '1970-01-01T00:00:00.000000Z');" || echo "Already configured"
  EOT
}

resource "google_spanner_database" "event-sink-database" {
  name     = var.sink_database
  instance = google_spanner_instance.event-sink-instance.name
  deletion_protection = false

  ddl = [
    local.sink_ddl
  ]
  provisioner "local-exec" {
    command = local.setup_lpts
  }
}
