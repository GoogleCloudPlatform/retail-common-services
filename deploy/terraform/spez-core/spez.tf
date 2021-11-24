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
  backend "gcs" {
    prefix  = "spez-core-terraform-state"
  }
  required_version = ">= 0.12.0"
}

provider "google" {
  project = var.project
  region  = var.region
}

provider "archive" {}

# <ledger>
resource "google_pubsub_topic" "spez-ledger-topic" {
  name = "spez-ledger-topic"

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }
}
# </ledger>

# <source bucket>
resource "google_storage_bucket" "spez-function-source" {
  name     = join("-", ["spez", var.project, "function-source"])
  location = var.region
  uniform_bucket_level_access = true
}
# </source bucket>
