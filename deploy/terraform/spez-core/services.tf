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

resource "google_project_service" "cloudbuild" {
  service = "cloudbuild.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "cloudfunctions" {
  service = "cloudfunctions.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "cloudresourcemanager" {
  service = "cloudresourcemanager.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "compute" {
  service = "compute.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "container" {
  service = "container.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "containerregistry" {
  service = "containerregistry.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "iam" {
  service = "iam.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "pubsub" {
  service = "pubsub.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "spanner" {
  service = "spanner.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "storage-component" {
  service = "storage-component.googleapis.com"
  disable_dependent_services = true
}
