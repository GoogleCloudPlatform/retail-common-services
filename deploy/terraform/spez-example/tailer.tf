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
  backend "gcs" {
    prefix  = "spez-example-terraform-state"
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

provider "kubernetes" {
  host = data.google_container_cluster.spez-tailer-cluster.endpoint
  token = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(data.google_container_cluster.spez-tailer-cluster.master_auth[0].cluster_ca_certificate)
}

data "google_client_config" "default" {}

data "google_container_cluster" "spez-tailer-cluster" {
  name     = var.spez_tailer_cluster
  location = var.region
}

resource "kubernetes_service" "spez-tailer-service" {
  metadata {
    name = "spez-tailer-service-${var.sink_table}"
  }
  spec {
    port {
      port = 9010
      name = "jmx-port"
    }
    selector = {
      app = "spez-tailer-${var.sink_table}"
    }
  }
}

resource "kubernetes_deployment" "spez-tailer-deployment" {
  metadata {
    name = "spez-tailer-deployment-${var.sink_table}"
  }
  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "spez-tailer-${var.sink_table}"
      }
    }
    template {
      metadata {
        labels = {
          app = "spez-tailer-${var.sink_table}"
          version = "v1"
        }
      }
      spec {
        volume {
          name = "service-account"
          secret {
            secret_name = "service-account"
          }
        }
        container {
          name = "spez-tailer"
          image = var.tailer_image
          image_pull_policy = "Always"
          volume_mount {
            name = "service-account"
            mount_path = "/var/run/secret/cloud.google.com"
            read_only = "true"
          }
          resources {
            limits = {
              memory = "16Gi"
            }
            requests = {
              memory = "8Gi"
            }
          }
          port {
            container_port = 9010
          }
          args = [
            "-Dspez.project_id=${var.project}",
            "-Dspez.auth.credentials=${var.secret_name}",
            "-Dspez.pubsub.topic=${var.ledger_topic}",
            "-Dspez.sink.instance=${var.sink_instance}",
            "-Dspez.sink.database=${var.sink_database}",
            "-Dspez.sink.table=${var.sink_table}",
            "-Dspez.sink.uuid_column=${var.uuid_column}",
            "-Dspez.sink.timestamp_column=${var.timestamp_column}",
            "-Dspez.lpts.instance=${var.lpts_instance}",
            "-Dspez.lpts.database=${var.lpts_database}",
            "-Dspez.lpts.table=${var.lpts_table}",
            "-Dspez.loglevel.default=${var.log_level}"
          ]
        }
      }
    }
  }
}
