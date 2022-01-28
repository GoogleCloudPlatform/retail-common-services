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

data "google_container_cluster" "primary" {
  name     = var.spez_tailer_cluster
  location = var.region
}

provider "kubernetes" {
  host  = "https://${data.google_container_cluster.primary.endpoint}"
  token = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(data.google_container_cluster.primary.master_auth[0].cluster_ca_certificate)
}

data "google_client_config" "default" {}

resource "kubernetes_service" "spez-tailer-service" {
  metadata {
    name = "spez-tailer-service-${var.sink_table}"
  }
  spec {
    port {
      port = var.jmx_port
      name = "jmx-port"
    }
    selector = {
      app = "spez-tailer-${var.sink_table}"
    }
  }
}

resource "kubernetes_deployment" "spez-tailer-deployment" {
  depends_on = [
    google_spanner_database.event-sink-database
  ]

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
          resources {
            limits = {
              memory = "16Gi"
            }
            requests = {
              memory = "8Gi"
            }
          }
          port {
            container_port = var.jmx_port
          }
          args = [
            "-ea",
            "-Djava.net.preferIPv4Stack=true",
            "-Dio.netty.allocator.type=pooled",
            "-XX:+UnlockExperimentalVMOptions",
            "-XX:+UseZGC",
            "-XX:ConcGCThreads=4",
            "-XX:+UseTransparentHugePages",
            "-XX:+UseNUMA",
            "-XX:+UseStringDeduplication",
            "-XX:+HeapDumpOnOutOfMemoryError",
            "-Dcom.sun.management.jmxremote",
            "-Dcom.sun.management.jmxremote.port=${var.jmx_port}",
            "-Dcom.sun.management.jmxremote.rmi.port=${var.jmx_port}",
            "-Dcom.sun.management.jmxremote.local.only=false",
            "-Dcom.sun.management.jmxremote.authenticate=false",
            "-Dcom.sun.management.jmxremote.ssl=false",
            "-Djava.rmi.server.hostname=127.0.0.1",
            "-Dspez.project_id=${var.project}",
            "-Dspez.auth.credentials=default",
            "-Dspez.pubsub.topic=${var.ledger_topic}",
            "-Dspez.sink.instance=${var.sink_instance}",
            "-Dspez.sink.database=${var.sink_database}",
            "-Dspez.sink.table=${var.sink_table}",
            "-Dspez.sink.uuid_column=${var.uuid_column}",
            "-Dspez.sink.timestamp_column=${var.timestamp_column}",
            "-Dspez.lpts.instance=${var.lpts_instance}",
            "-Dspez.lpts.database=${var.lpts_database}",
            "-Dspez.lpts.table=${var.lpts_table}",
            "-Dspez.loglevel.default=${var.log_level}",
            "-jar",
            "Main.jar"
          ]
        }
      }
    }
  }
}
