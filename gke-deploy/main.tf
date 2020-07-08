terraform {
  required_version = ">= 0.12.0"
}

variable "project" {
  default = "spanner-event-exporter"
}

variable "region" {
  default = "us-central1"
}

variable "branch" {
  default = "master"
}

variable "cluster_name" {
  default = "spez"
}

variable "cluster_location" {
  default = "us-central1"
}

variable "tailer_image" {
}

variable "secret_name" {
  default = "credentials.json"
}

variable "lpts_instance" {
  default = "spez-lpts-instance"
}
variable "lpts_database" {
  default = "spez-lpts-database"
}
variable "lpts_table" {
  default = "lpts"
}

variable "ledger_topic" {
  default = "spez-ledger-topic"
}

variable "spez_tailer_cluster" {
  default = "spez-tailer-cluster"
}

variable "sink_instance" {
  default = "spez-test-instance"
}
variable "sink_database" {
  default = "spez-test-database"
}
variable "sink_table" {
  default = "test"
}
variable "uuid_column" {
  default = "uuid"
}
variable "timestamp_column" {
  default = "commit_timestamp"
}



provider "google" {
  project = var.project
  region  = var.region
}

data "google_container_cluster" "spez" {
  name     = var.cluster_name
  location = var.cluster_location
}

data "google_client_config" "default" {}

provider "kubernetes" {
  load_config_file = false

  host = data.google_container_cluster.spez.endpoint
  cluster_ca_certificate = base64decode(data.google_container_cluster.spez.master_auth[0].cluster_ca_certificate)
  token = data.google_client_config.default.access_token
}

/*
data "google_service_account" "spez" {
  account_id = "spez-tailer"
}

resource "google_service_account_key" "spez" {
  service_account_id = data.google_service_account.spez.name
}

resource "kubernetes_secret" "spez-secret" {
  metadata {
    name = "service-account"
  }
  data = {
    "${var.secret_name}" = base64decode(google_service_account_key.spez.private_key)
  }
}

resource "kubernetes_service" "spez-tailer-service" {
  metadata {
    name = "spez-tailer-service"
  }
  spec {
    port {
      port = 9010
      name = "jmx-port"
    }
    selector = {
      app = "spez-tailer"
    }
  }
}
*/

resource "kubernetes_deployment" "spez-tailer-deployment" {
  metadata {
    name = "spez-tailer-deployment"
  }
  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "spez-tailer"
      }
    }
    template {
      metadata {
        labels = {
          app = "spez-tailer"
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
            limits {
              memory = "3Gi"
            }
            requests {
              memory = "2Gi"
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
            "-Dspez.loglevel.default=DEBUG",
          ]
        }
      }
    }
  }
}
