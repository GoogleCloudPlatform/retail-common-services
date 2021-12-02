variable "terraform_credentials_file" {
  default = "~/secrets/terraform-admin.json"
}

variable "project" {}
variable "region" {
  default = "us-central1"
}

variable "ledger_topic" {
  default = "spez-ledger-topic"
}

variable "log_level" {
  default = "INFO"
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

variable "secret_name" {
  default = "service-account.json"
}

variable "spez_tailer_cluster" {
  default = "spez-tailer-cluster"
}

variable "sink_instance" {}
variable "sink_database" {}
variable "sink_table" {}

variable "tailer_image" {
  default = "gcr.io/spanner-event-exporter/spez:latest"
}

variable "timestamp_column" {}
variable "uuid_column" {}
