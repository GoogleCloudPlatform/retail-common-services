variable "terraform_credentials_file" {
  default = "../../secrets/terraform-admin.json"
}

variable "project" {}
variable "region" {
  default = "us-central1"
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
