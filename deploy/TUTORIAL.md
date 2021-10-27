# Deploying the RCS example infrastructure

## Setup project

<walkthrough-project-setup billing="true"></walkthrough-project-setup>

## Run setup script

```sh
./rcs-tf-setup.sh {{project-id}}

```

## Setup the terraform runner

```sh
gcloud compute ssh tf-runner --zone=us-central1-a
export PROJECT_ID={{project-id}}
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install git kubectl unzip wget -y
gcloud config set project $PROJECT_ID
gcloud auth login
gcloud source repos clone deployment --project=spanner-event-exporter
cd ~/deployment/util
bash tf-install.sh
gcloud iam service-accounts keys create ~/secrets/terraform-admin.json --iam-account=terraform-admin@$PROJECT_ID.iam.gserviceaccount.com

```

## Provision the infrastructure

```sh
cd ~/deployment/terraform/spez-core/
terraform init
terraform plan
terraform apply -var project=$PROJECT_ID -auto-approve

```
