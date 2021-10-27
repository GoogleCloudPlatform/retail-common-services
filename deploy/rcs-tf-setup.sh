#!/bin/bash -eux

PROJECT_ID=$1

echo "enabling gcloud"
gcloud config set project $PROJECT_ID
#gcloud auth login


echo "enabling services"
gcloud services enable spanner.googleapis.com
gcloud services enable compute.googleapis.com
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable iam.googleapis.com
gcloud services enable container.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable storage-component.googleapis.com
gcloud services enable containerregistry.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable cloudbuild.googleapis.com

echo "enabling iam"
gcloud iam service-accounts create terraform-admin
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:terraform-admin@$PROJECT_ID.iam.gserviceaccount.com --role=roles/editor
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:terraform-admin@$PROJECT_ID.iam.gserviceaccount.com --role=roles/iam.securityAdmin

echo "creating terraform instance"

gcloud beta compute --project=$PROJECT_ID instances create tf-runner --zone=us-central1-a --machine-type=n1-standard-2 --subnet=default --network-tier=PREMIUM --maintenance-policy=MIGRATE --service-account=terraform-admin@$PROJECT_ID.iam.gserviceaccount.com --scopes=https://www.googleapis.com/auth/cloud-platform --boot-disk-size=10GB --boot-disk-type=pd-standard --boot-disk-device-name=tf-runner --reservation-affinity=any
