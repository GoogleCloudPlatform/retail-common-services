#!/bin/sh

###
 # Copyright 2020 Google LLC
 #
 # Licensed under the Apache License, Version 2.0 (the "License");
 # you may not use this file except in compliance with the License.
 # You may obtain a copy of the License at
 #
 #     https://www.apache.org/licenses/LICENSE-2.0
 #
 # Unless required by applicable law or agreed to in writing, software
 # distributed under the License is distributed on an "AS IS" BASIS,
 # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 # See the License for the specific language governing permissions and
 # limitations under the License.
 #
###

set -eux

PROJECT="rcs-demo-prod"
REGION="us-central1"

CLUSTER="spez-tailer-cluster"
SA="spez-node-sa@${PROJECT}.iam.gserviceaccount.com"
SECRET="../secrets/service-account.json"

#echo "Configuring GKE cluster"
gcloud container clusters create $CLUSTER --region=$REGION \
	--machine-type=n1-standard-8 \
	--num-nodes=1 \
	--service-account=$SA

#echo "Setting kubectl context"
gcloud container clusters get-credentials $CLUSTER --region=$REGION

#echo "Creating gke secret from file"
kubectl create secret generic service-account --from-file=$SECRET
