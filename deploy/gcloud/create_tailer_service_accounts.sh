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
SECRETS="../secrets"
FQ="@${PROJECT}.iam.gserviceaccount.com"

# Tailer application SA
SA1="spez-tailer-sa"
#gcloud iam service-accounts create $SA1

gcloud projects add-iam-policy-binding $PROJECT --member=serviceAccount:$SA1$FQ --role=roles/cloudtrace.agent
gcloud projects add-iam-policy-binding $PROJECT --member=serviceAccount:$SA1$FQ --role=roles/monitoring.metricWriter
gcloud projects add-iam-policy-binding $PROJECT --member=serviceAccount:$SA1$FQ --role=roles/pubsub.publisher
gcloud projects add-iam-policy-binding $PROJECT --member=serviceAccount:$SA1$FQ --role=roles/spanner.databaseUser

gcloud iam service-accounts keys create ${SECRETS}/service-account.json --iam-account=$SA1$FQ


# Tailer cluster node SA
SA2="spez-node-sa"
gcloud iam service-accounts create $SA2

gcloud projects add-iam-policy-binding $PROJECT --member=serviceAccount:$SA2$FQ --role=roles/editor
