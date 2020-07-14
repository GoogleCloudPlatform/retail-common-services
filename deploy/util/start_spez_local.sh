#!/usr/bin/bash

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

RUNAS="spez-tailer-sa@css-storeops.iam.gserviceaccount.com"
IMAGE="gcr.io/css-storeops/spez/spanner-tailer-service-dev"

gcloud iam service-accounts keys create secrets/service-account.json --iam-account=$RUNAS

docker run -v "$PWD/secrets:/var/run/secret/cloud.google.com" --rm $IMAGE \
	-Dspez.project_id=css-storeops \
	-Dspez.auth.credentials=service-account.json \
	-Dspez.pubsub.topic=spez-ledger-topic \
	-Dspez.sink.instance=cortex-demo-tmp \
	-Dspez.sink.database=cortex-demo-products \
	-Dspez.sink.table=products \
	-Dspez.sink.uuid_column=uuid \
	-Dspez.sink.timestamp_column=Timestamp
        -Dspez.lpts.instance=spez-lpts-instance \
        -Dspez.lpts.database=spez-lpts-database \
        -Dspez.lpts.table=lpts \
	-Dspez.loglevel.default=INFO
