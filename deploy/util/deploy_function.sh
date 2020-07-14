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

PROJECT=""
FNNAME=""
SRCDIR="../src/python/${FNAME}"
ZIP="../build/${FNAME}_source.zip"

zip $ZIP ${SRCDIR}/main.py ${SRCDIR}/requirements.txt

gsutil cp $ZIP  gs://spez-${PROJECT}-function-source/python/lpts_source.zip

gcloud functions deploy spez-${FNAME}-function-py \
--region us-central1 \
--entry-point ${FNAME} \
--runtime python37 \
--service-account spez-lpts-function-sa@${PROJECT}.iam.gserviceaccount.com \
--set-env-vars DATABASE=spez-lpts-database,INSTANCE=spez-lpts-instance,PROJECT=${PROJECT},TABLE=lpts \
--source gs://spez-${PROJECT}-function-source/python/${ZIP} \
--trigger-topic spez-ledger-topic
