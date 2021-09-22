#!/bin/bash -eu
#
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


if [[ -z ${PROJECT_ID+x} ]]; then
  echo "Please set PROJECT_ID"
  exit 1
fi

if [[ -z ${BUCKET_NAME+x} ]]; then
  echo "Please set BUCKET_NAME"
  exit 1
fi

REPO_NAME=$(basename $(git rev-parse --show-toplevel))
SHORT_SHA=$(git rev-parse --short HEAD)

gcloud config set project $PROJECT_ID

cloud-build-local --config=.cloudbuild.yaml \
                  --dryrun=false \
                  --substitutions="_BUCKET_NAME=$BUCKET_NAME,REPO_NAME=$REPO_NAME,SHORT_SHA=$SHORT_SHA" \
                  "$PWD"
