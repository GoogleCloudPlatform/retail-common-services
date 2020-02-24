#!/bin/bash -eu

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
