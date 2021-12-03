#!/bin/bash -eu

if [ "${1:-}" = "" ] || [ "${2:-}" = "" ] || [ "${3:-}" = "" ]; then
  echo "$0 project-id github-username gh-token-file gh-image-tag"
  exit -1
fi

PROJECT_ID="$1"
GH_USER="$2"
GH_TOKEN="$3"
GH_TAG="$4"
GH_HOST=ghcr.io
GH_REPO="${GH_HOST}/googlecloudplatform/retail-common-services/spanner-event-exporter"
GCR_HOST=gcr.io
GCR_REPO="${GCR_HOST}/${PROJECT_ID}/spanner-event-exporter"
GCR_TAG="${GH_TAG}"

cat "$GH_TOKEN" | docker login -u $GH_USER --password-stdin $GH_HOST
gcloud auth configure-docker --project $PROJECT_ID

# pull from github
echo "Pulling from github"

docker pull "${GH_REPO}:${GH_TAG}"
docker tag "${GH_REPO}:${GH_TAG}" "${GCR_REPO}:${GCR_TAG}"

# push to gcr
echo "Pushing to gcr"

docker push "${GCR_REPO}:${GCR_TAG}"
