#!/bin/bash
project_id=$1
docker build -t rcs-run-example .
docker run -v "$HOME/.config/gcloud:/gcp/config:ro" \
  --env CLOUDSDK_CONFIG=/gcp/config \
  --env GOOGLE_APPLICATION_CREDENTIALS=/gcp/config/application_default_credentials.json \
  --env GCLOUD_PROJECT=rcs-demo-prod \
  -it rcs-run-example $project_id
