#!/bin/bash
pushd example-script
project_id=$1
docker build -t rcs-example-script:latest .
docker run -v "$HOME/.config/gcloud:/gcp/config:ro" \
  --env CLOUDSDK_CONFIG=/gcp/config \
  --env GOOGLE_APPLICATION_CREDENTIALS=/gcp/config/application_default_credentials.json \
  --env GCLOUD_PROJECT=rcs-demo-prod \
  -it rcs-example-script:latest $project_id
popd
