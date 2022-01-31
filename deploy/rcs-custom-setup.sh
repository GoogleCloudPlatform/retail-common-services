#!/bin/bash -eux

CUSTOM_DIR=$1
PROJECT_ID=$2
IMAGE_TAG=$3
IPV6_HACK=true
DRYRUN=${DRYRUN:-}
USE_GH_IMAGE=true

function tailer-image() {
  local project_id=$1
  local image_tag=$2
  if [[ "$USE_GH_IMAGE" = "true" ]]; then
    echo "ghcr.io/googlecloudplatform/retail-common-services/spanner-event-exporter:${image_tag}"
  else
    echo "gcr.io/${project_id}/spanner-event-exporter:${image_tag}"
  fi
}

function tf-bucket() {
  local project_id=$1
  echo "$project_id-rcs-tf"
}

set +x
if [ "$IPV6_HACK" = "true" ]; then
# Workaround https://github.com/hashicorp/terraform-provider-google/issues/6782
    sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1 net.ipv6.conf.default.disable_ipv6=1 net.ipv6.conf.lo.disable_ipv6=1 > /dev/null
    export APIS="googleapis.com www.googleapis.com storage.googleapis.com iam.googleapis.com container.googleapis.com cloudresourcemanager.googleapis.com"
    for name in $APIS
    do
      ipv4=$(getent ahostsv4 "$name" | head -n 1 | awk '{ print $1 }')
      grep -q "$name" /etc/hosts || ([ -n "$ipv4" ] && sudo sh -c "echo '$ipv4 $name' >> /etc/hosts")
    done
# Workaround end
fi
set -x

pushd terraform/$CUSTOM_DIR

terraform init -backend-config="bucket=$(tf-bucket $PROJECT_ID)"

terraform plan -var project=$PROJECT_ID -var "tailer_image=$(tailer-image ${PROJECT_ID} ${IMAGE_TAG})" -out=tf.plan
if [ "$DRYRUN" = "" ]; then
    terraform apply -auto-approve tf.plan
fi
rm -f tf.plan
popd
