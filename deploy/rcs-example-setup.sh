#!/bin/bash -eux

PROJECT_ID=$1
IMAGE_TAG=$2
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

pushd terraform/spez-example

terraform plan -var project=$PROJECT_ID -var "tailer_image=$(tailer-image ${PROJECT_ID} ${IMAGE_TAG})" -out=tf.plan
if [ "$DRYRUN" = "" ]; then
    terraform apply -auto-approve tf.plan
fi
rm -f tf.plan
popd
