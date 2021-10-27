#!/bin/bash -eux

PROJECT_ID=$1

function docker-run-tf {
  docker run -u $(id -u):$(id -g) -v $HOME:$HOME -w $PWD -i -t hashicorp/terraform:latest "$@"
}

pushd terraform/spez-core
docker-run-tf plan
docker-run-tf apply -var project=$PROJECT_ID -auto-approve
