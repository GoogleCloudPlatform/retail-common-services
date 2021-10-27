#!/bin/bash -eux

PROJECT_ID=$1

DOCKER_RUN_TF="run -u $(id -u):$(id -g) -v $HOME:$HOME -w $PWD -i -t hashicorp/terraform:latest"

pushd terraform/spez-core
docker $DOCKER_RUN_TF plan
docker $DOCKER_RUN_TF apply -var project=$PROJECT_ID -auto-approve
