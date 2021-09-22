#!/usr/bin/env bash
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


set -ef

CONTAINER_NAME=spanner-tailer

docker container kill $CONTAINER_NAME > /dev/null 2>&1 || echo

if [[ "$1" = "prod" ]]; then
  IMAGE=spez/spanner-tailer-service:latest
  shift
else
  IMAGE=spez/spanner-tailer-service-dev:latest
fi

if [[ "$1" = "nobuild" ]]; then
  BUILD="n"
  shift
else
  BUILD="y"
fi

XSOCK=/tmp/.X11-unix
JMC=$PWD/../jmc
DOCKERFILE_DEV="-f cdc/docker/Dockerfile.SpannerTailerService.dev"
DOCKERFILE="-f cdc/docker/Dockerfile.SpannerTailerService"
BASEFILE="-f cdc/docker/Dockerfile.build-env"
set -x
if [[ "$BUILD" = "y" ]]; then
  docker image inspect spez/build-env > /dev/null || docker build --target build-env -t spez/build-env $BASEFILE .
  docker build --target dev       -t spez/spanner-tailer-service-dev $DOCKERFILE_DEV .
  docker build --target prod      -t spez/spanner-tailer-service $DOCKERFILE .
fi

SECRET_MOUNT="$PWD/secrets:/var/run/secret/cloud.google.com"

docker run \
           --name $CONTAINER_NAME \
           -v $XSOCK:$XSOCK \
           -v $JMC:/app/jmc \
           -v $SECRET_MOUNT \
           -p 9010:9010 \
           -t \
           --rm $IMAGE $*
