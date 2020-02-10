#!/usr/bin/env bash

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
else
  BUILD="y"
fi

XSOCK=/tmp/.X11-unix
JMC=$PWD/../jmc
DOCKERFILE="-f cdc/docker/Dockerfile.SpannerTailerService"
BASEFILE="-f cdc/docker/Dockerfile.build-env"
set -x
if [[ "$BUILD" = "y" ]]; then
  docker build --target build-env -t spez/build-env $BASEFILE .
  docker build --target dev       -t spez/spanner-tailer-service-dev $DOCKERFILE .
  docker build --target prod      -t spez/spanner-tailer-service $DOCKERFILE .
fi
docker run --name $CONTAINER_NAME -v $XSOCK:$XSOCK -v $JMC:/app/jmc -p 9010:9010 -t --rm $IMAGE
