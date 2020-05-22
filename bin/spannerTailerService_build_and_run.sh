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
  shift
else
  BUILD="y"
fi

XSOCK=/tmp/.X11-unix
JMC=$PWD/../jmc
DOCKERFILE="-f cdc/docker/Dockerfile.SpannerTailerService"
BASEFILE="-f cdc/docker/Dockerfile.build-env"
set -x
if [[ "$BUILD" = "y" ]]; then
  docker image inspect spez/build-env > /dev/null || docker build --target build-env -t spez/build-env $BASEFILE .
  docker build --target dev       -t spez/spanner-tailer-service-dev $DOCKERFILE .
  docker build --target prod      -t spez/spanner-tailer-service $DOCKERFILE .
fi

export SPEZ_PROJECT_ID=${SPEZ_PROJECT_ID:-retail-common-services-249016}
export SPEZ_AUTH_CREDENTIALS=${SPEZ_AUTH_CREDENTIALS:-service-account.json}
export SPEZ_PUBSUB_TOPIC=${SPEZ_PUBSUB_TOPIC:-test-topic}
export SPEZ_SPANNERDB_INSTANCE=${SPEZ_SPANNERDB_INSTANCE:-test-db}
export SPEZ_SPANNERDB_DATABASE=${SPEZ_SPANNERDB_DATABASE:-test}
export SPEZ_SPANNERDB_TABLE=${SPEZ_SPANNERDB_TABLE:-test}
export SPEZ_SPANNERDB_UUID_COLUMN
export SPEZ_SPANNERDB_TIMESTAMP_COLUMN

SECRET_MOUNT="$PWD/secrets:/var/run/secret/cloud.google.com"

docker run \
           --env SPEZ_PROJECT_ID \
           --env SPEZ_AUTH_CREDENTIALS \
           --env SPEZ_PUBSUB_TOPIC \
           --env SPEZ_SPANNERDB_INSTANCE \
           --env SPEZ_SPANNERDB_DATABASE \
           --env SPEZ_SPANNERDB_TABLE \
           --env SPEZ_SPANNERDB_UUID_COLUMN \
           --env SPEZ_SPANNERDB_TIMESTAMP_COLUMN \
           --name $CONTAINER_NAME \
	   -v $XSOCK:$XSOCK \
	   -v $JMC:/app/jmc \
	   -v $SECRET_MOUNT \
	   -p 9010:9010 \
	   -t \
	   --rm $IMAGE $*
