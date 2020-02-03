#!/usr/bin/env bash

set -ef

CONTAINER_NAME=spanner-tailer

docker container kill $CONTAINER_NAME || echo

docker build -t spez/spanner-tailer-service -f cdc/docker/Dockerfile.SpannerTailerService . && \
	docker run --name $CONTAINER_NAME -p 9010:9010 -t --rm spez/spanner-tailer-service:latest
