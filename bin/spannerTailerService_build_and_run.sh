#!/usr/bin/env bash

set -ef

docker build -t spez/spanner-tailer-service -f cdc/docker/Dockerfile.SpannerTailerService . && docker run -p 9010:9010 -t spez/spanner-tailer-service:latest
