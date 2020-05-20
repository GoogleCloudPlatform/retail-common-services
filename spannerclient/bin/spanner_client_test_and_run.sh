#!/usr/bin/env bash

set -ef

docker build -t google/spanner-client . && docker run google/spanner-client $*
