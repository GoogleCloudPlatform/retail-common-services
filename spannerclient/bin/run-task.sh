#!/bin/bash
docker exec -it -w /data -u $(whoami) spanner-gradle ./gradlew -g /tmp/.gradle "$*"
