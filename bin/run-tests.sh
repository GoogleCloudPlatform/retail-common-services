#!/bin/bash
docker pull openjdk:11
mkdir -p "$HOME/gradle.home"
docker run -it --rm -v "$PWD":/data -v "$HOME/gradle.home:/root/.gradle" -w /data openjdk:11 ./gradlew --no-daemon -t test
