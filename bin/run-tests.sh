#!/bin/bash
JDK="openjdk:11"
docker pull $JDK
CACHE="$HOME/gradle.home"
mkdir -p "$CACHE"
D_UID=$(id -u)
D_GID=$(id -g)
D_USER=$(whoami)
docker run -v "$PWD:/data" -v "$CACHE:/tmp/.gradle" -w /data \
	-it --rm --name spanner-gradle $JDK \
	/bin/bash -c "addgroup --gid $D_GID $D_USER && \
	              adduser --uid $D_UID --gid $D_GID --disabled-password --gecos '' $D_USER && \
		      runuser -u $D_USER -- ./gradlew -g /tmp/.gradle -t test"
