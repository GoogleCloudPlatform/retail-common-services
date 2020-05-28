#!/bin/bash -ex

TAG=$1
NEXT_TAG=$2

if [ -z $TAG ] || [ -z $NEXT_TAG ]; then
  echo "$0 tag next-tag"
  exit 1
fi

DOCKERFILE="-f cdc/docker/Dockerfile.SpannerTailerService"
BASEFILE="-f cdc/docker/Dockerfile.build-env"
IMAGE="spez/spanner-tailer-service"
GCR_REPO="gcr.io/retail-common-services-249016"

if ! git tag --contains $TAG > /dev/null 2>&1; then
  ./gradlew release -Prelease.useAutomaticVersion=true -Prelease.releaseVersion=$TAG -Prelease.newVersion=$NEXT_TAG-SNAPSHOT
fi

git checkout -b release-$TAG $TAG || git checkout release-$TAG

docker build --target build-env -t spez/build-env $BASEFILE .
docker build --target prod      -t $IMAGE -t $IMAGE:$TAG $DOCKERFILE .
docker tag $IMAGE:$TAG $GCR_REPO/$IMAGE:$TAG
docker tag $IMAGE:$TAG $GCR_REPO/$IMAGE:latest
docker push $GCR_REPO/$IMAGE:$TAG
docker push $GCR_REPO/$IMAGE:latest

git checkout master
