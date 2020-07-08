#!/bin/bash -ex

TAG=$1
NEXT_TAG=$2

if [ -z $TAG ] || [ -z $NEXT_TAG ]; then
  echo "$0 tag next-tag"
  exit 1
fi


if ! git tag --contains $TAG > /dev/null 2>&1; then
  ./gradlew release -Prelease.useAutomaticVersion=true -Prelease.releaseVersion=$TAG -Prelease.newVersion=$NEXT_TAG-SNAPSHOT
fi

git checkout -b release-$TAG $TAG || git checkout release-$TAG

git push origin HEAD

git checkout master
