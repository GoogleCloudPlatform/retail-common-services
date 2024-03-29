# Copyright (C) 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG JDK_VERSION=11-jdk-slim
ARG DISTROLESS_JAVA_VERSION=11
FROM openjdk:${JDK_VERSION} as build-env
WORKDIR /app
COPY *.gradle gradle.* gradlew /app/
COPY gradle /app/gradle
RUN /app/gradlew --version

FROM build-env as app-build
ADD . /app
WORKDIR /app
USER root
RUN ./gradlew clean :cdc:shadowJar && \
  mv cdc/build/libs/cdc-*-all.jar Main.jar

FROM gcr.io/distroless/java:${DISTROLESS_JAVA_VERSION} as prod
COPY --from=app-build /app/Main.jar /app/Main.jar
WORKDIR /app
ENTRYPOINT ["java"]
