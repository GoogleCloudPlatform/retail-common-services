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
RUN apt-get update
RUN apt-get install -y build-essential

FROM build-env as app-build
ADD . /app
WORKDIR /app
USER root
RUN ./gradlew clean :cdc:spannerTailerService && \
  mv cdc/build/libs/Main-fat-*.jar Main.jar
RUN make -C ./scripts

FROM gcr.io/distroless/java:${DISTROLESS_JAVA_VERSION} as prod
COPY --from=app-build /app/Main.jar /app/Main.jar
COPY --from=app-build /app/scripts/run-spez /app/run-spez
ENV JVM_HEAP_SIZE=12g
ENV JAVA_TOOL_OPTIONS="-Xmx${JVM_HEAP_SIZE}"
ADD cdc/docker/jvm-arguments /app/
WORKDIR /app
ENTRYPOINT ["/app/run-spez"]
