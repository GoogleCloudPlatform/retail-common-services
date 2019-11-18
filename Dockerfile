# Copyright (C) 2019 Google LLC.
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

FROM gradle:latest AS build-env
ADD . /app
WORKDIR /app
USER root
RUN ./gradlew build && \
  mv build/libs/spannerclient-1.0-SNAPSHOT.jar app.jar

FROM gcr.io/distroless/java
COPY --from=build-env /app/app.jar /app/app.jar
## This line is for local testing and should be commented out !!!
ADD secrets /var/run/secret/cloud.google.com
ENV GOOGLE_APPLICATION_CREDENTIALS /var/run/secret/cloud.google.com/service-account.json
WORKDIR /app
ENTRYPOINT ["java", \
  "-ea", \
  "-Djava.net.preferIPv4Stack=true", \
  "-Dio.netty.allocator.type=pooled", \
  "-XX:+UseStringDeduplication", \
  "-XX:+UseG1GC",                \
  "-Dcom.sun.management.jmxremote", \
  "-Dcom.sun.management.jmxremote.port=9010", \
  "-Dcom.sun.management.jmxremote.rmi.port=9010", \
  "-Dcom.sun.management.jmxremote.local.only=false",\
  "-Dcom.sun.management.jmxremote.authenticate=false",\
  "-Dcom.sun.management.jmxremote.ssl=false",\
  "-Djava.rmi.server.hostname=127.0.0.1",\
  "-jar", \
  "app.jar"]
