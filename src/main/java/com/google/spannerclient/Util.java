/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.spannerclient;

import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Util {
  private static final Logger log = LoggerFactory.getLogger(Util.class);

  private Util() {}

  static ManagedChannel buildManagedChannel(String target) {
    final Map<String, Object> retryPolicy = new HashMap<>();
    retryPolicy.put("maxAttempts", 3D);
    retryPolicy.put("initialBackoff", "10s");
    retryPolicy.put("maxBackoff", "30s");
    retryPolicy.put("backoffMultiplier", 2D);
    retryPolicy.put("retryableStatusCodes", Arrays.<Object>asList("UNAVAILABLE"));
    Map<String, Object> methodConfig = new HashMap<>();
    Map<String, Object> name = new HashMap<>();
    name.put("service", "MyServer");
    methodConfig.put("name", Collections.<Object>singletonList(name));
    methodConfig.put("retryPolicy", retryPolicy);
    Map<String, Object> serviceConfig = new HashMap<>();
    serviceConfig.put("methodConfig", Collections.<Object>singletonList(methodConfig));

    try {
      return NettyChannelBuilder.forTarget(target)
          .sslContext(GrpcSslContexts.forClient().build())
          .enableRetry()
          .defaultServiceConfig(serviceConfig)
          .build();
    } catch (SSLException e) {
      // TODO(xjdr): Do something better here
      log.error("Error creating ManagedChannel: ", e);
    }

    return null;
  }
}
