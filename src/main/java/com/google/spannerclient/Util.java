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
import javax.net.ssl.SSLException;

class Util {

  private Util() {}

  static ManagedChannel buildManagedChannel(String target) {
    try {
      return NettyChannelBuilder.forTarget(target)
          .sslContext(GrpcSslContexts.forClient().build())
          .build();
    } catch (SSLException e) {
      // TODO(xjdr): Do something better here
      e.printStackTrace();
    }

    return null;
  }
}
