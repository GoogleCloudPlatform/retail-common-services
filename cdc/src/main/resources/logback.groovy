/*
 * Copyright 2020 Google LLC
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
import ch.qos.logback.classic.filter.ThresholdFilter
jmxConfigurator()

// make changes for dev appender here
appender("DEV-CONSOLE", ConsoleAppender) {
  withJansi = true

  encoder(PatternLayoutEncoder) {
    pattern = "%-4relative [%thread] %-5level %logger{30} - %msg%n"
    outputPatternAsHeader = false
  }
}

// make changes for prod appender here
appender("PROD-CONSOLE", ConsoleAppender) {
  withJansi = true

}

// used for logging during test coverage
appender("DEVNULL", FileAppender) {
  file = "/dev/null"
  encoder(PatternLayoutEncoder) {
    pattern = "%-4relative [%thread] %-5level %logger{30} - %msg%n"
    outputPatternAsHeader = false
  }
}

appender("DEBUGGER", FileAppender) {
  file = "/tmp/spez.log"
  encoder(PatternLayoutEncoder) {
    pattern = "%-4relative [%thread] %-5level %logger{30} - %msg%n"
    outputPatternAsHeader = false
  }
}

def getLoglevel(property, default_value) {
  level = System.getProperty(property, "")
  if (level.equals("")) {
    return default_value
  }
  return toLevel(level)
}

log_level_default = getLoglevel("spez.loglevel.default", INFO)
log_level_cdc = getLoglevel("spez.loglevel.cdc", log_level_default)
log_level_core = getLoglevel("spez.loglevel.core", log_level_default)
log_level_netty = getLoglevel("spez.loglevel.netty", log_level_default)
log_level_spannerclient = getLoglevel("spez.loglevel.spannerclient", log_level_default)

def set_logger(name, default_level) {
  logger(name, getLoglevel("spez.loglevel." + name, default_level))
}

set_logger("com", log_level_default)
set_logger("com.google", log_level_default)
set_logger("com.google.spannerclient", log_level_spannerclient)
set_logger("com.google.spannerclient.Database", log_level_spannerclient)
set_logger("com.google.spannerclient.GrpcClient", OFF)
set_logger("com.google.spannerclient.Spanner", OFF)
set_logger("com.google.spannerclient.Util", log_level_spannerclient)
set_logger("com.google.spez", log_level_default)
set_logger("com.google.spez.cdc", log_level_cdc)
set_logger("com.google.spez.cdc.Main", log_level_cdc)
set_logger("com.google.spez.core", log_level_core)
set_logger("com.google.spez.core.EventPublisher", log_level_core)
set_logger("com.google.spez.core.SpannerTailer", log_level_core)
set_logger("com.google.spez.core.Spez", log_level_core)
set_logger("com.google.spez.core.RowProcessor", log_level_core)
set_logger("io", log_level_default)
set_logger("io.netty", log_level_netty)
set_logger("io.netty.buffer", log_level_netty)
set_logger("io.netty.buffer.AbstractByteBuf", log_level_netty)
set_logger("io.netty.buffer.ByteBufUtil", log_level_netty)
set_logger("io.netty.buffer.PoolThreadCache", log_level_netty)
set_logger("io.netty.buffer.PooledByteBufAllocator", log_level_netty)
set_logger("io.netty.channel", log_level_netty)
set_logger("io.netty.channel.DefaultChannelPipeline", OFF)
set_logger("io.netty.channel.MultithreadEventLoopGroup", log_level_netty)
set_logger("io.netty.channel.nio", log_level_netty)
set_logger("io.netty.channel.nio.NioEventLoop", log_level_netty)
set_logger("io.netty.handler", log_level_netty)
set_logger("io.netty.handler.ssl", log_level_netty)
set_logger("io.netty.handler.ssl.CipherSuiteConverter", OFF)
set_logger("io.netty.handler.ssl.OpenSsl", log_level_netty)
set_logger("io.netty.handler.ssl.OpenSslX509TrustManagerWrapper", log_level_netty)
set_logger("io.netty.handler.ssl.ReferenceCountedOpenSslClientContext", log_level_netty)
set_logger("io.netty.handler.ssl.ReferenceCountedOpenSslContext", log_level_netty)
set_logger("io.netty.util", log_level_netty)
set_logger("io.netty.util.Recycler", log_level_netty)
set_logger("io.netty.util.ResourceLeakDetector", log_level_netty)
set_logger("io.netty.util.ResourceLeakDetectorFactory", log_level_netty)
set_logger("io.netty.util.concurrent", log_level_netty)
set_logger("io.netty.util.concurrent.AbstractEventExecutor", log_level_netty)
set_logger("io.netty.util.concurrent.DefaultPromise", log_level_netty)
set_logger("io.netty.util.concurrent.DefaultPromise.rejectedExecution", log_level_netty)
set_logger("io.netty.util.concurrent.GlobalEventExecutor", log_level_netty)
set_logger("io.netty.util.concurrent.SingleThreadEventExecutor", log_level_netty)
set_logger("io.netty.util.internal", log_level_netty)
set_logger("io.netty.util.internal.CleanerJava9", log_level_netty)
set_logger("io.netty.util.internal.InternalThreadLocalMap", log_level_netty)
set_logger("io.netty.util.internal.NativeLibraryLoader", OFF)
set_logger("io.netty.util.internal.PlatformDependent", log_level_netty)
set_logger("io.netty.util.internal.PlatformDependent0", log_level_netty)
set_logger("io.netty.util.internal.SystemPropertyUtil", log_level_netty)
set_logger("io.netty.util.internal.logging", log_level_netty)
set_logger("io.netty.util.internal.logging.InternalLoggerFactory", log_level_netty)


switch (System.getProperty("PRICE-ENV")) {
  case "PROD":
    root(toLevel(System.getProperty("PRICE-LOGLEVEL"), WARN), ["PROD-CONSOLE"])
    break
  case "TEST-COVERAGE":
    root(ALL, ["DEVNULL"])
    break
  case "DEV":
  default:
    root(ALL, ["DEV-CONSOLE"])
    break
}

