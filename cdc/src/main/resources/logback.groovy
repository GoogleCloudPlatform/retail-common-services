import ch.qos.logback.classic.filter.ThresholdFilter
jmxConfigurator()

// make changes for dev appender here
appender("DEV-CONSOLE", ConsoleAppender) {
  withJansi = true

  filter(ThresholdFilter) {
    level = INFO
  }
  encoder(PatternLayoutEncoder) {
    pattern = "%-4relative [%thread] %-5level %logger{30} - %msg%n"
    outputPatternAsHeader = false
  }
}

// make changes for prod appender here
appender("PROD-CONSOLE", ConsoleAppender) {
  withJansi = true

  filter(ThresholdFilter) {
    level = INFO
  }
  //encoder(LogstashEncoder)
}

// used for logging during test coverage
appender("DEVNULL", FileAppender) {
  file = "/dev/null"
  filter(ThresholdFilter) {
    level = DEBUG
  }
  encoder(PatternLayoutEncoder) {
    pattern = "%-4relative [%thread] %-5level %logger{30} - %msg%n"
    outputPatternAsHeader = false
  }
}

logger("io.netty.channel.DefaultChannelPipeline", OFF)
logger("io.netty.util.internal.NativeLibraryLoader", OFF)
logger("io.netty.handler.ssl.CipherSuiteConverter", OFF)

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


