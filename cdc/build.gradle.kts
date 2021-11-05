import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone

plugins {
  application
  idea
  eclipse
  java
  id("net.ltgt.errorprone") version "0.8.1"
  // id("com.google.cloud.artifactregistry.gradle-plugin") version "2.1.0"
}

repositories {
  maven {
    url = uri("file://${rootProject.projectDir}/libs/maven")
  }
  mavenCentral()
  google()
  // maven("artifactregistry://us-maven.pkg.dev/retail-common-services-249016/spez-maven-repo")
}

dependencies {
  implementation(project(":core"))
  implementation(project(":common"))

  implementation(Config.Libs.typesafe_config)
  implementation(Config.Libs.slf4j)
  implementation(Config.Libs.logback_classic)
  implementation(Config.Libs.logback_core)

  implementation(Config.Libs.groovy) // For logback
  // implementation(Config.Libs.protobuf)
  implementation(Config.Libs.grpc_core)
  // implementation(Config.Libs.grpc_protobuf)
  // implementation(Config.Libs.grpc_stub)
  // implementation(Config.Libs.grpc_netty)
  implementation(Config.Libs.guava)
  // implementation(Config.Libs.spanner)
  implementation(Config.Libs.pubsub)
  // implementation(Config.Libs.storage)
  // implementation(Config.Libs.rocksdb)
  implementation("io.opencensus:opencensus-exporter-trace-stackdriver:0.28.3")
  implementation("io.opencensus:opencensus-contrib-zpages:0.28.3")

  // AutoValue
  compileOnly("com.google.auto.value:auto-value-annotations:1.6.2")
  annotationProcessor("com.google.auto.value:auto-value:1.6.2")

  // ---
  compileOnly("com.google.code.findbugs:jsr305:3.0.2")
  annotationProcessor("com.uber.nullaway:nullaway:0.7.5")
  errorprone("com.google.errorprone:error_prone_core:2.3.3")
  errorproneJavac("com.google.errorprone:javac:9+181-r4173-1")
}

// ErrorProne
tasks.withType<JavaCompile>().configureEach {
  options.errorprone.excludedPaths.set(".*/gen/.*")
  options.errorprone.disableWarningsInGeneratedCode.set(true)

  if (!name.toLowerCase().contains("test")) {
    options.errorprone {
      check("NullAway", CheckSeverity.ERROR)
      option("NullAway:AnnotatedPackages", "com.uber")
    }
  }
}

tasks.register<Jar>("spannerTailerService") {
  archiveClassifier.set("uber")
  manifest {
    attributes(
      mapOf(
        "Implementation-Title" to "Spanner Tailer Service",
        "Implementation-Version" to "version",
        "Main-Class" to "com.google.spez.cdc.Main"
      )
    )
  }
  baseName = "Main"
  appendix = "fat"

  from(sourceSets.main.get().output)
  dependsOn(configurations.runtimeClasspath)
  from({
    configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
  })
}

val project_id = System.getenv().getOrDefault("PROJECT_ID", "rcs-demo-prod")

application {
  mainClassName = "com.google.spez.cdc.Main"
  applicationDefaultJvmArgs = listOf(
    "-Dspez.auth.cloud_secrets_dir=${rootProject.projectDir}/secrets",
    "-Dspez.project_id=$project_id",
    "-Dspez.auth.credentials=credentials.json",
    "-Dspez.pubsub.topic=spez-ledger-topic",
    "-Dspez.sink.instance=example-event-sink-instance",
    "-Dspez.sink.database=example-event-sink-database",
    "-Dspez.sink.table=example",
    "-Dspez.sink.uuid_column=uuid",
    "-Dspez.sink.timestamp_column=CommitTimestamp",
    "-Dspez.lpts.instance=spez-lpts-instance",
    "-Dspez.lpts.database=spez-lpts-database",
    "-Dspez.lpts.table=lpts",
    "-Dspez.loglevel.default=INFO",
    "-Dspez.loglevel.com.google.spez.core.EventPublisher=INFO",
    "-Djava.net.preferIPv4Stack=true",
    "-Dio.netty.allocator.type=pooled",
    "-XX:+UnlockExperimentalVMOptions",
    "-XX:+UseZGC",
    "-Xlog:gc:mylog.log*",
    "-XX:ConcGCThreads=4",
    "-XX:+UseTransparentHugePages",
    "-XX:+UseNUMA",
    "-XX:+UseStringDeduplication",
    "-XX:+HeapDumpOnOutOfMemoryError",
    "-Dcom.sun.management.jmxremote",
    "-Dcom.sun.management.jmxremote.port=9010",
    "-Dcom.sun.management.jmxremote.rmi.port=9010",
    "-Dcom.sun.management.jmxremote.local.only=false",
    "-Dcom.sun.management.jmxremote.authenticate=false",
    "-Dcom.sun.management.jmxremote.ssl=false",
    "-Djava.rmi.server.hostname=127.0.0.1"
  )
}
