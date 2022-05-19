import com.google.protobuf.gradle.* // ktlint-disable no-wildcard-imports
import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone

buildscript {
  dependencies {
    classpath("com.google.guava:guava:28.1-jre")
  }
}

plugins {
  idea
  eclipse
  java
  id("com.google.protobuf") version "0.8.10"
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
  implementation(project(":common"))
  implementation(Config.Libs.typesafe_config)
  implementation(Config.Libs.slf4j)
  // implementation(Config.Libs.logback_classic)
  // implementation(Config.Libs.logback_core)
  implementation(Config.Libs.protobuf)
  implementation(Config.Libs.grpc_core)
  implementation(Config.Libs.grpc_protobuf)
  implementation(Config.Libs.grpc_stub)
  implementation(Config.Libs.grpc_netty)
  implementation(Config.Libs.grpc_native)
  implementation(Config.Libs.guava)
  // implementation(Config.Libs.spanner)
  implementation(Config.Libs.pubsub)
  // implementation(Config.Libs.storage)
  implementation(Config.Libs.bigquery)
  // implementation(Config.Libs.rocksdb)
  implementation("com.google.api.grpc:proto-google-cloud-spanner-v1:1.55.1")
  // implementation(Config.Libs.spanner)
  // custom spannerclient implementation
  implementation("com.google.spannerclient:spannerclient:0.1.8")
  // BOM needed for cloud-spanner
  implementation(platform("com.google.cloud:libraries-bom:25.3.0"))
  //implementation("com.google.cloud:google-cloud-spanner")
  implementation("com.google.cloud:google-cloud-spanner:6.24.0")



  // AutoValue
  compileOnly("com.google.auto.value:auto-value-annotations:1.6.2")
  annotationProcessor("com.google.auto.value:auto-value:1.6.2")

  // Avro
  implementation("org.apache.avro:avro:1.8.2")
  implementation("io.netty:netty-buffer:4.1.33.Final")

  // Protobuf
  protobuf(files("src/main/protos"))

  // Static Analysis
  compileOnly("com.google.code.findbugs:jsr305:3.0.2")
  annotationProcessor("com.uber.nullaway:nullaway:0.7.5")
  errorprone("com.google.errorprone:error_prone_core:2.3.3")
  errorproneJavac("com.google.errorprone:javac:9+181-r4173-1")

  // OpenCensus core
  compile("io.opencensus:opencensus-api:0.28.3")
  compile("io.opencensus:opencensus-contrib-exemplar-util:0.28.3")
  runtime("io.opencensus:opencensus-impl:0.28.3")

  // Junit and friends
  testImplementation("org.junit.jupiter:junit-jupiter-engine:5.6.2")
  testImplementation("org.junit.platform:junit-platform-engine:1.6.2")
  testImplementation("org.mockito:mockito-core:3.5.2")
  testImplementation("org.mockito:mockito-inline:3.5.2")
  testImplementation("org.mockito:mockito-junit-jupiter:3.5.2")
  testImplementation("org.assertj:assertj-core:3.16.1")
  testImplementation("com.google.cloud:google-cloud-spanner:2.0.2")
  testImplementation(Config.Libs.logback_classic)
  testImplementation(Config.Libs.logback_core)
  testImplementation(Config.Libs.groovy) // For logback
}

tasks.test {
  useJUnitPlatform()
  testLogging {
    events("passed", "skipped", "failed")
  }
  environment["GOOGLE_APPLICATION_CREDENTIALS"] = System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
  environment["GOOGLE_CLOUD_PROJECT"] = System.getenv("GOOGLE_CLOUD_PROJECT")
  //systemProperties["gcp.credentials.file"] = project.properties["gcp.credentials.file"]
}

// ErrorProne
tasks.withType<JavaCompile>().configureEach {
  options.compilerArgs.add("-Xlint:unchecked")
  options.isDeprecation = true
  options.errorprone.excludedPaths.set(".*/gen/.*")
  options.errorprone.disableWarningsInGeneratedCode.set(true)

  if (!name.toLowerCase().contains("test")) {
    options.errorprone {
      check("NullAway", CheckSeverity.ERROR)
      option("NullAway:AnnotatedPackages", "com.uber")
    }
  }
}

// Protobuf
protobuf {
  protoc {
    // The artifact spec for the Protobuf Compiler
    artifact = "com.google.protobuf:protoc:3.9.1"
  }
  plugins {
    // Optional: an artifact spec for a protoc plugin, with "grpc" as
    // the identifier, which can be referred to in the "plugins"
    // container of the "generateProtoTasks" closure.
    id("grpc") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.22.1"
    }
  }
  generateProtoTasks {
    ofSourceSet("main").forEach {
      it.plugins {
        // Apply the "grpc" plugin whose spec is defined above, without options.
        id("grpc")
      }
    }
  }

  generatedFilesBaseDir = "$projectDir/gen"
}

tasks.register("cleanProtos").configure {
  delete("$projectDir/gen")
}
