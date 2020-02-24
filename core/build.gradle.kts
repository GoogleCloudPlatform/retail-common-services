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
    checkstyle
    id("com.github.spotbugs") version "2.0.1"
    id("com.google.protobuf") version "0.8.10"
    id("com.diffplug.gradle.spotless") version "3.24.0"
    id("net.ltgt.errorprone") version "0.8.1"
}

dependencies {
    implementation(project(":spannerclient"))
    implementation(Config.Libs.typesafe_config)
    implementation(Config.Libs.slf4j)
    implementation(Config.Libs.logback_classic)
    implementation(Config.Libs.logback_core)
    implementation(Config.Libs.protobuf)
    implementation(Config.Libs.grpc_core)
    implementation(Config.Libs.grpc_protobuf)
    implementation(Config.Libs.grpc_stub)
    implementation(Config.Libs.grpc_netty)
    implementation(Config.Libs.guava)
    implementation(Config.Libs.spanner)
    implementation(Config.Libs.pubsub)
    // implementation(Config.Libs.storage)
    implementation(Config.Libs.bigquery)
    // implementation(Config.Libs.rocksdb)

    // LMAX
    implementation("com.lmax:disruptor:3.4.2")

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
    compile("io.opencensus:opencensus-api:0.24.0")
    runtime("io.opencensus:opencensus-impl:0.24.0")
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

spotless {
    java {
        googleJavaFormat("1.7")
        licenseHeaderFile("../spotless.license.java")
    }

    format("misc") {
        target("**/*.java")
    }
}

tasks.withType<com.github.spotbugs.SpotBugsTask> {
    ignoreFailures = true
    exclude("$projectDir/gen")

    reports {
        xml.isEnabled = false
        html.isEnabled = true
    }
}
