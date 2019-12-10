import com.google.protobuf.gradle.*
import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.errorprone.CheckSeverity

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
    kotlin("jvm") version "1.3.50"
}

repositories {
    mavenCentral()
    google()
}

group = "com.google.spannerclient"
version = "0.1.0-SNAPSHOT"

tasks {
    "wrapper"(Wrapper::class) {
        version = "5.5.1"
    }
}


dependencies {
    implementation(Config.Libs.typesafe_config)
    implementation(Config.Libs.slf4j)
    implementation(Config.Libs.logback_classic)
    implementation(Config.Libs.logback_core)
    implementation(Config.Libs.protobuf)
    implementation(Config.Libs.grpc_core)
    implementation(Config.Libs.grpc_protobuf)
    implementation(Config.Libs.grpc_stub)
    implementation(Config.Libs.grpc_auth)
    implementation(Config.Libs.grpc_netty)
    implementation(Config.Libs.grpc_native)
    implementation(Config.Libs.guava)
    implementation(Config.Libs.gcp_core)

    // ---
    compileOnly("com.google.auto.value:auto-value-annotations:1.6.2")
    annotationProcessor("com.google.auto.value:auto-value:1.6.2")

    // ---
    protobuf(files("src/main/protos"))

    // ---
    compileOnly("com.google.code.findbugs:jsr305:3.0.2")
    annotationProcessor("com.uber.nullaway:nullaway:0.7.5")
    errorprone("com.google.errorprone:error_prone_core:2.3.3")
    errorproneJavac("com.google.errorprone:javac:9+181-r4173-1")

    if (JavaVersion.current().isJava9Compatible) {
        // Workaround for @javax.annotation.Generated
        // see: https://github.com/grpc/grpc-java/issues/3633
        compile("javax.annotation:javax.annotation-api:1.3.2")
    }
    testCompile(Config.Libs.junit_api)
    testRuntime(Config.Libs.junit_engine)

}

tasks.named<Test>("test") {
    useJUnitPlatform()
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
        licenseHeaderFile("spotless.license.java")
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

tasks.register<Jar>("spannerClientTest") {
    archiveClassifier.set("uber")
    manifest {
        attributes(mapOf(
                "Implementation-Title" to "Spanner Client Testin",
                "Implementation-Version" to "version",
                "Main-Class" to "com.google.spannerclient.Main"
        ))
    }
    baseName = "Main"
    appendix = "fat"

    from(sourceSets.main.get().output)
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
    })
}

idea {
    module {
        // proto files and generated Java files are automatically added as
        // source dirs.
        // If you have additional sources, add them here:
        sourceDirs.add(file("$projectDir/gen"))
    }
}
