import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.errorprone.CheckSeverity

plugins {
	  idea
    eclipse
	  java
	  checkstyle
    id("com.github.spotbugs") version "2.0.1"
	  id("com.diffplug.gradle.spotless") version "3.24.0"
    id("net.ltgt.errorprone") version "0.8.1"
}

dependencies {
	  implementation(project(":spannerclient"))
	  implementation(project(":core"))
	  implementation(Config.Libs.typesafe_config)
	  implementation(Config.Libs.slf4j)
	  implementation(Config.Libs.logback_classic)
	  implementation(Config.Libs.logback_core)
	  implementation(Config.Libs.groovy) // For logback
	  implementation(Config.Libs.protobuf)
	  implementation(Config.Libs.grpc_core)
	  implementation(Config.Libs.grpc_protobuf)
	  implementation(Config.Libs.grpc_stub)
	  implementation(Config.Libs.grpc_netty)
	  implementation(Config.Libs.guava)
	  implementation(Config.Libs.spanner)
	  implementation(Config.Libs.pubsub)
	  implementation(Config.Libs.storage)
	  implementation(Config.Libs.rocksdb)

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

    reports {
	      xml.isEnabled = false
	      html.isEnabled = true
    }
}


tasks.register<Jar>("spannerTailerService") {
	  archiveClassifier.set("uber")
	  manifest {
		    attributes(mapOf(
				               "Implementation-Title" to "Spanner Tailer Service",
				               "Implementation-Version" to "version",
				               "Main-Class" to "com.google.spez.cdc.Main"
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
