import net.researchgate.release.GitAdapter

buildscript {
  repositories {
    maven("https://plugins.gradle.org/m2/")
  }
}

plugins {
  eclipse
  idea
  jacoco
  java
  checkstyle
  pmd
  kotlin("jvm") version "1.3.41"
  id("net.researchgate.release") version "2.8.1"
  id("com.github.spotbugs") version "4.5.0"
  id("com.diffplug.spotless") version "5.1.1"
  id("org.sonarqube") version "2.8"
}

group = "com.google.retail"
version = "0.1.0-SNAPSHOT"

tasks {
  "wrapper"(Wrapper::class) {
    version = "5.6.0"
  }
}

allprojects {
  repositories {
    jcenter()
    google()
  }
}

release {
  val gitConfig = getProperty("git") as GitAdapter.GitConfig
  gitConfig.pushToRemote = "origin"
  gitConfig.pushOptions = listOf("--tags")
  gitConfig.requireBranch = "main"
}

subprojects {
  apply(plugin = "checkstyle")
  apply(plugin = "jacoco")
  apply(plugin = "java")
  apply(plugin = "pmd")
  apply(plugin = "com.github.spotbugs")
  apply(plugin = "com.diffplug.spotless")
  tasks.named<Test>("test") {
    reports.junitXml.isEnabled = true
  }

  val checkstyleConfig by configurations.creating

  dependencies {
    checkstyleConfig("com.puppycrawl.tools:checkstyle:8.35") {
      setTransitive(false)
    }

    compileOnly("com.github.spotbugs:spotbugs-annotations:4.1.2")
    spotbugs("com.github.spotbugs:spotbugs:4.1.2")
  }

  checkstyle {
    setToolVersion("8.35")
    setConfig(resources.text.fromArchiveEntry(configurations["checkstyleConfig"].first(), "google_checks.xml"))
  }

  tasks.jacocoTestReport {
    reports {
      xml.isEnabled = true
      html.isEnabled = true
    }
  }

  pmd {
    setConsoleOutput(true)
    setIgnoreFailures(true)
  }

  spotbugs {
    toolVersion.set("4.1.2")
    ignoreFailures.set(true)
    showStackTraces.set(true)
    showProgress.set(true)
    setEffort("default")
    setReportLevel("high")
    // onlyAnalyze.set(listOf("com.google.spez.*"))
    // jvmArgs.set(listOf("-Dfindbugs.debug=true"))
  }

  spotless {
    java {
      googleJavaFormat("1.7")
      licenseHeaderFile(rootProject.file("spotless.license.java"))
    }

    format("misc") {
      target("**/*.java")
    }
  }
}
