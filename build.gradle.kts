buildscript {
    repositories {
        maven("https://plugins.gradle.org/m2/")
    }
}

plugins {
    idea
    java
    eclipse
    kotlin("jvm") version "1.3.41"
    id("net.researchgate.release") version "2.8.1"
    id("org.jlleitschuh.gradle.ktlint") version "9.1.1"
}

group = "com.google.retail"
version = "0.1.0-SNAPSHOT"

tasks {
    "wrapper"(Wrapper::class) {
        version = "5.5.1"
    }
}

allprojects {
    repositories {
        jcenter()
        google()
    }
}

subprojects {
    apply(plugin = "org.jlleitschuh.gradle.ktlint") // Version should be inherited from parent
}
