plugins {
    idea
    java
    eclipse
    kotlin("jvm") version "1.3.41"
    id("net.researchgate.release") version "2.8.1"
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
