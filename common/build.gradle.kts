plugins {
    java
    idea
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(Config.Libs.typesafe_config)
    implementation(Config.Libs.slf4j)
    implementation(Config.Libs.logback_classic)
    implementation(Config.Libs.logback_core)
    implementation(Config.Libs.guava)
    implementation(Config.Libs.typesafe_config)
    implementation("io.opencensus:opencensus-exporter-trace-logging:0.28.3")
    implementation("io.opencensus:opencensus-exporter-trace-stackdriver:0.28.3")
    implementation("io.opencensus:opencensus-exporter-stats-stackdriver:0.28.3")
    implementation("io.opencensus:opencensus-contrib-zpages:0.26.0")
    implementation("com.google.oauth-client:google-oauth-client:1.31.0")

    // AutoValue
    compileOnly("com.google.auto.value:auto-value-annotations:1.6.2")
    annotationProcessor("com.google.auto.value:auto-value:1.6.2")

    // ---
    compileOnly("com.google.code.findbugs:jsr305:3.0.2")
    annotationProcessor("com.uber.nullaway:nullaway:0.7.5")

    testCompile("junit", "junit", "4.12")
}
