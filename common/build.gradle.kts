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
    implementation(Config.Libs.groovy) // For logback
    // implementation(Config.Libs.protobuf)
    implementation(Config.Libs.grpc_core)
    // implementation(Config.Libs.grpc_protobuf)
    // implementation(Config.Libs.grpc_stub)
    // implementation(Config.Libs.grpc_netty)
    implementation(Config.Libs.guava)
    // implementation(Config.Libs.spanner)
    implementation(Config.Libs.pubsub)
    implementation(Config.Libs.typesafe_config)
    implementation(Config.Libs.guava)
    // implementation(Config.Libs.storage)
    // implementation(Config.Libs.rocksdb)
    implementation("io.opencensus:opencensus-exporter-trace-stackdriver:0.26.0")
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
