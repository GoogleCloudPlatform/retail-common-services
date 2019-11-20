object  Config {
    object Version {
	val protobuf_version = "3.8.0"
	val grpc_version = "1.22.0"
	val netty_version = "4.1.37.Final"
	val netty_boring_ssl_version = "2.0.25.Final"
	val slf4j_version = "1.7.25"
	val type_safe_config_version = "1.3.1"
	val logback_version = "1.2.3"
	val logback_appender_version = "1.4.2"
	val logstash_encoder_version = "5.0"
	val groovy_version = "2.5.8"
	val rocks_version = "6.0.1"
	val guava_version = "28.0.1-jre"
	val spanner_version = "1.44.0"
	val pubsub_version = "1.99.0"
	val storage_version = "1.99.0"
	val bigquery_version = "1.99.0"
    }

    object Libs {
	// Logging and Config
	val typesafe_config = "com.typesafe:config:".plus(Version.type_safe_config_version)
	val slf4j = "org.slf4j:jul-to-slf4j:".plus(Version.slf4j_version)
	val logback_classic = "ch.qos.logback:logback-classic:".plus(Version.logback_version)
	val logback_core = "ch.qos.logback:logback-core:".plus(Version.logback_version)
	val protobuf = "com.google.protobuf:protobuf-java:".plus(Version.protobuf_version)
	val groovy = "org.codehaus.groovy:groovy-all:".plus(Version.groovy_version)

	// gRPC
	val grpc_core = "io.grpc:grpc-core:".plus(Version.grpc_version)
	val grpc_protobuf = "io.grpc:grpc-protobuf:".plus(Version.grpc_version)
	val grpc_stub = "io.grpc:grpc-stub:".plus(Version.grpc_version)
	val grpc_netty = "io.grpc:grpc-netty:".plus(Version.grpc_version)

	// Google
	val guava = "com.google.guava:guava:".plus(Version.guava_version)
	val spanner = "com.google.cloud:google-cloud-spanner:".plus(Version.spanner_version)
	val pubsub = "com.google.cloud:google-cloud-pubsub:".plus(Version.pubsub_version)
	val storage = "com.google.cloud:google-cloud-storage:".plus(Version.storage_version)
        val bigquery  = "com.google.cloud:google-cloud-bigquery:".plus(Version.bigquery_version)

	// DB
	val rocksdb = "org.rocksdb:rocksdbjni:".plus(Version.rocks_version)
    }
}
