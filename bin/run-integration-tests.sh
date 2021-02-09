INTEGRATION_TESTS=true GOOGLE_CLOUD_PROJECT=spanner-event-exporter GOOGLE_APPLICATION_CREDENTIALS=$PWD/secrets/credentials.json ./gradlew --include-build ../spannerclient clean build
