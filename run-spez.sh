export PROJECT_ID="retail-common-services-249016"
export SINK_INSTANCE="repro-instance"
export SINK_DATABASE="repro-database"
export SINK_TABLE="Transactions10Million"
export SINK_TABLE="Transactions100Million"
export LPTS_INSTANCE="repro-instance"
export LPTS_DATABASE="repro-database"
export LPTS_TABLE="lpts"
export DEFAULT_LOG_LEVEL="INFO"
export TIMESTAMP_COLUMN="commit_timestamp"
export UUID_COLUMN="uuid"

./gradlew run
