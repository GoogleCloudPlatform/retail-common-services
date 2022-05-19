# developer README

For local development / testing you will need to supply the correct environment
variables to the gradle run command.

```
GOOGLE_APPLICATION_CREDENTIALS=<PATH TO credentials.json> \
PROJECT_ID=<gcp project> \
SINK_INSTANCE=<spanner sink table instance> \
SINK_DATABASE=<spanner sink table database> \
SINK_TABLE=<spanner sink table> \
LPTS_INSTANCE=<spanner lpts table instance> \
LPTS_DATABASE=<spanner lpts table database> \
LPTS_TABLE=<spanner lpts table> \
DEFAULT_LOG_LEVEL=ERROR \
TIMESTAMP_COLUMN=<timestamp column in sink table i.e. CommitTimestamp> \
UUID_COLUMN=<uuid column in sink table i.e. uuid> \
./gradlew :cdc:run
```
