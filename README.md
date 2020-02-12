Spanner Event Exporter
======================
*** WARNING: Immutable insterts only with OpCode and with a single primary key is
supported by default, any other implementation will probably work but will contain
dragons of various shapes and sizes lurking around each corner *** 


The Spanner Event Exporter is a library that will publish any updates to a
Cloud Spanner table as an Avro record to a pub/sub topic. This is a great
foundtation for creating an *Event* *Sourced* *System*. The library (called
`Spez` internally) also provides a Cloud Function called `Archiver` that will
be triggered by any write to pub/sub and will archive that record to Google
Cloud Storage. Additionally, there is an included Cloud Function called
`LastProcessedTimestamp` which will keep track of the last processed timestamp
and store that in a Cloud Spanner table. The `LastProcessedTimestamp` cloud
function is required to restart the tailer after an unexpected failure.

This is an example of how you might work with a `spez` record once it is on the
pub / sub queue. We include a working version of this in the `cdc` gradle project
included in this repo.

### Example:

```java
class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    final List<ListeningExecutorService> l =
        Spez.ServicePoolGenerator(12, "Spanner Tailer Event Worker");

    final SpannerTailer tailer = new SpannerTailer(1200000);
    final EventPublisher publisher = new EventPublisher("project-name", "topic-name");
    final Map<String, String> metadata = new HashMap<>();

    // Populate CDC Metadata
    metadata.put("test_key1", "test_val1");
    metadata.put("test_key2", "test_val2");

    final ResultSet resultSet =
        tailer.getSchema("project-name", "db-name", "instance-name", "table-name");

    final SpannerToAvro.SchemaSet schemaSet =
        SpannerToAvro.GetSchema("table-name", "avroNamespace", resultSet);

    final SpannerEventHandler handler =
        (bucket, s, ts) -> {
          final ListenableFuture<?> f =
              l.get(bucket)
                  .submit(
                      () -> {
                        Optional<ByteString> record = SpannerToAvro.MakeRecord(schemaSet, s);
                        publisher.publish(record.get(), attrs, ts);
                      });

          return Boolean.TRUE;
        };

    tailer.start(
        handler,
        l.size(),
        2,
        500,
        "project-name",
        "db-name",
        "instance-name",
        "table-name",
        "lpts_table",
        "2000",
        500,
        500);
  }
}

```

## Configuration

## Create a Spanner Table

In order to use this poller you must have a column named Timestamp that is not
null and contains the Spanner CommitTimestamp.

The poller will perform a full table scan on each poll interval. This will
consume resources on your db instance. Typically to help with this, you would
create a secondary index with the timestamp as the primary key. Do not do that
as it will cause hotspots. In this case, you may want to instead increase the
polling interval in order to address any excessive resource consumption on your
instance.

Do not use a commit timestamp column as the first part of the primary key of a
table or the first part of the primary key of a secondary index. Using a commit
timestamp column as the first part of a primary key creates hotspots and reduces
data performance, but performance issues may occur even with low write rates.
There is no performance overhead to enable the commit timestamps on non-key
columns that are not indexed.

[Review this document for more information on sharding Spanner CommitTimestamps](https://cloud.google.com/blog/products/gcp/sharding-of-timestamp-ordered-data-in-cloud-spanner)

Example:

```sql
CREATE TABLE spez_poller_table (
    ID INT64 NOT NULL,
    Color STRING(MAX),
    Name STRING(MAX),
    Timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ID)

```

## Create a Service Account

In order to allow `spez` to interact with the necessary Google Cloud resources,
you must create a service account for `spez` and give it the following
permissions:

The `spez` application requires the following permissions:

*   Tha ability to read data from you spanner instance.
*   The ability to publish messages to a pub/sub topic.
*   The ability to write trace data to Stackdriver Trace.

Create a service account for the `spez` application:

```bash
export PROJECT_ID=$(gcloud config get-value core/project)
export SERVICE_ACCOUNT_NAME="spez-service-account"

## Create the service account
gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} \
  --display-name "spez service account"

### Add the `spanner.databaseReader`, `pubsub.editor` and `cloudtrace.agent` IAM permissions to the spez service account:
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role='roles/spanner.databaseReader'

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role='roles/pubsub.editor'

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role='roles/cloudtrace.agent'

### Generate and download the `spez` service account:
gcloud iam service-accounts keys create \
  --iam-account "${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  service-account.json
```

## Deploying Spez

Spez is intended to be run on kubernetes. The deployment and service
yaml are provided in the kubernetes/ directory for each deployable directories.

```bash
# Deploy spez with the appropriate kubectl context
kubectl apply -f cdc/kubernetes/
```
## JMX Monitoring

For monitoring and debugging of the Spez poller, forward the JMX port (9010) to
your local PC via kubectl and then open jconsole or jVisualVM:

```bash
kubectl port-forward <your-app-pod> 9010

## Open jconsole connection to your local port 9010:
jconsole 127.0.0.1:9010
```

## JVM Tuning

https://chriswhocodes.com/vm-options-explorer.html

## Dynamic Logging

```
$ java -jar jmc/jmxterm-1.0.1-uber.jar
Welcome to JMX terminal. Type "help" for available commands.
$>jvms
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.cyclopsgroup.jmxterm.utils.WeakCastUtils$2 (file:/app/jmc/jmxterm-1.0.1-uber.jar) to method sun.tools.jconsole.LocalVirtualMachine.getAllVirtualMachines()
WARNING: Please consider reporting this to the maintainers of org.cyclopsgroup.jmxterm.utils.WeakCastUtils$2
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
1        (m) - Main.jar
79       ( ) - jmxterm-1.0.1-uber.jar
$>open 1
#Connection to 1 is opened
$>domain ch.qos.logback.classic
#domain is set to ch.qos.logback.classic
$>beans
#domain = ch.qos.logback.classic:
ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator
$>bean ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator
#bean is set to ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator
$>info
#mbean = ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator
#class name = ch.qos.logback.classic.jmx.JMXConfigurator
# attributes
  %0   - LoggerList (java.util.List, r)
  %1   - Statuses (java.util.List, r)
# operations
  %0   - java.lang.String getLoggerEffectiveLevel(java.lang.String p1)
  %1   - java.lang.String getLoggerLevel(java.lang.String p1)
  %2   - void reloadByFileName(java.lang.String p1)
  %3   - void reloadByURL(java.net.URL p1)
  %4   - void reloadDefaultConfiguration()
  %5   - void setLoggerLevel(java.lang.String p1,java.lang.String p2)
#there's no notifications
$>run setLoggerLevel com.google.spez.core.SpannerTailer DEBUG
#calling operation setLoggerLevel of mbean ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator with params [com.google.spez.core.SpannerTailer, DEBUG]
#operation returns:
null
$>run setLoggerLevel com.google.spez.core.SpannerTailer INFO
#calling operation setLoggerLevel of mbean ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator with params [com.google.spez.core.SpannerTailer, INFO]
#operation returns:
null
$>
```
