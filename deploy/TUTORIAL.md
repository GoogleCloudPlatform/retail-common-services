# Deploying the RCS example infrastructure

## Setup project

<walkthrough-project-setup billing="true"></walkthrough-project-setup>

## Run terraform setup script

This will create a bucket to store terraform state and initialize the core and example terraform modules.

```sh
./rcs-tf-setup.sh {{project-id}}

```

## Provision the RCS core components

This will `terraform plan` and `terraform apply` the rcs core module (terraform/spez-core).
Afterwards the following will be provisioned:

* `spanner-event-exporter` GKE cluster where the containers willrun
* `lpts` spanner table for tracking last processed timestamps
* `spez-ledger-topic` pubsub topic where events will be published
* `function-source` cloud storage bucket for storing cloud function source
* `archive` cloud function for storing events in GCS
* `lpts` cloud function for updating the last processed timestamp

```sh
./rcs-core-setup.sh {{project-id}}

```

## Provision the RCS example tailer

This will `terraform plan` and `terraform apply` the rcs example module (terraform/spez-example).
Afterwards the following will be provisioned:

* `spanner-event-exporter` container
* `example` spanner table for receiving events

```sh
./rcs-example-setup.sh {{project-id}} latest
```

## Run the python example

This will build and run a Docker container with a python script to demonstrate
the following:

* Inserting a single event into the `example` spanner table
* Waiting for the inserted event to be propagated to the `spez-ledger-topic`

```sh
./run-example-script.sh {{project-id}}
```

## Teardown the RCS example tailer

Now that we've successfully demonstrated the RCS example we can clean up the
cloud resources.

```sh
./rcs-example-teardown.sh {{project-id}}
```

## Teardown the RCS core components

If you have no further use for the RCS core components you may clean them up as
well.

```
./rcs-core-teardown.sh {{project-id}}
```
