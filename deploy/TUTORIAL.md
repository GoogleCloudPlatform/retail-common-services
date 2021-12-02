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
./rcs-example-setup.sh {{project-id}}
```

