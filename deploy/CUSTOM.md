
# Deploying the RCS with a custom table

To setup RCS with a custom table you'll need to have deployed the core components as outlined
in the [reference deployment](TUTORIAL.md)

## Copy the example terraform

You can name your copy anything that you'd like, here we are creating a folder named `order-events`
Please make sure to keep your copy in the terraform folder otherwise the `rcs-custom-setup.sh` script
will not be able to find it.

```sh
cp -Rv terraform/spez-example terraform/order-events
```

## Update the example DDL with your schema

edit `terraform/order-events/sink.tf`

Find the sink_ddl block and customize it with your schema.
BE SURE TO LEAVE THE `uuid` AND `CommitTimestamp` COLUMNS IN YOUR SCHEMA.

```
  sink_ddl = <<-EOT
    CREATE TABLE ${var.sink_table} (
      uuid STRING(MAX) NOT NULL,
      CommitTimestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
    ) primary key (uuid)
  EOT
```

## Update the table name

edit `terraform/order-events/tailer.auto.tfvars`

Change the `sink_table` variable to your custom table name

## Update the terraform backend prefix

edit `terraform/order-events/tailer.tf`

Find the backend block and update the prefix to something other than `spez-example-terraform-state`

```
terraform {
  required_version = ">= 0.12.0"
  backend "gcs" {
    prefix  = "spez-example-terraform-state"
  }
}
```

## Provision the RCS custom tailer

This will `terraform init`, `terraform plan` and `terraform apply` the order-events module (terraform/order-events).
Afterwards the following will be provisioned:

* `spanner-event-exporter` container
* `order-events` spanner table for receiving events

```sh
./rcs-custom-setup.sh order-events {{project-id}} latest
```

## Teardown the RCS example tailer

When you'd like to can clean up the cloud resources in the custom module you can run the following

```sh
./rcs-example-teardown.sh order-events {{project-id}}
```
