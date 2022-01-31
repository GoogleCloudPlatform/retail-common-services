# Retail Common Services Foundation

## How to build

### local development

```
./gradlew build
```

### build a container for deployment

```
docker build .
```

## How to deploy

The reference deployment works best when run from google cloud-shell.

Clone this repo to your cloud-shell and run the following
```
cd deploy
cloudshell_open --tutorial TUTORIAL.md

```

or use this link to open the deploy tutorial in cloud-shell:

[![Open in Cloud Shell](https://gstatic.com/cloudssh/images/open-btn.svg)](https://ssh.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2Fretail-common-services&cloudshell_workspace=deploy&cloudshell_tutorial=TUTORIAL.md)

### deploying with a custom table

Once you've walked through the reference deployment you'll want to [read up](deploy/CUSTOM.md) on how to setup a custom table.
