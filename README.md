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


```
cd deploy
cloudshell_open --tutorial TUTORIAL.md

```
