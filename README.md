# Chuckwalla2 Data Warehouse Project

## Dockerfile

This code is deployed using docker. Things should be workable without
docker but docker is the way this stuff is automated and so gotta get
that working eventually.

### Build

To build an image called chuckwalla2, within the same directory as
this file do:

```bash
docker build -t chuckwalla2 .
```

### Test

To be able to test the container, you'll need to run it with
credentials  so it can talk to s3:

```bash
docker run \
  -e AWS_ACCESS_KEY_ID=$(aws configure get default.aws_access_key_id) \
  -e AWS_SECRET_ACCESS_KEY=$(aws configure get default.aws_secret_access_key) \
  -e AWS_DEFAULT_REGION=$(aws configure get default.region) \
  -p 9000:8080 \
  chuckwalla2
```

To send a test query to the container run:
```bash
curl -XPOST \
  "http://localhost:9000/2015-03-31/functions/function/invocations" \
   -d '{ "etl" : "chuckwalla2.etl.nba.clean_games", "partition_date" : "2022-01-01", "production" : true }'
```

### Ship

Log in to the AWS docker registry

```bash
aws ecr-public get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin public.ecr.aws/x1p0z8n2
```

Tag the latest local image as the latest in the AWS chuckwalla2 repository:
```bash
docker tag chuckwalla2:latest public.ecr.aws/x1p0z8n2/chuckwalla2:latest
```

Upload the image:
```back
docker push public.ecr.aws/x1p0z8n2/chuckwalla2:latest       
```

