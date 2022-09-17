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

Run the container with s3 credentials and a `--time` argument to see if it works. The
way this is all structured, this command is live and will write stuff to s3, so try to
make sure it's something sane. `--time` is the execution time, so this'll produce output
for the previous day:

```bash
docker run \
  -e AWS_ACCESS_KEY_ID=$(aws configure get default.aws_access_key_id) \
  -e AWS_SECRET_ACCESS_KEY=$(aws configure get default.aws_secret_access_key) \
  -e AWS_DEFAULT_REGION=$(aws configure get default.region) \
  chuckwalla2 \
  --time `date -u +%F`
```
