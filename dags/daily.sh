#!/bin/bash
# Run this from the same directory that contains the Dockerfile
run_date=$(date -u +%F)
mkdir -p logs
log_file="logs/$run_date"
echo "Logging daily dag to $log_file"
git pull >& $log_file
docker build -t chuckwalla2 . >& $log_file
docker run \
  -e AWS_ACCESS_KEY_ID="$(aws configure get default.aws_access_key_id)" \
  -e AWS_SECRET_ACCESS_KEY="$(aws configure get default.aws_secret_access_key)" \
  -e AWS_DEFAULT_REGION="$(aws configure get default.region)" \
  chuckwalla2 \
  --time "$run_date" >& $log_file
