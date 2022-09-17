# Run this from the same directory that contains the Dockerfile
git pull
docker build -t chuckwalla2 .
docker run \
  -e AWS_ACCESS_KEY_ID=$(aws configure get default.aws_access_key_id) \
  -e AWS_SECRET_ACCESS_KEY=$(aws configure get default.aws_secret_access_key) \
  -e AWS_DEFAULT_REGION=$(aws configure get default.region) \
  chuckwalla2 \
  --time `date -u +%F`