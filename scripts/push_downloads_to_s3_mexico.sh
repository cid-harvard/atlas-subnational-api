set -e

export BUCKETNAME=s3://datlas-mexico-downloads-prod
export PROFILENAME=datlas-mexico-downloads-prod
export SOURCE=downloads/

./scripts/versioned_push_to_s3.sh
