set -e

export BUCKETNAME=s3://datlas-colombia-downloads
export PROFILENAME=datlas-colombia-downloads-prod
export SOURCE=downloads/

./scripts/versioned_push_to_s3.sh
