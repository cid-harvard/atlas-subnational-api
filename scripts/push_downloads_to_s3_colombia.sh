set -e
aws s3 sync downloads/ s3://datlas-colombia-downloads/ --content-encoding=gzip --acl=public-read --profile datlas-colombia-downloads-prod
