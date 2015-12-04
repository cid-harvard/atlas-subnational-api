set -e
aws s3 sync downloads/ s3://datlas-mexico-downloads-prod/ --content-encoding=gzip --acl=public-read --profile datlas-mexico-downloads-prod
