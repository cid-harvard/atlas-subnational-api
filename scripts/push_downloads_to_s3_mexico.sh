set -e
read -p "Paste in the SHA of the commit that generated these downloads, to name the s3 bucket: " s3_sha
aws s3 sync downloads/ s3://datlas-mexico-downloads-prod/$s3_sha/ --content-encoding=gzip --acl=public-read --profile datlas-mexico-downloads-prod
