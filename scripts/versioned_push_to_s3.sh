set -e

if [ -z $BUCKETNAME ]; then
    echo "You need to set BUCKETNAME=blah to say which s3 bucket to upload to e.g. s3://blah-bucket-prod";
    exit 1;
fi

if [ -z $PROFILENAME ]; then
    echo "You need to set PROFILENAME=blah to refer to the awscli credentials you want to use.";
    exit 1;
fi

if [ -z $SOURCE ]; then
    echo "You need to set SOURCE=blah to tell me what folder to copy.";
    exit 1;
fi

# Generate new folder name: yyyy-mm-dd-git_tag
FOLDERNAME=$(./scripts/get_version_string.sh)

# Upload generated files
# CSV files that are generated are all gzipped, so tag them that way so that
# the browser knows how to decode it when downloading from s3
aws s3 sync $SOURCE $BUCKETNAME/generated/$FOLDERNAME/ --include "*.csv" --content-encoding=gzip --profile $PROFILENAME
aws s3 sync $SOURCE $BUCKETNAME/generated/$FOLDERNAME/ --exclude "*.csv" --profile $PROFILENAME

# Copy in manually uploaded custom files to complete downloads
aws s3 sync $BUCKETNAME/custom/ $BUCKETNAME/generated/$FOLDERNAME/ --profile $PROFILENAME

# Remove original contents of the production folder because otherwise it keeps
# the metadata of the original files, which matters for gzip encoded files etc.
# https://github.com/aws/aws-cli/issues/319#issuecomment-25498909
aws s3 rm $BUCKETNAME/production/ --recursive --profile $PROFILENAME

# Update the production folder to what we just uploaded and make it public
aws s3 sync $BUCKETNAME/generated/$FOLDERNAME/ $BUCKETNAME/production/ --acl=public-read --delete --profile $PROFILENAME
