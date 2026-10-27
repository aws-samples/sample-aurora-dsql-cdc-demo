#!/bin/bash
set -euo pipefail

STACK_NAME="${STACK_NAME:-dsql-cdc-pipeline}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region)}}"
if [ -z "${REGION:-}" ]; then
  echo "ERROR: AWS region not set. Export AWS_REGION or run 'aws configure'." >&2
  exit 1
fi

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

KINESIS_STREAM_NAME="${KINESIS_STREAM_NAME:-dsql-cdc-stream}"
DATA_BUCKET_NAME="${DATA_BUCKET_NAME:-dsql-cdc-processed-${ACCOUNT_ID}}"
VECTOR_BUCKET_NAME="${VECTOR_BUCKET_NAME:-dsql-cdc-vectors}"
VECTOR_INDEX_NAME="${VECTOR_INDEX_NAME:-cdc-events}"
RESOURCE_PREFIX="${RESOURCE_PREFIX:-dsql-cdc}"

echo "Deploying CloudFormation stack: $STACK_NAME (region=$REGION, account=$ACCOUNT_ID)"

# Package Lambda code
echo "Packaging Lambda function..."
zip -q lambda.zip lambda_function.py

# Deploy stack
aws cloudformation deploy \
  --template-file cfn-template.yaml \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    "KinesisStreamName=$KINESIS_STREAM_NAME" \
    "DataBucketName=$DATA_BUCKET_NAME" \
    "VectorBucketName=$VECTOR_BUCKET_NAME" \
    "VectorIndexName=$VECTOR_INDEX_NAME" \
    "ResourcePrefix=$RESOURCE_PREFIX"

echo "Stack deployment complete!"
echo ""
echo "To view stack outputs:"
echo "aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION --query 'Stacks[0].Outputs'"
