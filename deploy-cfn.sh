#!/bin/bash

STACK_NAME="dsql-cdc-pipeline"
REGION="us-east-2"

echo "Deploying CloudFormation stack: $STACK_NAME"

# Package Lambda code
echo "Packaging Lambda function..."
zip -q lambda.zip lambda_function.py

# Deploy stack
aws cloudformation deploy \
  --template-file cfn-template.yaml \
  --stack-name $STACK_NAME \
  --region $REGION \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    KinesisStreamArn=arn:aws:kinesis:us-east-2:771062417019:stream/dsql-cdc-stream \
    DataBucketName=dsql-cdc-processed-771062417019 \
    VectorBucketName=dsql-cdc-vectors \
    VectorIndexName=cdc-events

echo "Stack deployment complete!"
echo ""
echo "To view stack outputs:"
echo "aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION --query 'Stacks[0].Outputs'"
