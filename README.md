# Amazon DSQL CDC Demo

This demo showcases Amazon DSQL's Change Data Capture (CDC) feature, streaming real-time database changes to a downstream pipeline for semantic search and AI-powered querying.

## What This Demo Shows

1. **DSQL CDC in action**: Every insert, update, and delete in DSQL is automatically captured and streamed to Kinesis in real time
2. **Handling CDC ordering challenges**: Under high concurrency, CDC events can arrive slightly out of order. This demo shows how to detect and solve that
3. **Amazon S3 Vectors for embedding storage**: CDC events are converted to vector embeddings using Bedrock Titan and stored in S3 Vectors, the new purpose-built vector storage from AWS. No need to manage a separate vector database
4. **Real-time event processing**: A Lambda function consumes CDC events, generates embeddings, and stores them in S3 Vectors with metadata for filtering
5. **Semantic search chatbot**: Ask natural language questions about your data using a chatbot powered by Amazon Nova Lite and S3 Vectors

## Key AWS Services Showcased

| Service | Role in Demo |
|---------|-------------|
| **Amazon DSQL** | Source database with CDC enabled. Streams changes to Kinesis |
| **Amazon S3 Vectors** | Stores vector embeddings of CDC events with metadata. New AWS feature: serverless, no infrastructure to manage, up to 90% cheaper than traditional vector databases |
| **Amazon Kinesis** | Receives CDC event stream from DSQL |
| **AWS Lambda** | Processes CDC events in ordered batches |
| **Amazon Bedrock (Titan Embeddings V2)** | Generates 1024-dimension vector embeddings from CDC events |
| **Amazon Bedrock (Claude Sonnet 4)** | Powers the semantic search chatbot |

## Architecture

```
┌──────────┐     ┌─────────────────┐     ┌────────────────────┐     ┌──────────────┐
│  Amazon  │────▶│  Kinesis Stream  │────▶│  Lambda            │────▶│  S3 Vectors   │
│  DSQL    │ CDC │                  │     │  (Batch + Order)   │     │  (Embeddings) │
└──────────┘     └─────────────────┘     └────────┬───────────┘     └───────┬───────┘
                                                   │                         │
                                                   ▼                         ▼
                                            ┌──────────────┐        ┌────────────────┐
                                            │  S3 Bucket    │        │  Chatbot        │
                                            │  (Raw Events) │        │  (Nova Lite)    │
                                            └──────────────┘        └────────────────┘
```

## Demo Walkthrough

### Step 1: Prerequisites

- AWS CLI configured with credentials
- Python 3.9+ with `boto3` and `psycopg2-binary` (`pip install boto3 psycopg2-binary`)
- An S3 Vectors bucket with an index (bucket: `dsql-cdc-vectors`, index: `cdc-events`)

### Step 2: Set Up DSQL Cluster and CDC Infrastructure

#### 2.1 Set Environment Variables

```bash
export REGION="us-east-2"
export DSQL_ENDPOINT="https://dsql.${REGION}.api.aws"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
```

#### 2.2 Create a New DSQL Cluster

```bash
aws dsql create-cluster \
  --region ${REGION} \
  --endpoint-url ${DSQL_ENDPOINT}
```

Expected Output:

```json
{
    "identifier": "abc123xyz456",
    "arn": "arn:aws:dsql:us-east-2:123456789012:cluster/abc123xyz456",
    "status": "CREATING",
    "creationTime": "2025-01-15T10:30:00.000000+00:00",
    "deletionProtectionEnabled": true,
    "endpoint": "abc123xyz456.dsql.us-east-2.on.aws"
}
```

#### 2.3 Save the Cluster Identifier

```bash
export CLUSTER_ID="<your-cluster-identifier-from-output>"
```

Also write down the cluster identifier in your notes since you may need it later.

#### 2.4 Wait for Cluster to be Active

```bash
# Check cluster status (repeat until status is "ACTIVE")
aws dsql get-cluster \
  --identifier ${CLUSTER_ID} \
  --region ${REGION} \
  --endpoint-url ${DSQL_ENDPOINT} \
  --query 'status'
```

> **Note:** Cluster creation can take up to a minute.

### Step 3: Create a Kinesis Data Stream

#### 3.1 Create the Stream

```bash
export KINESIS_STREAM_NAME="dsql-cdc-stream"

aws kinesis create-stream \
  --stream-name ${KINESIS_STREAM_NAME} \
  --shard-count 1 \
  --region ${REGION}
```

#### 3.2 Wait for Stream to be Active

```bash
# Check stream status
aws kinesis describe-stream \
  --stream-name ${KINESIS_STREAM_NAME} \
  --region ${REGION} \
  --query 'StreamDescription.StreamStatus'
```

Wait until the status shows `"ACTIVE"`.

#### 3.3 Get the Stream ARN

```bash
export KINESIS_STREAM_ARN=$(aws kinesis describe-stream \
  --stream-name ${KINESIS_STREAM_NAME} \
  --region ${REGION} \
  --query 'StreamDescription.StreamARN' \
  --output text)

echo "Kinesis Stream ARN: ${KINESIS_STREAM_ARN}"
```

Also write down the Stream ARN in your notes since you may need it later.

### Step 4: Create an IAM Role for CDC

DSQL needs to assume an IAM role to publish CDC events to your Kinesis stream. This role must have a trust policy allowing DSQL to assume it, and permissions to write to Kinesis.

#### 4.1 Create the Trust Policy

```bash
export CDC_ROLE_NAME="dsql-cdc-kinesis-role"


# For Production, use dsql.amazonaws.com
export DSQL_SERVICE_PRINCIPAL="dsql.amazonaws.com"

cat > trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "${DSQL_SERVICE_PRINCIPAL}"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "aws:SourceAccount": "${ACCOUNT_ID}"
                },
                "ArnEquals": {
                    "aws:SourceArn": "arn:aws:dsql:*:${ACCOUNT_ID}:cluster/*"
                }
            }
        }
    ]
}
EOF
```


#### 4.2 Create the Permissions Policy

```bash
cat > permissions-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:PutRecord",
                "kinesis:PutRecords",
                "kinesis:DescribeStreamSummary",
                "kinesis:ListShards"
            ],
            "Resource": "${KINESIS_STREAM_ARN}"
        }
    ]
}
EOF
```

> **Note:** If your Kinesis stream uses a customer-managed KMS key, add the following to the policy:
> ```json
> {
>     "Effect": "Allow",
>     "Action": ["kms:GenerateDataKey"],
>     "Resource": "arn:aws:kms:${REGION}:${ACCOUNT_ID}:key/<your-key-id>",
>     "Condition": {
>         "StringEquals": {
>             "kms:ViaService": "kinesis.${REGION}.amazonaws.com"
>         }
>     }
> }
> ```

#### 4.3 Create the IAM Role with Appropriate Permissions

```bash
aws iam create-role \
  --role-name ${CDC_ROLE_NAME} \
  --assume-role-policy-document file://trust-policy.json

aws iam put-role-policy \
  --role-name ${CDC_ROLE_NAME} \
  --policy-name cdc-kinesis-policy \
  --policy-document file://permissions-policy.json
```

#### 4.4 Get the Role ARN

```bash
export CDC_ROLE_ARN=$(aws iam get-role \
  --role-name ${CDC_ROLE_NAME} \
  --query 'Role.Arn' \
  --output text)

echo "CDC Role ARN: ${CDC_ROLE_ARN}"
```

Also write down the CDC Role ARN in your notes since you may need it later.

### Step 5: Create the CDC Stream

Now we'll create the CDC stream that connects your DSQL cluster to your Kinesis stream.

#### 5.1 Create the Stream

```bash
aws dsql create-stream \
  --cluster-identifier ${CLUSTER_ID} \
  --target-definition "{\"kinesis\":{\"streamArn\":\"${KINESIS_STREAM_ARN}\",\"roleArn\":\"${CDC_ROLE_ARN}\"}}" \
  --endpoint-url ${DSQL_ENDPOINT} \
  --ordering UNORDERED \
  --region ${REGION} \
  --format JSON
```

Expected Output:

```json
{
    "clusterIdentifier": "abc123xyz456",
    "streamIdentifier": "xyz789def012",
    "arn": "arn:aws:dsql:us-east-2:123456789012:cluster/abc123xyz456/stream/xyz789def012",
    "status": "CREATING",
    "creationTime": "2025-01-15T10:45:00.000000+00:00",
    "ordering": "UNORDERED",
    "format": "JSON"
}
```

#### 5.2 Save the Stream Identifier

```bash
export STREAM_ID="<your-stream-identifier-from-output>"
```

Also write down the stream identifier in your notes since you may need it later.

#### 5.3 Wait for Stream to be Active

```bash
# Check stream status (repeat until status is "ACTIVE")
aws dsql get-stream \
  --cluster-identifier ${CLUSTER_ID} \
  --stream-identifier ${STREAM_ID} \
  --endpoint-url ${DSQL_ENDPOINT} \
  --region ${REGION} \
  --query 'status'
```

> **Note:** Stream creation typically takes 1-3 minutes.

### Step 6: Connect to PostgreSQL and Test

#### 6.1 Generate Authentication Token

```bash
export CLUSTER_HOST="${CLUSTER_ID}.dsql.${REGION}.on.aws"

export PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token \
  --hostname ${CLUSTER_HOST} \
  --region ${REGION})
```

#### 6.2 Connect to the Database

```bash
PGSSLMODE=require psql \
  -h ${CLUSTER_HOST} \
  -U admin \
  -d postgres
```

```bash
#create the table
psql "host=$CLUSTER_HOST port=5432 dbname=postgres user=admin sslmode=require" -f create_tables.sql
```

Alternative one-liner to replace 6.1 and 6.2:

```bash
PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token --hostname ${CLUSTER_ID}.dsql-gamma.${REGION}.on.aws --region ${REGION}) \
PGSSLMODE=require \
psql -h ${CLUSTER_ID}.dsql-gamma.${REGION}.on.aws -U admin -d postgres
```

### Step 7: Deploy the Processing Pipeline

The CloudFormation template deploys the Lambda function, IAM role, S3 bucket, and Kinesis event source mapping:

```bash
./deploy-cfn.sh
```

Or directly:

```bash
aws cloudformation deploy \
  --template-file cfn-template.yaml \
  --stack-name dsql-cdc-pipeline \
  --region us-east-2 \
  --capabilities CAPABILITY_NAMED_IAM
```

### Step 8: Insert Sample Data

Connect to your DSQL cluster and load sample data (customers, products, orders, reviews, users):

```bash
psql "host=$CLUSTER_HOST port=5432 dbname=postgres user=admin sslmode=require" \
  -f insert_data.sql
```

### Step 9: See CDC Events Flowing

Read directly from the Kinesis stream to see CDC events:

```bash
# List shards in the stream
aws kinesis list-shards \
  --stream-name ${KINESIS_STREAM_NAME} \
  --region ${REGION}
```

```bash
# Get a shard iterator
SHARD_ITERATOR=$(aws kinesis get-shard-iterator \
  --stream-name dsql-cdc-stream \
  --shard-id shardId-000000000000 \
  --shard-iterator-type TRIM_HORIZON \
  --region $REGION \
  --query 'ShardIterator' --output text)

# Fetch records
aws kinesis get-records --shard-iterator $SHARD_ITERATOR --region $REGION
```

Each CDC event looks like:

```json
{
    "op": "c",                          // "c" = create/insert
    "before": null,                     // null for inserts
    "after": {
        "id": "e17ca3fe-cf55-4b7f-8c71-1101a6fa1753",
        "name": "User471",
        "email": "user471@example.com"
    },
    "source": {
        "version": "1.0",
        "ts_ms": 1775074947790,
        "ts_ns": 1775074947790874837,   // transaction commit timestamp (nanoseconds)
        "txId": "kztvflmmvgu7q7vdvekt3zwtny",
        "schema": "public",
        "table": "users",
        "db": "postgres",
        "cluster": "xxxxxxasaegafada"
    }
}
```

- `op`: Operation type: `c` (create/insert/update),  `d` (delete)
- `before` / `after`: Row state before and after the change
- `source.ts_ns`: Transaction commit timestamp (nanoseconds since epoch)

### Step 10: Run a Load Test

Generate concurrent traffic to see CDC under pressure:

```bash
# Small test (122 events)
python3 dsql_load_test.py \
  --endpoint $CLUSTER_ENDPOINT \
  --rows 20 --updates 5 --deletes 2 --workers 10

# Full test (5505 events)
python3 dsql_load_test.py \
  --endpoint $CLUSTER_ENDPOINT \
  --rows 500 --updates 10 --deletes 5 --workers 20
```

### Step 11: Verify Lambda Is Processing

```bash
aws logs tail /aws/lambda/dsql-cdc --since 5m --region us-east-2 | grep "REPORT"
```

### Step 12: Query with the Chatbot

```bash
python3 chatbot.py "find the user with email updated-8-0024e4af"
python3 chatbot.py "list all customers from Denver"
python3 chatbot.py "what are the top rated products?"
python3 chatbot.py "show me all 5-star reviews"
```

### Step 13: Check Vectors in S3 Vectors

```bash
aws s3vectors list-vectors \
  --vector-bucket-name dsql-cdc-vectors \
  --index-name cdc-events \
  --max-results 5 \
  --return-metadata \
  --region us-east-2
```

## Challenge: CDC Event Ordering

### What We Observed

During our load test with 500 rows, 5000 updates, and 5 deletes (5505 total CDC events), we ran the ordering verifier:

```bash
python3 verify_cdc_order.py --stream dsql-cdc-stream
```

Results:

```
Total records:     6,226
Operations:        6,220 creates + 6 deletes
Entities tracked:  1,003
Violations found:  9 ordering violations
```

Out of 6,226 events, **9 arrived slightly out of order**, all within 22 milliseconds of each other. This happens because concurrent transactions on the same row can be committed and emitted in a slightly different order than their timestamps.

### Why It Matters

If you're replicating CDC events to a downstream database, an out-of-order event could overwrite newer data with older data:

```
T1: UPDATE email = 'old@example.com'  (ts=100)  → arrives SECOND
T2: UPDATE email = 'new@example.com'  (ts=200)  → arrives FIRST

Without ordering: Apply T2, then T1 overwrites → email = 'old@example.com' ✗
With ordering:    Sort by ts, apply T1 then T2  → email = 'new@example.com' ✓
```

### How We Solve It

**Batching**: The Lambda event source mapping collects records into batches:
- `BatchSize: 100`: up to 100 records per invocation
- `MaximumBatchingWindowInSeconds: 5`: waits up to 5 seconds to fill the batch

Since the observed violations were within 22ms, a 5-second window captures them all in the same batch.

**Per-entity sorting**: Before processing, the Lambda groups events by entity ID and sorts each group by `source.ts_ns`:

```python
def sort_batch_by_ts(records):
    by_entity = defaultdict(list)
    for record in records:
        data = json.loads(base64.b64decode(record['kinesis']['data']))
        eid = get_entity_id(data)
        source_ts = data.get('source', {}).get('ts_ns', data.get('ts_ns', 0))
        by_entity[eid].append({'record': record, 'data': data, 'ts_ns': source_ts})

    ordered = []
    for eid, entries in by_entity.items():
        entries.sort(key=lambda e: e['ts_ns'])
        ordered.extend(entries)
    return ordered
```

### Why `source.ts_ns`?

Each CDC event has two timestamps:

| Field | What it is | Consistent per transaction? |
|-------|-----------|---------------------------|
| `source.ts_ns` | When the transaction committed in DSQL | ✅ Yes, same for all rows in one transaction |
| Top-level `ts_ns` | When the CDC producer emitted the record | ❌ No, can differ if one transaction is split across producers |

We sort by `source.ts_ns` because it represents the true commit order.

## Files

| File | Purpose |
|------|---------|
| `README.md` | This file |
| `cfn-template.yaml` | CloudFormation template: Lambda, IAM, S3, event source mapping |
| `lambda_function.py` | Lambda source: batch ordering + Bedrock embeddings + S3 Vectors |
| `chatbot.py` | Semantic search chatbot powered by Nova Lite |
| `dsql_load_test.py` | Load test: concurrent inserts, updates, deletes against DSQL |
| `verify_cdc_order.py` | Reads Kinesis and verifies per-entity ordering by `source.ts_ns` |
| `insert_data.sql` | Sample data: customers, products, orders, reviews, users |
| `deploy-cfn.sh` | One-command CloudFormation deployment |

## Prerequisites

See [Step 1: Prerequisites](#step-1-prerequisites) above.

## Configuration

### Lambda Tuning

In `cfn-template.yaml`:
- `BatchSize` (default: 100): Higher = better ordering coverage, longer processing time
- `MaximumBatchingWindowInSeconds` (default: 5): Must exceed the max reordering gap
- `Timeout` (default: 300s): Each record takes ~200-500ms for Bedrock embedding

### Chatbot Model

In `chatbot.py`, change `MODEL_ID`:
```python
MODEL_ID = 'us.amazon.nova-lite-v1:0'              # Fast, cheap (default)
MODEL_ID = 'us.anthropic.claude-sonnet-4-20250514-v1:0'       # Better quality
```

## Cleanup

```bash
# Delete CloudFormation stack (Lambda, IAM role, S3 bucket, log group)
aws cloudformation delete-stack --stack-name dsql-cdc-pipeline --region us-east-2

# Delete Kinesis stream
aws kinesis delete-stream --stream-name dsql-cdc-stream --enforce-consumer-deletion --region us-east-2

# Delete DSQL CDC IAM role
aws iam delete-role-policy --role-name dsql-cdc-kinesis-role --policy-name cdc-kinesis-policy
aws iam delete-role --role-name dsql-cdc-kinesis-role
```
