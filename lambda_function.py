import json
import boto3
import base64
import os
from collections import defaultdict
from datetime import datetime

bedrock = boto3.client('bedrock-runtime')
s3vectors = boto3.client('s3vectors')
s3 = boto3.client('s3')


def get_entity_id(data):
    """Extract entity ID from CDC event."""
    if data.get('after'):
        return data['after'].get('id')
    if data.get('before'):
        return data['before'].get('id')
    return None


def sort_batch_by_ts(records):
    """Group by entity, sort each group by ts_ns, return ordered list."""
    by_entity = defaultdict(list)
    ungrouped = []

    for record in records:
        data = json.loads(base64.b64decode(record['kinesis']['data']))
        eid = get_entity_id(data)
        entry = {'record': record, 'data': data, 'ts_ns': data.get('source', {}).get('ts_ns', data.get('ts_ns', 0))}
        if eid:
            by_entity[eid].append(entry)
        else:
            ungrouped.append(entry)

    # Sort each entity's events by ts_ns
    ordered = []
    for eid, entries in by_entity.items():
        entries.sort(key=lambda e: e['ts_ns'])
        ordered.extend(entries)
    ordered.extend(ungrouped)
    return ordered


def process_record(record, data, vector_bucket, index_name, data_bucket):
    """Process a single CDC record."""
    source = data.get('source', {})
    table_name = source.get('table', 'unknown')
    schema_name = source.get('schema', 'public')

    op_map = {'c': 'INSERT', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ'}
    action = op_map.get(data.get('op', ''), 'unknown')

    text = json.dumps(data)
    response = bedrock.invoke_model(
        modelId='amazon.titan-embed-text-v2:0',
        body=json.dumps({"inputText": text})
    )

    result = json.loads(response['body'].read())
    vector_id = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{record['kinesis']['sequenceNumber']}"

    s3vectors.put_vectors(
        vectorBucketName=vector_bucket,
        indexName=index_name,
        vectors=[{
            'key': vector_id,
            'data': {'float32': result['embedding']},
            'metadata': {
                'timestamp': datetime.utcnow().isoformat(),
                'action': action,
                'table': table_name,
                'schema': schema_name
            }
        }]
    )

    s3.put_object(
        Bucket=data_bucket,
        Key=f"data/{vector_id}.json",
        Body=json.dumps({
            'timestamp': datetime.utcnow().isoformat(),
            'cdc_event': data
        })
    )


def handler(event, context):
    vector_bucket = os.environ['VECTOR_BUCKET']
    index_name = os.environ['INDEX_NAME']
    data_bucket = os.environ['S3_BUCKET']

    # Sort batch by ts_ns per entity before processing
    ordered = sort_batch_by_ts(event['Records'])

    for entry in ordered:
        process_record(entry['record'], entry['data'], vector_bucket, index_name, data_bucket)

    return {'statusCode': 200}
