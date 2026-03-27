#!/usr/bin/env python3
"""Verify CDC ordering from Kinesis stream after load test."""

import argparse
import base64
import json
import time
from collections import defaultdict

import boto3


def read_all_records(stream_name, region, wait_secs=5):
    client = boto3.client("kinesis", region_name=region)
    shards = client.list_shards(StreamName=stream_name)["Shards"]
    all_records = []

    for shard in shards:
        shard_id = shard["ShardId"]
        resp = client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )
        iterator = resp["ShardIterator"]

        while iterator:
            out = client.get_records(ShardIterator=iterator, Limit=1000)
            for r in out["Records"]:
                try:
                    payload = json.loads(r["Data"])
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                payload["_shard"] = shard_id
                payload["_seq"] = r["SequenceNumber"]
                all_records.append(payload)

            if out["MillisBehindLatest"] == 0 and not out["Records"]:
                break
            iterator = out.get("NextShardIterator")
            if out["MillisBehindLatest"] == 0:
                # One more read to confirm no trailing records
                time.sleep(1)
                out2 = client.get_records(ShardIterator=iterator, Limit=1000)
                for r in out2["Records"]:
                    try:
                        payload = json.loads(r["Data"])
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    payload["_shard"] = shard_id
                    payload["_seq"] = r["SequenceNumber"]
                    all_records.append(payload)
                break

    return all_records


def verify_ordering(records):
    # Group by entity id
    by_id = defaultdict(list)
    for r in records:
        entity_id = None
        if r.get("after"):
            entity_id = r["after"].get("id")
        elif r.get("before"):
            entity_id = r["before"].get("id")
        if entity_id:
            by_id[entity_id].append(r)

    violations = 0
    for entity_id, events in by_id.items():
        for i in range(1, len(events)):
            prev_ts = events[i - 1].get("source", {}).get("ts_ns", events[i - 1].get("ts_ns", 0))
            curr_ts = events[i].get("source", {}).get("ts_ns", events[i].get("ts_ns", 0))
            if curr_ts < prev_ts:
                violations += 1
                print(f"  VIOLATION: {entity_id} event {i}: ts {curr_ts} < prev {prev_ts}")
                print(f"    prev: op={events[i-1]['op']} shard={events[i-1]['_shard']}")
                print(f"    curr: op={events[i]['op']} shard={events[i]['_shard']}")

    return violations, len(by_id)


def main():
    parser = argparse.ArgumentParser(description="Verify CDC ordering from Kinesis")
    parser.add_argument("--stream", required=True, help="Kinesis stream name")
    parser.add_argument("--region", default="us-east-2")
    args = parser.parse_args()

    print(f"Reading all records from {args.stream}...")
    records = read_all_records(args.stream, args.region)
    print(f"Total records: {len(records)}")

    # Count by op type
    ops = defaultdict(int)
    for r in records:
        ops[r["op"]] += 1
    print(f"Operations: {dict(ops)}")

    print("\nVerifying per-entity ordering...")
    violations, entity_count = verify_ordering(records)

    print(f"\nEntities tracked: {entity_count}")
    if violations == 0:
        print("PASS: All events are in order per entity.")
    else:
        print(f"FAIL: {violations} ordering violation(s) found.")


if __name__ == "__main__":
    main()
