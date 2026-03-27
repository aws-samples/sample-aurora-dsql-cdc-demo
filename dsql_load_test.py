#!/usr/bin/env python3
"""DSQL CDC Load Test - Writes concurrent operations to a DSQL table."""

import argparse
import concurrent.futures
import json
import time
import uuid

import boto3
import psycopg2
import psycopg2.errors
from psycopg2 import sql


def get_dsql_token(region, endpoint):
    client = boto3.client("dsql", region_name=region)
    return client.generate_db_connect_admin_auth_token(endpoint, region)


def get_connection(endpoint, region, token):
    return psycopg2.connect(
        host=endpoint,
        port=5432,
        user="admin",
        password=token,
        dbname="postgres",
        sslmode="require",
    )


def retry(fn, max_retries=5):
    for attempt in range(max_retries):
        try:
            return fn()
        except psycopg2.errors.SerializationFailure:
            if attempt == max_retries - 1:
                raise
            time.sleep(0.1 * (2 ** attempt))


ALLOWED_TABLES = {"users", "customers", "products", "orders", "reviews"}


def _validate_table(table):
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Invalid table name: {table}. Allowed: {ALLOWED_TABLES}")


def insert_row(endpoint, region, token, table, row_id):
    _validate_table(table)
    def do():
        conn = get_connection(endpoint, region, token)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            sql.SQL("INSERT INTO {} (id, name, email) VALUES (%s, %s, %s)").format(sql.Identifier(table)),
            (row_id, f"user-{row_id[:8]}", f"user-{row_id[:8]}@example.com"),
        )
        cur.close()
        conn.close()
    retry(do)
    return ("insert", row_id)


def update_row(endpoint, region, token, table, row_id, seq):
    _validate_table(table)
    def do():
        conn = get_connection(endpoint, region, token)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            sql.SQL("UPDATE {} SET email = %s WHERE id = %s").format(sql.Identifier(table)),
            (f"updated-{seq}-{row_id[:8]}@example.com", row_id),
        )
        cur.close()
        conn.close()
    retry(do)
    return ("update", row_id, seq)


def delete_row(endpoint, region, token, table, row_id):
    _validate_table(table)
    def do():
        conn = get_connection(endpoint, region, token)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            sql.SQL("DELETE FROM {} WHERE id = %s").format(sql.Identifier(table)),
            (row_id,),
        )
        cur.close()
        conn.close()
    retry(do)
    return ("delete", row_id)


def run_load_test(endpoint, region, table, num_rows, num_updates, num_deletes, workers):
    token = get_dsql_token(region, endpoint)
    row_ids = [str(uuid.uuid4()) for _ in range(num_rows)]

    print(f"Inserting {num_rows} rows with {workers} workers...")
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(insert_row, endpoint, region, token, table, rid) for rid in row_ids]
        for f in concurrent.futures.as_completed(futures):
            f.result()
    print(f"  Inserts done in {time.time() - t0:.2f}s")

    if num_updates > 0:
        print(f"Updating {num_updates} rows per record ({num_rows * num_updates} total)...")
        t0 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = []
            for rid in row_ids:
                for seq in range(num_updates):
                    futures.append(pool.submit(update_row, endpoint, region, token, table, rid, seq))
            for f in concurrent.futures.as_completed(futures):
                f.result()
        print(f"  Updates done in {time.time() - t0:.2f}s")

    delete_ids = row_ids[:num_deletes]
    if delete_ids:
        print(f"Deleting {len(delete_ids)} rows...")
        t0 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(delete_row, endpoint, region, token, table, rid) for rid in delete_ids]
            for f in concurrent.futures.as_completed(futures):
                f.result()
        print(f"  Deletes done in {time.time() - t0:.2f}s")

    summary = {"row_ids": row_ids, "deleted_ids": delete_ids, "num_updates": num_updates}
    with open("load_test_manifest.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Manifest written to load_test_manifest.json ({len(row_ids)} rows tracked)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DSQL CDC Load Test")
    parser.add_argument("--endpoint", required=True, help="DSQL cluster endpoint")
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--table", default="users")
    parser.add_argument("--rows", type=int, default=100, help="Number of rows to insert")
    parser.add_argument("--updates", type=int, default=2, help="Number of updates per row")
    parser.add_argument("--deletes", type=int, default=10, help="Number of rows to delete")
    parser.add_argument("--workers", type=int, default=10, help="Concurrent workers")
    args = parser.parse_args()

    run_load_test(args.endpoint, args.region, args.table, args.rows, args.updates, args.deletes, args.workers)
