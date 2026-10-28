#!/usr/bin/env python3
"""DSQL CDC Load Test - Writes concurrent operations to a DSQL table.

Each unit of work runs inside an explicit BEGIN/COMMIT so every write is
atomic. Use --txn-size to bundle multiple same-op DMLs into a single
transaction (atomicity across multiple rows). Totals (committed txns,
failed txns, total DMLs) are tracked and written to the manifest so the
verifier can assert end-to-end record counts.
"""

import argparse
import concurrent.futures
import json
import os
import threading
import time
import uuid

import boto3
import psycopg2
import psycopg2.errors
from psycopg2 import sql


ALLOWED_TABLES = {"users", "customers", "products", "orders", "reviews"}


class Counters:
    """Thread-safe totals for the manifest."""

    def __init__(self):
        self.lock = threading.Lock()
        self.committed_txns = 0
        self.failed_txns = 0
        self.total_dmls = 0

    def record_commit(self, dmls):
        with self.lock:
            self.committed_txns += 1
            self.total_dmls += dmls

    def record_failure(self):
        with self.lock:
            self.failed_txns += 1

    def snapshot(self):
        with self.lock:
            return {
                "committed_txns": self.committed_txns,
                "failed_txns": self.failed_txns,
                "total_dmls": self.total_dmls,
            }


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


def _validate_table(table):
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Invalid table name: {table}. Allowed: {ALLOWED_TABLES}")


def _run_txn(endpoint, region, token, statements, counters, max_retries=5):
    """Execute statements inside one explicit transaction. Atomic: all commit or none."""
    for attempt in range(max_retries):
        conn = None
        try:
            conn = get_connection(endpoint, region, token)
            conn.autocommit = False
            with conn:
                with conn.cursor() as cur:
                    for stmt, params in statements:
                        cur.execute(stmt, params)
            counters.record_commit(len(statements))
            return
        except psycopg2.errors.SerializationFailure:
            if attempt == max_retries - 1:
                counters.record_failure()
                raise
            time.sleep(0.1 * (2 ** attempt))
        except Exception:
            counters.record_failure()
            raise
        finally:
            if conn is not None:
                conn.close()


def insert_batch(endpoint, region, token, table, row_ids, counters):
    _validate_table(table)
    stmts = [
        (
            sql.SQL("INSERT INTO {} (id, name, email) VALUES (%s, %s, %s)").format(sql.Identifier(table)),
            (rid, f"user-{rid[:8]}", f"user-{rid[:8]}@example.com"),
        )
        for rid in row_ids
    ]
    _run_txn(endpoint, region, token, stmts, counters)


def update_batch(endpoint, region, token, table, items, counters):
    """items: list of (row_id, seq) tuples applied atomically."""
    _validate_table(table)
    stmts = [
        (
            sql.SQL("UPDATE {} SET email = %s WHERE id = %s").format(sql.Identifier(table)),
            (f"updated-{seq}-{rid[:8]}@example.com", rid),
        )
        for rid, seq in items
    ]
    _run_txn(endpoint, region, token, stmts, counters)


def delete_batch(endpoint, region, token, table, row_ids, counters):
    _validate_table(table)
    stmts = [
        (
            sql.SQL("DELETE FROM {} WHERE id = %s").format(sql.Identifier(table)),
            (rid,),
        )
        for rid in row_ids
    ]
    _run_txn(endpoint, region, token, stmts, counters)


def _chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def run_load_test(endpoint, region, table, num_rows, num_updates, num_deletes, workers, txn_size):
    token = get_dsql_token(region, endpoint)
    row_ids = [str(uuid.uuid4()) for _ in range(num_rows)]
    counters = Counters()

    insert_batches = list(_chunks(row_ids, txn_size))
    print(f"Inserting {num_rows} rows in {len(insert_batches)} transaction(s) (txn_size={txn_size}, workers={workers})...")
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(insert_batch, endpoint, region, token, table, batch, counters) for batch in insert_batches]
        for f in concurrent.futures.as_completed(futures):
            f.result()
    print(f"  Inserts done in {time.time() - t0:.2f}s")

    if num_updates > 0:
        update_items = [(rid, seq) for rid in row_ids for seq in range(num_updates)]
        update_batches = list(_chunks(update_items, txn_size))
        print(f"Updating {len(update_items)} DMLs in {len(update_batches)} transaction(s)...")
        t0 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(update_batch, endpoint, region, token, table, batch, counters) for batch in update_batches]
            for f in concurrent.futures.as_completed(futures):
                f.result()
        print(f"  Updates done in {time.time() - t0:.2f}s")

    delete_ids = row_ids[:num_deletes]
    if delete_ids:
        delete_batches = list(_chunks(delete_ids, txn_size))
        print(f"Deleting {len(delete_ids)} rows in {len(delete_batches)} transaction(s)...")
        t0 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(delete_batch, endpoint, region, token, table, batch, counters) for batch in delete_batches]
            for f in concurrent.futures.as_completed(futures):
                f.result()
        print(f"  Deletes done in {time.time() - t0:.2f}s")

    totals = counters.snapshot()
    summary = {
        "row_ids": row_ids,
        "deleted_ids": delete_ids,
        "num_updates": num_updates,
        "txn_size": txn_size,
        "totals": totals,
    }
    with open("load_test_manifest.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(
        f"Manifest written to load_test_manifest.json | "
        f"committed_txns={totals['committed_txns']} failed_txns={totals['failed_txns']} total_dmls={totals['total_dmls']}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DSQL CDC Load Test")
    parser.add_argument("--endpoint", required=True, help="DSQL cluster endpoint")
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
        help="AWS region (defaults to AWS_REGION / AWS_DEFAULT_REGION env var)",
    )
    parser.add_argument("--table", default="users")
    parser.add_argument("--rows", type=int, default=100, help="Number of rows to insert")
    parser.add_argument("--updates", type=int, default=2, help="Number of updates per row")
    parser.add_argument("--deletes", type=int, default=10, help="Number of rows to delete")
    parser.add_argument("--workers", type=int, default=10, help="Concurrent workers")
    parser.add_argument(
        "--txn-size",
        type=int,
        default=1,
        help="DMLs grouped into one atomic transaction (default 1 = per-DML atomicity)",
    )
    args = parser.parse_args()

    if not args.region:
        parser.error("--region is required (or set AWS_REGION / AWS_DEFAULT_REGION)")
    if args.txn_size < 1:
        parser.error("--txn-size must be >= 1")

    run_load_test(
        args.endpoint, args.region, args.table, args.rows, args.updates, args.deletes,
        args.workers, args.txn_size,
    )
