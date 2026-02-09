#!/usr/bin/env python3
"""
Pipeline Challenge — Log Processing Pipeline
=============================================

A log processing pipeline that reads security logs, parses them,
and indexes them into a database for querying.

How to run:
    Terminal 1:  python pipeline.py reader
    Terminal 2:  python pipeline.py processor
    Terminal 3:  python benchmark.py
"""

import json
import os
import re
import sys
import time

import psycopg2
import redis


# ─── Configuration ───────────────────────────────────────────────────────────

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "pipeline")
PG_USER = os.environ.get("PG_USER", "pipeline")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "pipeline")
LOG_DIR = os.environ.get("LOG_DIR", "./data/logs")
QUEUE_NAME = "log_queue"


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_pg_connection():
    """Connect to PostgreSQL with basic retry logic."""
    for attempt in range(5):
        try:
            return psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
            )
        except psycopg2.OperationalError:
            if attempt < 4:
                print(f"[DB] Connection failed, retrying in 2s... (attempt {attempt + 1}/5)")
                time.sleep(2)
            else:
                raise


# ─── Database Setup ──────────────────────────────────────────────────────────

def init_db():
    """Initialize the database schema."""
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            log_type VARCHAR(50),
            raw_line TEXT,
            timestamp VARCHAR(50),
            source_file VARCHAR(255),
            parsed_data JSONB,
            indexed_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DB] Initialized 'logs' table.")


# ─── Log Parsing ─────────────────────────────────────────────────────────────

def parse_firewall(line: str) -> dict | None:
    """Parse firewall key-value log format."""
    if "|" not in line or "=" not in line:
        return None

    parts = line.split("|")
    if len(parts) < 3:
        return None

    parsed = {"log_type": "firewall", "timestamp": parts[0]}
    for part in parts[1:]:
        if "=" in part:
            key, value = part.split("=", 1)
            parsed[key] = value
    return parsed


def parse_dns(line: str) -> dict | None:
    """Parse DNS query log format."""
    match = re.match(
        r"(\S+)\s+client\s+(\S+)\s+query:\s+(\S+)\s+IN\s+(\S+)\s+\S+\s+\((\S+)\)\s+(\S+)",
        line,
    )
    if not match:
        return None
    return {
        "log_type": "dns",
        "timestamp": match.group(1),
        "client_ip": match.group(2),
        "query_domain": match.group(3),
        "query_type": match.group(4),
        "server_ip": match.group(5),
        "response_code": match.group(6),
    }


def parse_auth(line: str) -> dict | None:
    """Parse authentication syslog format."""
    match = re.match(
        r"(\S+)\s+(\S+)\s+sshd\[(\d+)\]:\s+(Accepted|Failed)\s+(\S+)\s+for\s+(\S+)\s+from\s+(\S+)\s+port\s+(\d+)",
        line,
    )
    if not match:
        return None
    return {
        "log_type": "auth",
        "timestamp": match.group(1),
        "hostname": match.group(2),
        "pid": match.group(3),
        "status": match.group(4),
        "auth_method": match.group(5),
        "username": match.group(6),
        "source_ip": match.group(7),
        "source_port": match.group(8),
    }


PARSERS = [parse_firewall, parse_dns, parse_auth]


def parse_log_line(line: str) -> dict | None:
    """Try all parsers in order until one succeeds."""
    for parser in PARSERS:
        result = parser(line)
        if result is not None:
            return result
    return None


# ─── Reader ──────────────────────────────────────────────────────────────────

def reader():
    """Reads log files from disk and publishes to the Redis queue."""
    r = get_redis_client()
    processed_files: set[str] = set()

    print(f"[Reader] Watching {LOG_DIR} for .log files...")

    while True:
        for filename in sorted(os.listdir(LOG_DIR)):
            filepath = os.path.join(LOG_DIR, filename)
            if filepath in processed_files or not filename.endswith(".log"):
                continue

            print(f"[Reader] Processing: {filename}")
            line_count = 0

            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    message = json.dumps({
                        "line": line,
                        "source_file": filename,
                    })

                    r.lpush(QUEUE_NAME, message)
                    line_count += 1

            processed_files.add(filepath)
            print(f"[Reader] Done with {filename}: {line_count:,} lines pushed to queue.")

        time.sleep(1)


# ─── Processor ───────────────────────────────────────────────────────────────

def processor():
    """Consumes messages from Redis, parses logs, and indexes to PostgreSQL."""
    r = get_redis_client()
    conn = get_pg_connection()
    init_db()

    print("[Processor] Consuming from queue...")
    total_processed = 0

    while True:
        result = r.brpop(QUEUE_NAME, timeout=1)

        if result is None:
            continue

        _, raw_message = result
        data = json.loads(raw_message)
        line = data["line"]
        source_file = data["source_file"]

        parsed = parse_log_line(line)
        if parsed is None:
            continue

        cur = conn.cursor()
        cur.execute(
            """INSERT INTO logs (log_type, raw_line, timestamp, source_file, parsed_data)
               VALUES (%s, %s, %s, %s, %s)""",
            (
                parsed.get("log_type"),
                line,
                parsed.get("timestamp"),
                source_file,
                json.dumps(parsed),
            ),
        )
        conn.commit()
        cur.close()

        total_processed += 1
        if total_processed % 1000 == 0:
            print(f"[Processor] Indexed {total_processed:,} records so far...")


# ─── Entry Point ─────────────────────────────────────────────────────────────

ROLES = {
    "reader": reader,
    "processor": processor,
    "init-db": init_db,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ROLES:
        print("Usage: python pipeline.py <role>")
        print()
        print("Roles:")
        print("  reader      Read log files and push to queue")
        print("  processor   Consume from queue and index to database")
        print("  init-db     Initialize database schema")
        sys.exit(1)

    ROLES[sys.argv[1]]()
