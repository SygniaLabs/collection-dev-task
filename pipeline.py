#!/usr/bin/env python3
"""
Pipeline Challenge — Log Processing Pipeline
=============================================

This is a NAIVE implementation of a log processing pipeline.
Your task is to optimize it for throughput and cost efficiency.

Architecture
------------

    Log Files (disk)  ──►  [Reader]  ──►  Redis Queue  ──►  [Processor]  ──►  PostgreSQL
                           1 msg/line      (list)            1 row/insert      (single table)

How to run
----------

    Terminal 1:  python pipeline.py reader
    Terminal 2:  python pipeline.py processor
    Terminal 3:  python benchmark.py          # measures throughput

Your Task
---------

The current implementation works — but it's painfully slow and would be very expensive at scale.
Imagine this running in production handling millions of logs per day on AWS.

Optimize it. Consider:
  • Batching      — Where are we making unnecessary individual round-trips?
  • Compression   — How can we reduce message sizes on the queue?
  • Schema        — Is one flat JSONB table optimal for our query patterns? (see sample_queries.sql)
  • Concurrency   — Can we parallelize anything?
  • Architecture  — Should the Processor be split into separate components?
  • Extensibility — How easy is it to add support for a new log type?
  • Cost          — What would this cost on AWS at scale? How does each optimization help?

You may restructure, split, or completely rewrite any part of this code.

Constraints:
  ✅  You may use AI tools (Copilot, ChatGPT, etc.)
  ✅  You may install additional Python packages
  ✅  You may completely restructure the code, add files, etc.
  ❌  Do NOT replace PostgreSQL or Redis (but you can use them differently)
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
    """
    Creates a single flat table for ALL log types.

    Every parsed log — regardless of type — is stored in one table with the
    parsed fields dumped into a JSONB column.

    Think about:
      - Is this the right schema for the queries in sample_queries.sql?
      - What indexes would help?
      - Should different log types have different tables?
    """
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
    """
    Parse firewall key-value log format.

    Example input:
        2024-01-15T10:23:45.123Z|action=accept|src=192.168.1.100|dst=10.0.0.50|proto=TCP|src_port=54321|dst_port=443|bytes_sent=1524|rule=web_access

    Expected output:
        {"log_type": "firewall", "timestamp": "2024-01-15T10:23:45.123Z", "action": "accept", "src": "192.168.1.100", ...}
    """
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
    """
    Parse DNS query log format.

    Example input:
        2024-01-15T10:23:45.123Z client 192.168.1.100 query: evil-domain.com IN A + (10.0.0.1) NOERROR

    Expected output:
        {"log_type": "dns", "timestamp": "...", "client_ip": "192.168.1.100", "query_domain": "evil-domain.com", ...}
    """
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
    """
    Parse authentication syslog format.

    Example input:
        2024-01-15T10:23:45.123Z auth-srv01 sshd[12345]: Accepted password for admin from 192.168.1.100 port 54321 ssh2

    Expected output:
        {"log_type": "auth", "timestamp": "...", "hostname": "auth-srv01", "status": "Accepted", "username": "admin", ...}
    """
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


# List of parsers to try — the first one that returns a result wins.
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
    """
    Reads log files from disk and publishes each line individually to a Redis queue.

    Known issues (for you to identify and fix):
      • Each line triggers a separate Redis LPUSH (one network round-trip per line)
      • No compression of message payloads
      • No batching of any kind
      • Sequential file processing, single-threaded
      • Full raw line is included in every message
    """
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

                    # Package each line as a JSON message
                    message = json.dumps({
                        "line": line,
                        "source_file": filename,
                    })

                    # ⚠️  One network round-trip per line
                    r.lpush(QUEUE_NAME, message)
                    line_count += 1

            processed_files.add(filepath)
            print(f"[Reader] Done with {filename}: {line_count:,} lines pushed to queue.")

        # All current files processed — wait for new files
        time.sleep(1)


# ─── Processor ───────────────────────────────────────────────────────────────

def processor():
    """
    Consumes messages from Redis, parses each log line,
    and inserts into PostgreSQL one row at a time.

    Known issues (for you to identify and fix):
      • Pops one message at a time from Redis (BRPOP)
      • Creates a new cursor and COMMITs for every single INSERT
      • Single flat table with JSONB — no specialized schema per log type
      • No database indexes for the common query patterns (see sample_queries.sql)
      • Parsing and indexing are tightly coupled in one component
      • Single-threaded, no concurrency
    """
    r = get_redis_client()
    conn = get_pg_connection()
    init_db()

    print("[Processor] Consuming from queue...")
    total_processed = 0

    while True:
        # ⚠️  Pop one message at a time — blocks for up to 1 second if queue is empty
        result = r.brpop(QUEUE_NAME, timeout=1)

        if result is None:
            continue

        _, raw_message = result
        data = json.loads(raw_message)
        line = data["line"]
        source_file = data["source_file"]

        # Parse the raw log line into a structured dict
        parsed = parse_log_line(line)
        if parsed is None:
            # Unparseable line — silently dropped.
            # In production, what should happen to these?
            continue

        # ⚠️  One INSERT + COMMIT per row
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
        print("  reader      Read log files from disk and push to Redis queue")
        print("  processor   Consume from Redis, parse logs, and index to PostgreSQL")
        print("  init-db     Initialize the database schema (processor does this automatically)")
        sys.exit(1)

    ROLES[sys.argv[1]]()

