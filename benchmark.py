#!/usr/bin/env python3
"""
Benchmark — Measures Pipeline Throughput
=========================================

Measures end-to-end performance of the pipeline:
  Log Files → Reader → Redis → Processor → PostgreSQL

Usage:
    1. Make sure docker-compose services are running (postgres + redis)
    2. Start your pipeline (reader + processor) in separate terminals
    3. Run:  python benchmark.py

The benchmark will:
  • Reset the database and queue to a clean state
  • Wait for you to start the pipeline
  • Monitor progress until all records are indexed
  • Report throughput metrics and run sample queries

Run this BEFORE and AFTER your optimizations to measure improvement.
"""

import os
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

TIMEOUT_SECONDS = 600  # 10 minute max


def get_pg():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def count_log_lines() -> int:
    """Count total non-empty lines across all .log files."""
    total = 0
    if not os.path.isdir(LOG_DIR):
        return 0
    for fname in os.listdir(LOG_DIR):
        if fname.endswith(".log"):
            with open(os.path.join(LOG_DIR, fname)) as fh:
                total += sum(1 for line in fh if line.strip())
    return total


def count_pg_rows() -> int:
    """Count total rows across ALL tables in the public schema (handles any schema the candidate creates)."""
    try:
        conn = get_pg()
        cur = conn.cursor()
        # Get all user tables
        cur.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
        """)
        tables = [row[0] for row in cur.fetchall()]

        total = 0
        for table in tables:
            try:
                cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                total += cur.fetchone()[0]
            except Exception:
                conn.rollback()

        cur.close()
        conn.close()
        return total
    except Exception:
        return 0


def get_queue_depth() -> int:
    """Get the current number of messages in the Redis queue."""
    try:
        r = get_redis_client()
        # Check all keys that look like queues (in case candidate creates multiple)
        depth = 0
        for key in r.keys("*queue*") + r.keys("*Queue*") + [QUEUE_NAME]:
            try:
                depth += r.llen(key)
            except Exception:
                pass
        return depth
    except Exception:
        return 0


# ─── Reset ───────────────────────────────────────────────────────────────────

def reset():
    """Clear all data for a clean benchmark run."""
    print("🧹 Resetting state...")

    # Clear Redis
    try:
        r = get_redis_client()
        r.flushdb()
        print("   ✓ Redis flushed")
    except Exception as e:
        print(f"   ✗ Redis reset failed: {e}")
        return False

    # Clear PostgreSQL
    try:
        conn = get_pg()
        cur = conn.cursor()
        cur.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
        """)
        tables = [row[0] for row in cur.fetchall()]
        for table in tables:
            cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
        conn.commit()
        cur.close()
        conn.close()
        print(f"   ✓ PostgreSQL cleared ({len(tables)} tables dropped)")
    except Exception as e:
        print(f"   ✗ PostgreSQL reset failed: {e}")
        return False

    return True


# ─── Monitor ─────────────────────────────────────────────────────────────────

def wait_for_completion(expected_lines: int) -> tuple[float, int]:
    """
    Monitor pipeline progress.
    Returns (elapsed_seconds, final_row_count).
    """
    start = time.time()
    prev_count = 0
    stall_start = None
    STALL_THRESHOLD = 15  # seconds with no progress = assume done

    while True:
        elapsed = time.time() - start
        pg_rows = count_pg_rows()
        queue_depth = get_queue_depth()
        rate = pg_rows / elapsed if elapsed > 0 else 0

        # Progress bar
        pct = min(100, (pg_rows / expected_lines * 100)) if expected_lines > 0 else 0
        bar_len = 30
        filled = int(bar_len * pct / 100)
        bar = "█" * filled + "░" * (bar_len - filled)

        status = (
            f"\r   {bar} {pct:5.1f}% │ "
            f"{pg_rows:>8,} / {expected_lines:,} rows │ "
            f"Queue: {queue_depth:>6,} │ "
            f"{rate:>8,.0f} rows/sec │ "
            f"{elapsed:>6.1f}s"
        )
        sys.stdout.write(status)
        sys.stdout.flush()

        # Check if we've reached the target
        if pg_rows >= expected_lines:
            print()
            return elapsed, pg_rows

        # Detect stall (queue empty + no new rows for STALL_THRESHOLD seconds)
        if pg_rows == prev_count and queue_depth == 0 and pg_rows > 0:
            if stall_start is None:
                stall_start = time.time()
            elif time.time() - stall_start > STALL_THRESHOLD:
                print(f"\n   ⚠️  Pipeline stalled for {STALL_THRESHOLD}s with {pg_rows:,} rows indexed.")
                return elapsed, pg_rows
        else:
            stall_start = None

        prev_count = pg_rows

        # Timeout
        if elapsed > TIMEOUT_SECONDS:
            print(f"\n   ⏰ Timeout after {TIMEOUT_SECONDS}s")
            return elapsed, pg_rows

        time.sleep(0.5)


# ─── Sample Queries ──────────────────────────────────────────────────────────

def run_sample_queries():
    """Run typical analyst queries to verify the pipeline and measure query speed."""
    conn = get_pg()
    cur = conn.cursor()

    print("\n📊 Sample Queries (verifying data + measuring query speed):")
    print("─" * 70)

    # First, figure out what tables exist
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    tables = [row[0] for row in cur.fetchall()]
    print(f"   Tables found: {', '.join(tables)}\n")

    # ── Generic queries that work with the default 'logs' table ──
    # We wrap each in a try/except so they gracefully handle schema changes
    queries = [
        (
            "Records by log type",
            """SELECT COALESCE(log_type, parsed_data->>'log_type', 'unknown') AS type, COUNT(*)
               FROM logs GROUP BY 1 ORDER BY 2 DESC""",
        ),
        (
            "Top 5 firewall source IPs",
            """SELECT parsed_data->>'src' AS src_ip, COUNT(*) AS cnt
               FROM logs WHERE log_type = 'firewall'
               GROUP BY 1 ORDER BY 2 DESC LIMIT 5""",
        ),
        (
            "Failed auth attempts",
            """SELECT parsed_data->>'username' AS username, COUNT(*) AS cnt
               FROM logs WHERE log_type = 'auth' AND parsed_data->>'status' = 'Failed'
               GROUP BY 1 ORDER BY 2 DESC LIMIT 5""",
        ),
        (
            "DNS queries to suspicious domains",
            """SELECT parsed_data->>'query_domain' AS domain, COUNT(*) AS cnt
               FROM logs WHERE log_type = 'dns'
                 AND parsed_data->>'query_domain' IN ('evil-domain.com', 'malware-c2.bad', 'suspicious-site.xyz')
               GROUP BY 1 ORDER BY 2 DESC""",
        ),
    ]

    for title, query in queries:
        print(f"  🔍 {title}")
        try:
            start = time.time()
            cur.execute(query)
            duration_ms = (time.time() - start) * 1000
            rows = cur.fetchall()
            if rows:
                for row in rows[:5]:
                    print(f"       {row}")
            else:
                print("       (no results)")
            print(f"       ⏱  {duration_ms:.1f}ms\n")
        except Exception as e:
            print(f"       ⚠️  Skipped — table schema changed (this is fine!): {type(e).__name__}")
            print(f"       If you changed the schema, update the queries in benchmark.py or write your own.\n")
            conn.rollback()

    cur.close()
    conn.close()


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  📏  Pipeline Challenge — Benchmark")
    print("=" * 70)
    print()

    # Check data exists
    total_lines = count_log_lines()
    if total_lines == 0:
        print(f"❌ No log files found in {LOG_DIR}/")
        print("   Run:  python generate_logs.py")
        sys.exit(1)
    print(f"📁 Found {total_lines:,} log lines in {LOG_DIR}/\n")

    # Reset
    if not reset():
        print("❌ Failed to reset. Are PostgreSQL and Redis running?")
        print("   Run:  docker compose up -d")
        sys.exit(1)

    print()
    print("▶️  Now start the pipeline in separate terminal(s).")
    print("   For the naive version:")
    print("     Terminal 1:  python pipeline.py reader")
    print("     Terminal 2:  python pipeline.py processor")
    print()
    input("   Press ENTER when the pipeline is running... ")
    print()

    # Monitor
    print("⏳ Monitoring pipeline progress...")
    elapsed, final_count = wait_for_completion(total_lines)

    # Report
    throughput = final_count / elapsed if elapsed > 0 else 0
    print()
    print("=" * 70)
    print("  📈  RESULTS")
    print("=" * 70)
    print(f"  Records indexed:  {final_count:>12,}")
    print(f"  Total time:       {elapsed:>12.1f} s")
    print(f"  Throughput:       {throughput:>12,.0f} records/sec")
    print()

    print("=" * 70)

    # Run sample queries
    run_sample_queries()

    # Cost calculation based on candidate's batch size
    print()
    print("=" * 70)
    print("  💰  COST ANALYSIS")
    print("=" * 70)
    print()
    print("  SQS charges $0.40 per million requests.")
    print()
    
    try:
        batch_size_input = input("  What batch size did you use for queue messages? [1]: ").strip()
        batch_size = int(batch_size_input) if batch_size_input else 1
        if batch_size < 1:
            batch_size = 1
    except (ValueError, EOFError):
        batch_size = 1
    
    daily_volume = 10_000_000  # 10M logs/day (production scale)
    daily_messages = daily_volume / batch_size
    daily_cost = (daily_messages / 1_000_000) * 0.40
    monthly_cost = daily_cost * 30
    
    print()
    print(f"  At production scale ({daily_volume:,} logs/day):")
    print(f"     Batch size:      {batch_size:,} logs per message")
    print(f"     SQS messages:    {daily_messages:,.0f}/day")
    print(f"     SQS cost:        ${daily_cost:,.2f}/day  (${monthly_cost:,.0f}/month)")
    print("=" * 70)


if __name__ == "__main__":
    main()

