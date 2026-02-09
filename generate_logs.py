#!/usr/bin/env python3
"""
Log File Generator for the Pipeline Challenge.
===============================================

Generates realistic security log files (firewall, DNS, authentication)
for use as input to the pipeline.

This is PRE-RUN before the interview. The candidate does NOT need to modify this file.

Usage:
    python generate_logs.py                    # Default: 5 files x 50K lines = 250K lines
    python generate_logs.py --files 10 --lines 100000   # Custom
"""

import argparse
import datetime
import os
import random

OUTPUT_DIR = os.environ.get("LOG_DIR", "./data/logs")
NUM_FILES = 5
LINES_PER_FILE = 10_000  # 50K total lines across all files (faster for interviews)

# ─── Seed for reproducible data ─────────────────────────────────────────────
random.seed(42)

# ─── Realistic value pools ──────────────────────────────────────────────────

INTERNAL_IPS = [f"192.168.1.{i}" for i in range(1, 255)]
EXTERNAL_IPS = [f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}" for _ in range(500)]
DOMAINS = [
    "google.com", "github.com", "evil-domain.com", "cdn.example.com",
    "api.internal.local", "malware-c2.bad", "office365.com", "aws.amazon.com",
    "suspicious-site.xyz", "login.microsoftonline.com", "update.service.net",
    "stackoverflow.com", "slack.com", "zoom.us", "dropbox.com",
    "phishing-login.evil", "data-exfil.bad", "legit-saas.com",
]
USERS = ["admin", "root", "jsmith", "adrutin", "deploy-bot", "backup-svc", "unknown", "cjones", "mlee"]
ACTIONS_FW = ["accept", "drop", "reject"]
PROTOCOLS = ["TCP", "UDP", "ICMP"]
PORTS = [22, 53, 80, 443, 8080, 8443, 3389, 3306, 5432, 25, 110, 993, 995]
RULES = ["web_access", "dns_allow", "ssh_admin", "block_malware", "default_drop", "vpn_access", "internal_only"]
AUTH_STATUSES = ["Accepted", "Failed", "Failed", "Accepted", "Accepted"]  # weighted towards success
AUTH_METHODS = ["password", "publickey", "keyboard-interactive"]
HOSTNAMES = [f"auth-srv{str(i).zfill(2)}" for i in range(1, 8)]
QUERY_TYPES = ["A", "AAAA", "MX", "CNAME", "TXT", "PTR"]
RESPONSE_CODES = ["NOERROR", "NXDOMAIN", "SERVFAIL", "NOERROR", "NOERROR"]  # weighted


# ─── Generators ──────────────────────────────────────────────────────────────

def random_ts(base: datetime.datetime, offset_seconds: int) -> str:
    """Generate a realistic ISO timestamp with millisecond precision."""
    ts = base + datetime.timedelta(seconds=offset_seconds, milliseconds=random.randint(0, 999))
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def gen_firewall_line(ts: str) -> str:
    """Firewall log: key-value pipe-delimited format (like Check Point / Palo Alto)."""
    return (
        f"{ts}|action={random.choice(ACTIONS_FW)}|src={random.choice(INTERNAL_IPS)}"
        f"|dst={random.choice(EXTERNAL_IPS)}|proto={random.choice(PROTOCOLS)}"
        f"|src_port={random.randint(10000, 65535)}|dst_port={random.choice(PORTS)}"
        f"|bytes_sent={random.randint(64, 65535)}|rule={random.choice(RULES)}"
    )


def gen_dns_line(ts: str) -> str:
    """DNS query log: space-delimited format (like BIND / Windows DNS)."""
    return (
        f"{ts} client {random.choice(INTERNAL_IPS)} query: {random.choice(DOMAINS)}"
        f" IN {random.choice(QUERY_TYPES)} + ({random.choice(EXTERNAL_IPS[:10])})"
        f" {random.choice(RESPONSE_CODES)}"
    )


def gen_auth_line(ts: str) -> str:
    """Authentication log: syslog-style format (like OpenSSH)."""
    status = random.choice(AUTH_STATUSES)
    return (
        f"{ts} {random.choice(HOSTNAMES)} sshd[{random.randint(1000, 65535)}]:"
        f" {status} {random.choice(AUTH_METHODS)} for {random.choice(USERS)}"
        f" from {random.choice(INTERNAL_IPS)} port {random.randint(10000, 65535)} ssh2"
    )


GENERATORS = [gen_firewall_line, gen_dns_line, gen_auth_line]
LOG_TYPE_WEIGHTS = [0.5, 0.3, 0.2]  # firewall-heavy, realistic distribution


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate test log files for the pipeline challenge.")
    parser.add_argument("--files", type=int, default=5, help="Number of log files to generate (default: 5)")
    parser.add_argument("--lines", type=int, default=10_000, help="Lines per file (default: 10,000)")
    args = parser.parse_args()

    num_files = args.files
    lines_per_file = args.lines

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Check if data already exists
    existing = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(".log")] if os.path.exists(OUTPUT_DIR) else []
    if existing:
        print(f"⚠️  Found {len(existing)} existing log files in {OUTPUT_DIR}/")
        print("   Delete them first if you want fresh data, or they will be overwritten.\n")

    base_time = datetime.datetime(2024, 1, 15, 0, 0, 0)
    total_lines = 0

    for file_idx in range(num_files):
        filename = f"logs_{file_idx:04d}.log"
        filepath = os.path.join(OUTPUT_DIR, filename)
        print(f"  Generating {filename} ({lines_per_file:,} lines)...")

        with open(filepath, "w") as f:
            for line_num in range(lines_per_file):
                offset = file_idx * lines_per_file + line_num
                ts = random_ts(base_time, offset)
                gen = random.choices(GENERATORS, weights=LOG_TYPE_WEIGHTS, k=1)[0]
                f.write(gen(ts) + "\n")
                total_lines += 1

    print(f"\n✅ Generated {num_files} files × {lines_per_file:,} lines = {total_lines:,} total log lines")
    print(f"   Output directory: {os.path.abspath(OUTPUT_DIR)}/")


if __name__ == "__main__":
    main()

