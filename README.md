# 🏗️ Pipeline Challenge

## Background

Our pipeline ingests raw log files (firewall, DNS, authentication) from various sources, parses them into structured records, and indexes them into a database for fast querying by security analysts.

This repo contains a **working implementation** of a simplified version of our pipeline. Your task is to **optimize it for throughput and cost**.

## Architecture

```
┌─────────────┐                     ┌─────────────┐                         ┌────────────┐
│  Log Files   │ ──────────────────► │ Redis Queue  │ ──────────────────────► │ PostgreSQL  │
│  (on disk)   │      READER         │              │       PROCESSOR         │            │
└─────────────┘                      └─────────────┘                         └────────────┘
```

The pipeline has two components:

- **Reader** — Reads raw log files from disk and publishes to a Redis queue
- **Processor** — Consumes from the Redis queue, parses each log line into structured fields, and inserts into PostgreSQL

## Log Types

The pipeline handles three types of security logs (mixed together in each file):

| Type | Format | Example |
|------|--------|---------|
| **Firewall** | Pipe-delimited key-value | `2024-01-15T10:23:45.123Z\|action=accept\|src=192.168.1.100\|dst=10.0.0.50\|...` |
| **DNS** | Space-delimited | `2024-01-15T10:23:45.123Z client 192.168.1.100 query: evil-domain.com IN A ...` |
| **Auth** | Syslog-style | `2024-01-15T10:23:45.123Z auth-srv01 sshd[12345]: Accepted password for admin from ...` |

## Quick Start

### Option A: GitHub Codespaces (Recommended)

1. Click the green **"Code"** button → **"Codespaces"** → **"Create codespace on main"**
2. Wait for the environment to build (~2 minutes). PostgreSQL, Redis, and test data (50K log lines) are set up automatically.
3. Open terminals and start working!

### Option B: Local Development

```bash
# 1. Clone the repo
git clone <repo-url> && cd collection-dev-task

# 2. Start infrastructure
docker compose up -d

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Generate test data (50K log lines by default)
python generate_logs.py
```

## Running the Pipeline

```bash
# Terminal 1 — Start the reader
python pipeline.py reader

# Terminal 2 — Start the processor
python pipeline.py processor

# Terminal 3 — Measure throughput
python benchmark.py
```

The benchmark will:
- Reset the database and queue
- Wait for you to start the pipeline
- Monitor progress with a live throughput display
- Report final metrics and run sample queries

## Your Task

**Optimize this pipeline for production scale.** The current implementation works, but at our volume (millions of logs/day), performance and cost matter.

See `sample_queries.sql` for the types of queries analysts run — the system should support these efficiently.

Use `python benchmark.py` before and after changes to measure your improvement.

## Rules

| | |
|---|---|
| ✅ | Use AI tools (Copilot, ChatGPT, etc.) |
| ✅ | Install additional Python packages |
| ✅ | Restructure, split, or rewrite any code |
| ✅ | Add new files, scripts, or processes |

## Files

| File | Description                                                                    |
|------|--------------------------------------------------------------------------------|
| `pipeline.py` | The pipeline implementation                                                    |
| `benchmark.py` | Throughput measurement tool                                                    |
| `generate_logs.py` | Log file generator (pre-run, no need to modify)                                |
| `sample_queries.sql` | The types of queries analysts run — your schema should support these efficiently |
| `docker-compose.yml` | PostgreSQL + Redis infrastructure                                              |

## Good Luck!
