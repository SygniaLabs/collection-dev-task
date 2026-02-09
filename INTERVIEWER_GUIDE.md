# 📋 Interviewer Guide — Pipeline Challenge

> **⚠️ INTERNAL DOCUMENT — Do NOT include in the candidate's repo.**
> Remove this file before pushing to the candidate-facing repository.

---

## Interview Structure (1.5 hours total)

| Phase | Duration | Description |
|-------|----------|-------------|
| **1. Intro & Background** | 15 min | Team overview, role expectations, candidate experience deep-dive |
| **2. Exercise Briefing** | 5 min | Share Codespace/repo link, walk through the naive implementation together |
| **3. Bottleneck Analysis** | 5–10 min | Candidate reads code, identifies problems, discusses optimization strategy |
| **4. Hands-on Implementation** | 25–30 min | Candidate implements optimizations, runs benchmarks, explains trade-offs |
| **5. Architecture Discussion** | 15–20 min | Production scaling, cost analysis, new log types, monitoring, what-ifs |
| **Buffer** | ~5 min | Overflow / wrap-up |

### Phase 2 Tips
- Walk through `pipeline.py` together — show them the Reader and Processor
- Point out `benchmark.py` and `sample_queries.sql` — these define success
- Don't give hints yet; let them identify the issues themselves in Phase 3

### Phase 3 Tips
- Ask: *"What bottlenecks do you see in this code?"*
- A strong candidate should identify 3-5 issues within a few minutes
- Ask them to prioritize: *"If you had time for only one optimization, which would have the biggest impact?"*

### Phase 4 Tips
- Let them work. Observe their process — do they plan first or dive in?
- If they're stuck after 10 min, you can hint: *"What if the Reader sent messages in batches?"*
- Ask them to run the benchmark periodically to see improvement
- If they finish batching quickly, prompt: *"What about the database schema?"*

---

## Optimization Levels — Scoring Guide

### Level 1 — Batching (60% of candidates should reach this) ✅ MUST-HAVE

**What to look for:**

1. **Redis batch writes** — Use `r.pipeline()` to batch LPUSH commands
   ```python
   pipe = r.pipeline()
   for msg in batch:
       pipe.lpush(QUEUE_NAME, msg)
   pipe.execute()
   ```

2. **Redis batch reads** — Pop multiple messages at once (Lua script, pipeline RPOP, or LRANGE+LTRIM)
   ```python
   pipe = r.pipeline()
   for _ in range(BATCH_SIZE):
       pipe.rpop(QUEUE_NAME)
   results = pipe.execute()
   ```

3. **PostgreSQL bulk inserts** — Use `executemany()` or `psycopg2.extras.execute_values()` with single COMMIT per batch
   ```python
   from psycopg2.extras import execute_values
   execute_values(cur, "INSERT INTO logs (...) VALUES %s", batch_rows)
   conn.commit()
   ```

**Expected throughput improvement:** 5–10x

**Key talking points to listen for:**
- "Each LPUSH is a TCP round-trip; batching amortizes the network overhead"
- "Committing per INSERT forces a WAL fsync per row — bulk commit is critical"
- "On AWS, SQS charges per API request. Batching 10 messages per request = 10x cost reduction"

---

### Level 2 — Message Compression (40% of candidates)

**What to look for:**
- Compress batch payloads with gzip before publishing to Redis
- Send one Redis message containing N compressed log lines instead of N individual messages
- Understanding of SQS message size limits (256KB) and pricing model

**Key talking points:**
- "A batch of 100 JSON lines gzip-compressed is ~10x smaller than uncompressed"
- "Fewer, larger messages = dramatically lower queue costs"
- "This is a direct trade-off: compression CPU cost vs. network/queue savings"

---

### Level 3 — Schema Design & Database Indexing (35% of candidates) ⭐ DIFFERENTIATOR

**What to look for:**

1. **Separate tables per log type** with proper typed columns:
   ```sql
   CREATE TABLE firewall_logs (
       id BIGSERIAL PRIMARY KEY,
       timestamp TIMESTAMPTZ NOT NULL,
       src_ip INET NOT NULL,
       dst_ip INET NOT NULL,
       action VARCHAR(10),
       protocol VARCHAR(10),
       src_port INTEGER,
       dst_port INTEGER,
       bytes_sent INTEGER,
       rule VARCHAR(50),
       source_file VARCHAR(255)
   );
   ```

2. **B-tree indexes** on commonly queried fields:
   ```sql
   CREATE INDEX idx_fw_src_ts ON firewall_logs (src_ip, timestamp);
   CREATE INDEX idx_fw_dst_port ON firewall_logs (dst_port);
   CREATE INDEX idx_dns_domain ON dns_logs (query_domain);
   CREATE INDEX idx_auth_status_ip ON auth_logs (status, source_ip);
   ```

3. **Awareness of INET type** for IP columns (enables range/subnet queries)
4. **Discussion of timestamp partitioning** for large tables

**Key talking points:**
- "JSONB querying with `->>'field'` can't use B-tree indexes effectively"
- "Typed columns give better storage efficiency and query planning"
- "Separate tables mean each can be indexed for its specific query patterns"
- "In production at scale, this is why systems use columnar stores like Snowflake"

---

### Level 4 — Architecture & Concurrency (25% of candidates) ⭐⭐ SENIOR SIGNAL

**What to look for:**

1. **Split Processor into Parser + Indexer** (independent scaling)
2. **Multiple consumer instances** (N parallel processors)
3. **Per-log-type routing** — reader classifies lines, routes to type-specific queues
4. **Thread pool or multiprocessing** for parallelism

**Target architecture:**
```
Files → [Reader] → redis:raw_queue
                       ├─→ redis:firewall_queue → [FW Parser]  ──→ redis:index_queue → [Indexer] → PG
                       ├─→ redis:dns_queue      → [DNS Parser] ─┘
                       └─→ redis:auth_queue     → [Auth Parser]─┘
```

**Key talking points:**
- "Parsing is CPU-bound, indexing is I/O-bound — they should scale independently"
- "Adding a new log type just means adding a new parser + queue; the indexer is generic"
- "In k8s, we'd autoscale parsers based on queue depth (HPA)"
- "This maps to SQS → Lambda or SQS → ECS tasks in AWS"

---

### Level 5 — Production Readiness (15% of candidates) ⭐⭐⭐ EXCEPTIONAL

**What to look for:**
- **Dead letter queue** for unparseable/failed messages
- **Backpressure** — reader slows down when queue is too deep
- **Idempotency** — handle duplicate messages gracefully (dedup by hash)
- **Dynamic log type registration** — parser registry / plugin pattern
- **Configurable batch size with timeout** — "flush after N messages OR T seconds, whichever comes first"
- **Monitoring** — queue depth, processing latency, error rate, throughput metrics
- **Error recovery** — what happens if PG goes down mid-batch?

---

## Discussion Questions (Phase 5)

Use these AFTER the hands-on portion. Pick 3–4 based on what the candidate covered:

### Scaling
> *"A new customer sends 10x the log volume. What changes?"*

Expected: Horizontal scaling of consumers, queue partitioning, batch size tuning, possibly streaming (Kafka) instead of polling, auto-scaling based on queue depth.

### Schema Evolution
> *"A new log format has a field we've never seen before. How do you handle schema changes?"*

Expected: Additive-only migrations, backward compatibility, Snowflake VARIANT type, schema versioning, graceful degradation for unknown fields.

### Low-Latency Queries
> *"Analysts want sub-second query response for the last 24 hours of firewall data. How?"*

Expected: Hot/cold storage tiers, in-memory caching, materialized views, read replicas, Elasticsearch for recent data, or time-based partitioning with partition pruning.

### Cost Optimization
> *"Our SQS bill spiked. How do you investigate and reduce it?"*

Expected: Batch API calls (up to 10 per request), message compression, reduce empty receives (long polling), analyze CloudWatch metrics for queue patterns, possibly switch to SNS fan-out.

### Failure Handling
> *"A parser bug produced malformed data for 2 hours. How do you recover without data loss?"*

Expected: Dead letter queue, replayability from source files, bookkeeping/integrity tracking, versioned processing, ability to re-process a time window.

### Monitoring
> *"What metrics would you add to this pipeline for production observability?"*

Expected: Queue depth over time, processing latency (end-to-end and per-stage), error rate by type, throughput (records/sec), batch sizes, memory/CPU utilization, data lag (newest record age).

---

## Scoring Matrix

| Area | Weight | 1-2 (Junior) | 3-4 (Mid) | 5 (Senior) |
|------|--------|--------------|-----------|------------|
| **Bottleneck Identification** | 15% | Spots 1-2 issues | Systematically finds most issues | Immediately sees all issues, prioritizes by ROI |
| **Batching** | 20% | Basic batching in one place | Batching across reader + processor + DB | Configurable batch size with timeout fallback |
| **Schema Design** | 20% | Keeps single table, maybe adds an index | Separate tables with proper types | Typed tables + composite indexes + partitioning discussion |
| **Architecture Thinking** | 20% | Single-process improvements | Discusses component splitting and scaling | Draws production architecture with queue routing & auto-scaling |
| **Cost Awareness** | 10% | "Batching is cheaper" | Cites specific SQS pricing, estimates savings | Full cost model, compares optimization ROI |
| **Communication** | 15% | Explains what they're doing | Explains why, discusses trade-offs | Proactively raises edge cases, asks clarifying questions |

### Scoring Thresholds

| Score | Verdict |
|-------|---------|
| **4.0 – 5.0** | Strong hire — senior-level pipeline thinking |
| **3.0 – 3.9** | Hire — solid fundamentals, can grow into the role |
| **2.0 – 2.9** | Borderline — good coder but lacks scale/systems thinking |
| **< 2.0** | No hire — insufficient depth for this role |

---

## How This Maps to Our Real Pipeline

| Exercise Component | Our Pipeline |
|---|---|
| Reader → Redis LPUSH | **Dealer** → SQS → **Reader** with `GzipCompressingPublisher` |
| Redis list queue | **RabbitMQ / SQS** pipeline queues |
| Processor parsing | **Windmill Parser** with transport/source parsing chain |
| Log type routing | **Parsing chain** detection → per-source parsers (Check Point, ESXi, etc.) |
| PostgreSQL indexing | **Indexer** → Elasticsearch + Snowflake via `SnowflakePublisher` |
| Batch size + timeout | **Batcher** class (batch_size_messages, batch_size_bytes, timeout) |
| Dead letter queue | **Retry managers** in Enricher |
| Progress tracking | **Bookkeeper** tracking file processing integrity in PostgreSQL |

A candidate who excels at this exercise will naturally understand and contribute to our pipeline from day one.

---

## Pre-Interview Checklist

- [ ] Push repo to `SygniaLabs/pipeline-challenge` (REMOVE this file first!)
- [ ] Test the Codespace setup works (create one, run the benchmark)
- [ ] For each candidate: create a branch `candidate/<name>-<date>`
- [ ] Send the candidate the Codespace link (or repo URL for local setup)
- [ ] Have the scoring matrix open during the interview

