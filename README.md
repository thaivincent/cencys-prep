# Censys Technical Interview Prep (Target Date: Oct 22, 2026)

High-throughput, concurrent data ingestion pipeline built in Python with thread-safe queues, immutable dataclasses, and single-writer SQLite WAL persistence.

---

## Architecture Overview

```text
[Input Stream / Mock Queue]
            │
            ▼
   [2 Producer Threads] ──(Network / Raw Sockets)
            │
            │  q.put(Job)  [queue.Queue(maxsize=10)]
            ▼
    [Consumer Thread]   ──(JSON parsing, retries, validation)
            │
            │  db_queue.put(DBWriteJob)  [queue.Queue(maxsize=100)]
            ▼
 [Dedicated DB Writer Thread] ──(Single-thread connection ownership)
            │
            ▼
  [SQLite DB: app.db (WAL Mode + PRAGMA synchronous=NORMAL)]
```

---

## Daily Action Plan & Status

- [x] **Day 1 (Oct 15): Concurrency & Queues (Producer-Consumer)**
  - `threading.Thread` lifecycle (`start`, `join`)
  - `queue.Queue` primitives (`put`, `get`, `task_done`, `join`)
  - Clean draining and graceful termination with `SENTINEL = object()`
- [x] **Day 2 (Oct 16): Dataclasses & Callback Decoupling**
  - `@dataclass(frozen=True, slots=True)` for immutable thread-safe message passing
  - Asynchronous callback patterns (`on_success`, `on_failure`)
  - `dataclasses.replace` for updating retry counts without in-place mutation
- [x] **Day 3 (Oct 17): SQLite Thread Isolation & Concurrency**
  - Solved `sqlite3.ProgrammingError` via single-writer thread connection ownership
  - Configured `PRAGMA journal_mode=WAL;` and `PRAGMA synchronous=NORMAL;`
  - Safe transaction boundaries using `with db:`
  - Structured `DBWriteJob(query, params, callback)` message envelope
  - Cascading two-stage shutdown sequence
- [ ] **Day 4 (Oct 18): Schema Design, Parameterization & Atomic Upserts**
  - Modern SQLite upsert syntax: `INSERT INTO ... ON CONFLICT(id) DO UPDATE SET ... WHERE ...`
  - Version checks in upserts (`WHERE excluded.version >= scans.version` or `timestamp`)
  - Out-of-order write rejection unit tests
- [ ] **Day 5 (Oct 19): Serialization, Base64 & Safe Decompression**
  - Binary stream decompression (`gzip.decompress()`) and Base64 schema envelopes
  - Schema evolution: unpacking versioned envelopes
- [ ] **Day 6 (Oct 20): Failure Modes, DLQ & At-Least-Once Delivery**
  - Dead-Letter Queue (DLQ) routing for poison pills
  - Database lock handling with exponential backoff
- [ ] **Day 7 (Oct 21): Full Pipeline Mock Simulation & Review**
  - End-to-end 90-minute simulation combining all components

---

## How to Run

Requirements: Python 3.10+ (recommended 3.12 for `slots=True` in dataclasses).

```powershell
# Using the Windows py launcher (defaults to Python 3.12):
py threads.py

# Or using python:
python threads.py
```

### Inspecting the Database

```powershell
py -c "import sqlite3; conn = sqlite3.connect('app.db'); print(conn.execute('SELECT * FROM scans LIMIT 5').fetchall()); conn.close()"
```

