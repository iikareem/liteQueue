<div align="center">

# LiteQ

**A persistent, zero-infrastructure task queue for Node.js — powered by SQLite.**

[![npm version](https://img.shields.io/badge/npm-v1.0.0-CB3837?logo=npm)](https://npmjs.com/package/liteq)
[![node](https://img.shields.io/badge/node-%3E%3D18.0.0-339933?logo=nodedotjs)](https://nodejs.org)
[![license](https://img.shields.io/badge/license-MIT-blue)](./LICENSE)
[![TypeScript](https://img.shields.io/badge/TypeScript-ready-3178C6?logo=typescript)](https://www.typescriptlang.org)

Delayed scheduling · Atomic job locking · Exponential backoff · CPU thread isolation

**No Redis. No Docker. No infrastructure.**

</div>

---

## Why LiteQ?

Most apps don't need Redis. They need a reliable way to run background jobs without spinning up external services, managing connections, or paying for more infrastructure.

LiteQ uses SQLite as a persistent state machine. Jobs survive crashes, restarts, and deploys. Workers are isolated. Retries are automatic. And the entire thing is a single `npm install`.

```
Your App
   │
   ├── queue.register('send-email', handler)  ← runs once at boot
   │
   └── sendEmail({ to: 'user@example.com' })  ← runs on demand, anywhere
              │
              ▼
        SQLite (WAL mode)
              │
        ┌─────┴──────┐
        │            │
      pending  →  processing  →  completed
                               └→ failed (after retries exhausted)
```

---

## Install

```bash
npm install liteq
```

**Requirements:** Node.js ≥ 18.0.0

---

## Quick Start

```typescript
import { LiteQ } from 'liteq';

const queue = new LiteQ({ storagePath: './jobs.db' });

// Register a handler — returns a typed enqueuer function
const sendEmail = queue.register<{ to: string; subject: string }>(
    'send-email',
    async (job) => {
        await mailer.send(job.data.to, job.data.subject);
        return { sent: true };
    }
);

// Start the polling engine
await queue.start();

// Call the enqueuer from anywhere in your app — fully typed, no raw strings
await sendEmail({ to: 'user@example.com', subject: 'Welcome!' });
```

The string `'send-email'` is written **once** inside `register()`. The returned function is typed to your payload — no magic strings, no mismatches, no silent failures.

---

## How It Works

### The Job Lifecycle

| Step | What Happens |
|---|---|
| **Register** | `queue.register()` stores the handler in memory and returns a typed enqueuer function |
| **Enqueue** | Calling the enqueuer writes a row to SQLite with `status = 'pending'` |
| **Poll** | `queue.start()` begins querying the DB on a configurable interval |
| **Claim** | A worker atomically shifts the row to `'processing'` using `BEGIN IMMEDIATE TRANSACTION` — no two workers ever claim the same job |
| **Success** | Status shifts to `'completed'` |
| **Failure** | Status returns to `'pending'`, attempts increment, `run_at` bumps with exponential backoff |
| **Dead** | After `max_retries` exhausted, status shifts to `'failed'` |

### Crash Recovery

When your app crashes mid-job, SQLite survives. On restart, any job stuck in `'processing'` beyond `jobTimeout` is automatically returned to `'pending'` and retried. **No job is ever silently lost.**

---

## API

### Initialization

```typescript
import { LiteQ } from 'liteq';

const queue = new LiteQ({
    storagePath: './data/jobs.db', // or ':memory:' for tests
    concurrency: 4,                // max concurrent jobs (default: 1)
    pollInterval: 500,             // ms between DB polls (default: 500)
    jobTimeout: 60_000,            // ms before a stuck job is released (default: 60000)
});
```

---

### `queue.register()` — Register a Handler

Registers a handler and returns a **typed enqueuer function**. The job type string lives only here — never repeated anywhere else in your codebase.

#### I/O Bound (async callback)

Use for anything that waits on the network: emails, webhooks, API calls.

```typescript
const sendEmail = queue.register<{ email: string; templateId: string }>(
    'send-transactional-email',
    async (job) => {
        await emailProvider.send(job.data.email, job.data.templateId);
        return { sent: true };
    }
);
```

#### CPU Bound (worker thread)

Pass a file path instead of a callback. LiteQ detects the string and automatically spawns a `worker_thread`, keeping your main event loop completely unblocked.

```typescript
const generatePdf = queue.register('generate-pdf', './workers/pdf-worker.js');
```

```typescript
// workers/pdf-worker.js — runs in an isolated CPU thread
import { parentPort } from 'worker_threads';

parentPort.on('message', async (job) => {
    try {
        const url = await buildAndUploadPdf(job.data);
        parentPort.postMessage({ status: 'success', result: { url } });
    } catch (err) {
        parentPort.postMessage({ status: 'error', error: err.message });
    }
});
```

**The `job` object passed to your handler:**

| Property | Type | Description |
|---|---|---|
| `job.id` | `string` | UUID |
| `job.taskType` | `string` | The registered type name |
| `job.data` | `T` | Your typed payload |
| `job.attempts` | `number` | How many times this job has run |
| `job.maxRetries` | `number` | Max attempts before permanent failure |

---

### Enqueueing Jobs

Use the function returned by `register()`.

```typescript
// Immediate
await sendEmail({ email: 'user@domain.com', templateId: 'welcome_v2' });

// Delayed — run 1 hour from now
await checkTrialExpiry({ userId: 'usr_9011' }, { delay: 60 * 60 * 1000 });

// With custom retry config — retries at 1s → 2s → 4s → 8s → 16s
await syncLedger({ transactionId: 'ch_3Mv1' }, { maxRetries: 5 });
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `delay` | `number` | `0` | Milliseconds before the job becomes eligible to run |
| `maxRetries` | `number` | `3` | Max retry attempts before the job is marked failed |
| `priority` | `number` | `10` | Higher value = higher priority *(v1.2+)* |

---

### Lifecycle Methods

```typescript
await queue.start();  // Begin polling — call once at boot

await queue.stop();   // Graceful shutdown — finishes in-flight jobs first

const stats = await queue.stats();
// { pending: 3, processing: 1, completed: 142, failed: 2, total: 148 }

await queue.purge({ olderThan: 7 * 24 * 60 * 60 * 1000 });
// Removes completed/failed jobs older than 7 days
```

---

## Recommended Project Setup

```typescript
// queue.ts — create the instance once
import { LiteQ } from 'liteq';
export const queue = new LiteQ({ storagePath: './jobs.db' });
```

```typescript
// jobs/index.ts — register all handlers, export enqueuers
import { queue } from '../queue.js';

export const sendEmail = queue.register<{ to: string }>(
    'send-email',
    async (job) => { /* ... */ }
);

export const generateReport = queue.register(
    'generate-report',
    './workers/report.js'
);
```

```typescript
// main.ts — boot
import { queue } from './queue.js';
import './jobs/index.js'; // registers all handlers

await queue.start();
```

```typescript
// anywhere in your app
import { sendEmail } from './jobs/index.js';

await sendEmail({ to: 'user@example.com' });
```

---

## Comparison

| | LiteQ | BullMQ | Bee-Queue |
|---|---|---|---|
| Infrastructure required | **None** | Redis | Redis |
| Persistent jobs | ✅ | ✅ | ❌ |
| Survives crashes | ✅ | ✅ | ❌ |
| CPU thread isolation | ✅ | ❌ | ❌ |
| Delayed scheduling | ✅ | ✅ | ✅ |
| Exponential backoff | ✅ | ✅ | ✅ |
| Zero runtime deps | ✅ | ❌ | ❌ |
| TypeScript built-in | ✅ | ✅ | ❌ |
| Multi-machine workers | ❌ | ✅ | ✅ |

**LiteQ is the right choice when** you want BullMQ-level reliability without operating Redis. If you need workers running across multiple machines, use BullMQ.

---

## Database Internals

LiteQ configures SQLite on startup for maximum concurrency and durability:

```sql
PRAGMA journal_mode = WAL;      -- concurrent readers, single writer
PRAGMA busy_timeout = 5000;     -- wait up to 5s on write contention
PRAGMA synchronous = NORMAL;    -- crash-safe without full fsync overhead
```

```sql
CREATE TABLE IF NOT EXISTS liteq_jobs (
                                          id          TEXT     PRIMARY KEY,
                                          type        TEXT     NOT NULL,
                                          payload     TEXT     NOT NULL,
                                          status      TEXT     NOT NULL DEFAULT 'pending',
                                          attempts    INTEGER  DEFAULT 0,
                                          max_retries INTEGER  DEFAULT 3,
                                          priority    INTEGER  DEFAULT 10,
                                          run_at      INTEGER  NOT NULL,   -- epoch ms, eligible run time
                                          locked_at   INTEGER,             -- set when status = 'processing'
                                          error_log   TEXT
);

-- Prevents full table scans during high-frequency polling
CREATE INDEX IF NOT EXISTS idx_liteq_polling
    ON liteq_jobs (status, run_at, priority DESC);
```

---

## Roadmap

### ✅ v1.0 — Core Engine
SQLite WAL persistence · Atomic job locking · Async handler execution · Worker thread isolation · Exponential backoff · Delayed scheduling · Graceful shutdown

### 🔜 v1.2 — Priority Queues
```typescript
await sendAlert(data, { priority: 100 }); // runs before lower-priority jobs
```

### 🔜 v1.5 — Cron Scheduling
```typescript
queue.schedule('0 0 * * *', 'cleanup-sessions', {});
```

### 🔜 v2.0 — Idempotent Jobs
```typescript
await sendSms(data, {
    uniqueKey: 'sms-verify-+1234567890',
    uniqueWithin: 5 * 60 * 1000,
});
```

### 🔜 v3.0 — Embedded Dashboard
Zero-config HTTP dashboard for queue observability — no external UI framework.

---

## FAQ

**Does this work with multiple processes?**
Yes. WAL mode supports concurrent readers and `BEGIN IMMEDIATE TRANSACTION` ensures no two processes ever claim the same job, even across separate OS processes on the same machine.

**What happens if my app crashes mid-job?**
Any job stuck in `'processing'` beyond `jobTimeout` is automatically returned to `'pending'` on the next restart. Nothing is lost.

**Is it really zero-dependency?**
LiteQ uses `better-sqlite3` as its only runtime dependency — a native, battle-tested SQLite binding with no transitive deps of its own.

**When should I use BullMQ instead?**
When you need workers distributed across multiple machines, or throughput above tens of thousands of jobs per second. LiteQ is intentionally scoped to single-node deployments.

**Is TypeScript required?**
No — works with plain JavaScript too. TypeScript types are bundled; no separate `@types` package needed.

---

## License

MIT © LiteQ Contributors