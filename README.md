<div align="center">

# liteQueue

**A persistent, zero-infrastructure task queue for Node.js — powered by SQLite.**

[![GitHub](https://img.shields.io/badge/GitHub-iikareem/liteQueue-181717?logo=github)](https://github.com/iikareem/liteQueue)
[![node](https://img.shields.io/badge/node-%3E%3D18.0.0-339933?logo=nodedotjs)](https://nodejs.org)
[![license](https://img.shields.io/badge/license-MIT-blue)](./LICENSE)
[![TypeScript](https://img.shields.io/badge/TypeScript-ready-3178C6?logo=typescript)](https://www.typescriptlang.org)
[![npm](https://img.shields.io/badge/npm-v1.0.1-CB3837?logo=npm)](https://www.npmjs.com/package/@iikareem/litequeue)

Delayed scheduling · Atomic job locking · Exponential backoff · CPU thread isolation

**No Redis. No Docker. No infrastructure.**

</div>

---

## Why liteQueue?

Most apps don't need Redis. They need a reliable way to run background jobs without spinning up external services, managing connections, or paying for more infrastructure.

| Feature | External Redis/Infra | liteQueue (SQLite) |
| :--- | :--- | :--- |
| **Visibility** | No visibility into what's running. | You can inspect, pause, and retry jobs. |
| **Control** | Wait for the next poll cycle. | Trigger what is available to run right now. |
| **Performance** | No insight into execution time. | See exactly how much time each job takes. |
| **History** | Jobs are gone once processed. | Full history of completed and failed jobs. |

**Your data stays local.** Because liteQueue uses SQLite, all job data is stored on your local disk rather than being sent over a network. This eliminates:
- **Network Latency:** No round-trips to an external database.
- **TLS Overhead:** No encryption/decryption cycles for every job enqueue.
- **Connection Complexity:** No connection pooling or TCP handshake failures.

Every external dependency adds a new failure domain. liteQueue eliminates all of it — your queue runs in-process. No TCP connections, no serialization hops, no dropped connections to retry.

liteQueue uses SQLite as a persistent state machine. Jobs survive crashes, restarts, and deploys. Workers are isolated. Retries are automatic. And the entire thing is a single `npm install`.

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
        ┌─────┴─────────┐
        │               │
     pending  →  processing  →  completed
                             └→ failed (after retries exhausted)
```

---

## Install

```bash
npm install @iikareem/litequeue
```

**Requirements:** Node.js ≥ 18.0.0

---

## Quick Start

```typescript
import { LiteQ } from '@iikareem/litequeue';

const queue = new LiteQ({ storagePath: './jobs.db' });

// Register an I/O handler — returns a typed enqueuer function
const sendEmail = queue.register<{ to: string; subject: string }>(
    'send-email',
    async (job) => {
        await mailer.send(job.data.to, job.data.subject);
        return { sent: true };
    }
);

// Register a CPU handler — runs in a dedicated worker thread
const generatePdf = queue.register('generate-pdf', './workers/pdf-worker.js');

// Start the polling engine
await queue.start();

// Call the enqueuer from anywhere — fully typed, no raw strings
await sendEmail({ to: 'user@example.com', subject: 'Welcome!' });
await generatePdf({ orderId: 'ord_123' });
```

The string `'send-email'` is written **once** inside `register()`. The returned function is typed to your payload — no magic strings, no mismatches.

---

## How It Works

### The Job Lifecycle

| Step | What Happens |
|---|---|
| **Register** | `queue.register()` stores the handler and determines the execution type: **I/O** (function arg) or **CPU** (string path arg). Returns a typed enqueuer. |
| **Enqueue** | Calling the enqueuer writes a row to SQLite with status `'pending'` and the execution type (`'io'` or `'worker'`). |
| **Poll** | `queue.start()` polls every `pollInterval` ms, running `tryClaimIo()` and `tryClaimCpu()` independently. |
| **Claim — I/O** | `tryClaimIo()` claims jobs where `type = 'io'`, gated by the `concurrency` counter. The handler runs directly on the main thread. |
| **Claim — CPU** | `tryClaimCpu()` claims jobs where `type = 'worker'`, gated by the pool's `canAccept` (idle worker or room to spawn). Dispatched to a generic worker thread. |
| **Success** | Status shifts to `'completed'`. |
| **Failure** | Status returns to `'pending'`, attempts increment, `run_at` bumps with exponential backoff. |
| **Dead** | After `max_retries` exhausted, status shifts to `'failed'`. |

I/O and CPU jobs never block each other — they use independent concurrency controls.

### The Generic Worker Pool

CPU-bound jobs run in a pool of reusable worker threads. The pool is not coupled to handler paths — any idle worker can dynamically import and run any handler module.

```
pool.execute(handlerPath, job)
       │
       ├── idle worker? → dispatch(worker, handlerPath, job)
       │
       ├── room to spawn? → spawn() → dispatch(newWorker, handlerPath, job)
       │
       └── busy + at maxWorkers → queue internally → (dispatched when a worker frees)
```

When a job completes, the worker is marked idle and the pool drains its internal queue. Excess idle workers above `minWorkers` are automatically trimmed.

### Delivery Guarantee & Crash Recovery

liteQueue provides **at least once** delivery — a job will always run, but in rare cases (crash after execution, before the success is committed) it may retry. This means your handlers must be **idempotent**: running the same job twice should produce the same result as running it once.

On restart, any job stuck in `'processing'` beyond `jobTimeout` is returned to `'pending'` and retried. **No job is ever silently lost.**

---

## API

### Initialization

```typescript
import { LiteQ } from '@iikareem/litequeue';

const queue = new LiteQ({
    storagePath: './data/jobs.db', // or ':memory:' for tests
    concurrency: 4,                // max concurrent I/O jobs (default: 1)
    pollInterval: 500,             // ms between DB polls (default: 500)
    jobTimeout: 60_000,            // ms before a stuck job is released (default: 60000)
    minWorkers: 2,                 // min idle worker threads kept alive (default: 1)
    maxWorkers: 4,                 // max concurrent worker threads (default: 4)
});
```

| Option | Type | Default | Description |
|---|---|---|---|
| `storagePath` | `string` | — | SQLite file path (`':memory:'` for tests) |
| `concurrency` | `number` | `1` | Max concurrent I/O jobs on the main thread |
| `pollInterval` | `number` | `500` | ms between DB polls |
| `jobTimeout` | `number` | `60000` | ms before a stuck `'processing'` job is released |
| `minWorkers` | `number` | `1` | Min idle workers to keep alive |
| `maxWorkers` | `number` | `4` | Max concurrent worker threads |

---

### `queue.register()` — Register a Handler

Registers a handler and returns a **typed enqueuer function**. The job type string lives only here — never repeated.

#### I/O Bound (async callback)

Use for state-changing operations that **must** be durable: sending emails, SMS, processing payments, or triggering webhooks. These are tasks where you need at-least-once delivery and built-in retry logic to ensure the work is eventually completed without being "lost" or manually retried.

Because liteQueue is **at least once**, a job may retry if a crash happens after execution but before the success is committed. Pass an **idempotency key** (e.g. `job.id`) to the external provider — it skips the work if it already saw that key.

```typescript
const sendEmail = queue.register<{ email: string; templateId: string }>(
    'send-transactional-email',
    async (job) => {
        // Use job.id as the idempotency key — the provider
        // guarantees it only processes this once.
        await emailProvider.send(job.data.email, job.data.templateId, {
            idempotencyKey: job.id,
        });
        return { sent: true };
    }
);
```

Avoid using this for simple, read-only `fetch` calls that don't require persistence or crash recovery.

`register()` detects the function argument and marks this job with `type = 'io'` in the DB. Concurrency is managed by the `concurrency` counter — these run on the main thread.

#### CPU Bound (worker thread)

Pass a file path instead of a callback. liteQueue detects the string, resolves it to an absolute path, and marks the job with `type = 'worker'`. It runs in the generic worker pool, keeping the main event loop unblocked.

Write a **handler module** — a file that exports a default async function. No `worker_threads` API needed.

```typescript
const generatePdf = queue.register('generate-pdf', './workers/pdf-worker.js');
```

```typescript
// workers/pdf-worker.js — runs in an isolated CPU thread
export default async function (job) {
    const url = await buildAndUploadPdf(job.data);
    return { url };
}
```

Handler modules are dynamically imported by liteQueue's generic worker. Any idle thread can run any handler — the pool is not coupled to paths. Throw inside the handler and the error automatically propagates to liteQueue's retry logic.

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

// With priority — runs before lower-priority jobs
await sendAlert(data, { priority: 100 });
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `delay` | `number` | `0` | Milliseconds before the job becomes eligible |
| `maxRetries` | `number` | `3` | Max retry attempts before permanent failure |
| `priority` | `number` | `10` | Higher value = runs first |

---

### Lifecycle Methods

```typescript
await queue.start();  // Begin polling — call once at boot

await queue.stop();   // Graceful shutdown — drains pool, finishes in-flight jobs

const stats = await queue.stats();
// { pending: 3, processing: 1, completed: 142, failed: 2, total: 148 }

await queue.purge({ olderThan: 7 * 24 * 60 * 60 * 1000 });
// Removes completed/failed jobs older than 7 days
```

---

## Recommended Project Setup

```typescript
// queue.ts — create the instance once
import { LiteQ } from '@iikareem/litequeue';
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

| | liteQueue | BullMQ | Bee-Queue |
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

**liteQueue is the right choice when** you want BullMQ-level reliability without operating Redis. If you need workers across multiple machines, use BullMQ.

---

## Database Internals

liteQueue configures SQLite on startup for maximum concurrency and durability:

```sql
PRAGMA journal_mode = WAL;      -- concurrent readers, single writer
PRAGMA busy_timeout = 5000;     -- wait up to 5s on write contention
PRAGMA synchronous = NORMAL;    -- crash-safe without full fsync overhead
```

The `type` column distinguishes I/O jobs (main thread) from CPU jobs (worker thread), so each claim path queries only its own job type.

```sql
CREATE TABLE liteQueue_jobs (
    id          TEXT     PRIMARY KEY,
    name        TEXT     NOT NULL,         -- job type name: 'send-email', 'resize-image', etc.
    type        TEXT     NOT NULL,         -- execution type: 'io' or 'worker'
    payload     TEXT     NOT NULL,
    status      TEXT     NOT NULL DEFAULT 'pending',
    attempts    INTEGER  DEFAULT 0,
    max_retries INTEGER  DEFAULT 3,
    priority    INTEGER  DEFAULT 10,
    run_at      INTEGER  NOT NULL,         -- epoch ms, eligible run time
    locked_at   INTEGER,                   -- set when status = 'processing'
    error_log   TEXT
);

-- Prevents full table scans during high-frequency polling
CREATE INDEX IF NOT EXISTS idx_liteQueue_polling
    ON liteQueue_jobs (status, type, run_at, priority DESC);
```

---

## Roadmap

### ✅ v1.0 — Core Engine
SQLite WAL persistence · Atomic job locking · I/O + CPU concurrency separation · Generic worker pool with minWorkers/maxWorkers lifecycle · Exponential backoff · Delayed scheduling · Priority queues · Graceful shutdown · Handler modules (no `worker_threads` boilerplate)

### 🔜 v1.1 — Scheduled Jobs
```typescript
queue.schedule('0 0 * * *', 'cleanup-sessions', {});
```

### 🔜 v1.2 — Built-in Idempotency
```typescript
await sendSms(data, {
    uniqueKey: 'sms-verify-+1234567890',
    uniqueWithin: 5 * 60 * 1000,
});
```

### 🔜 v2.0 — Observability Dashboard
Zero-config HTTP dashboard for queue observability — no external UI framework.

---

## FAQ

**Does this work with multiple processes?**
Yes. WAL mode supports concurrent readers and `BEGIN IMMEDIATE TRANSACTION` ensures no two processes ever claim the same job, even across separate OS processes on the same machine.

**What happens if my app crashes mid-job?**
Any job stuck in `'processing'` beyond `jobTimeout` is automatically returned to `'pending'` on the next restart. Because the success may or may not have been committed before the crash, liteQueue provides **at least once** delivery — your handler should use an idempotency key (e.g. `job.id`) to detect and skip duplicates.

**When should I use BullMQ instead?**
When you need workers distributed across multiple machines, or throughput above tens of thousands of jobs per second. liteQueue is intentionally scoped to single-node deployments.

**Is TypeScript required?**
No — works with plain JavaScript too. TypeScript types are bundled; no separate `@types` package needed.

---

## Links

- **Source:** [github.com/iikareem/liteQueue](https://github.com/iikareem/liteQueue)
- **Issues:** [github.com/iikareem/liteQueue/issues](https://github.com/iikareem/liteQueue/issues)

---

## License

MIT © liteQueue Contributors
