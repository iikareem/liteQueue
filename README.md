<div align="center">

# lite-q

**A persistent, zero-infrastructure task queue for Node.js — powered by SQLite.**

[![GitHub](https://img.shields.io/badge/GitHub-iikareem/liteQ-181717?logo=github)](https://github.com/iikareem/liteQ)
[![node](https://img.shields.io/badge/node-%3E%3D18.0.0-339933?logo=nodedotjs)](https://nodejs.org)
[![license](https://img.shields.io/badge/license-MIT-blue)](./LICENSE)
[![TypeScript](https://img.shields.io/badge/TypeScript-ready-3178C6?logo=typescript)](https://www.typescriptlang.org)
[![npm](https://img.shields.io/badge/npm-v1.0.3-CB3837?logo=npm)](https://www.npmjs.com/package/@km-dev/lite-q)

Delayed scheduling · Cron jobs · Atomic job locking · Exponential backoff · CPU thread isolation

**No Redis. No Docker. No infrastructure.**

</div>

---

## Why lite-q?

Most apps don't need Redis. They need a reliable way to run background jobs without spinning up external services, managing connections, or paying for more infrastructure.

| Feature | External Redis/Infra | lite-q (SQLite) |
| :--- | :--- | :--- |
| **Visibility** | No visibility into what's running. | You can inspect, pause, and retry jobs. |
| **Control** | Wait for the next poll cycle. | Trigger what is available to run right now. |
| **Performance** | No insight into execution time. | See exactly how much time each job takes. |
| **History** | Jobs are gone once processed. | Full history of completed and failed jobs. |

**Your data stays local.** Because lite-q uses SQLite, all job data is stored on your local disk rather than being sent over a network. This eliminates:
- **Network Latency:** No round-trips to an external database.
- **TLS Overhead:** No encryption/decryption cycles for every job enqueue.
- **Connection Complexity:** No connection pooling or TCP handshake failures.

Every external dependency adds a new failure domain. lite-q eliminates all of it — your queue runs in-process. No TCP connections, no serialization hops, no dropped connections to retry.

lite-q uses SQLite as a persistent state machine. Jobs survive crashes, restarts, and deploys. Workers are isolated. Retries are automatic. And the entire thing is a single `npm install`.

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
npm install @km-dev/lite-q
```

**Requirements:** Node.js ≥ 18.0.0

---

## Quick Start

```typescript
import { LiteQ } from '@km-dev/lite-q';

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

// Schedule a recurring task — handler + cron expression in one call
const cleanup = queue.cron('cleanup-sessions', '0 0 * * *', async (job) => {
    await db.deleteExpiredSessions(job.data);
});

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

lite-q provides **at least once** delivery — a job will always run, but in rare cases (crash after execution, before the success is committed) it may retry. This means your handlers must be **idempotent**: running the same job twice should produce the same result as running it once.

On restart, any job stuck in `'processing'` beyond `jobTimeout` is returned to `'pending'` and retried. **No job is ever silently lost.**

---

## API

### Initialization

```typescript
import { LiteQ } from '@km-dev/lite-q';

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

Because lite-q is **at least once**, a job may retry if a crash happens after execution but before the success is committed. Pass an **idempotency key** (e.g. `job.id`) to the external provider — it skips the work if it already saw that key.

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

Pass a file path instead of a callback. lite-q detects the string, resolves it to an absolute path, and marks the job with `type = 'worker'`. It runs in the generic worker pool, keeping the main event loop unblocked.

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

Handler modules are dynamically imported by lite-q's generic worker. Any idle thread can run any handler — the pool is not coupled to paths. Throw inside the handler and the error automatically propagates to lite-q's retry logic.

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

### Scheduled Jobs (Cron)

lite-q supports **persistent recurring schedules** alongside one-off jobs. Each schedule stores its cron expression, next run time, and handler binding in SQLite. Every fire creates a separate **execution** row with timing, status, and error logs — so you get full history without mixing cron runs into the regular job queue.

Cron uses the same I/O vs CPU execution model as `register()`: pass a function for main-thread I/O work, or a file path for worker-thread CPU work.

Exported types: `CronHandle`, `CronExecution`, `CronStats`, `CronJobSummary`, `CronOptions`, `CronOptionsFor`.

#### Two ways to register

**`queue.cron()`** — register a handler and schedule in one call. Best for dedicated recurring tasks.

```typescript
// I/O handler — runs on the main thread
const cleanup = queue.cron<{ batchSize: number }>(
    'cleanup-sessions',
    '0 0 * * *', // daily at midnight
    async (job) => {
        await db.deleteExpiredSessions(job.data.batchSize);
    },
    { payload: { batchSize: 500 } },
);

// CPU handler — runs in a worker thread
queue.cron('generate-report', '0 6 * * 1', './workers/report.js', {
    payload: { format: 'pdf' },
});
```

**`queue.schedule()`** — attach a schedule to a handler already registered via `queue.register()`. Best when the same handler serves both on-demand enqueues and scheduled runs.

```typescript
const syncLedger = queue.register('sync-ledger', async (job) => {
    await ledger.sync(job.data);
});

// Also run every 6 hours on a schedule
const syncSchedule = queue.schedule('sync-ledger', '0 */6 * * *', {
    payload: { source: 'scheduled' },
});

// On demand — dynamic payload per call
await syncLedger({ accountId: 'acc_123' });
```

#### Cron expressions

Expressions are validated at registration time via [cron-parser](https://www.npmjs.com/package/cron-parser). Standard 5-field and 6-field (with seconds) formats are supported.

| Expression | Meaning |
|---|---|
| `0 0 * * *` | Daily at midnight |
| `0 */6 * * *` | Every 6 hours |
| `0 9 * * 1` | Mondays at 9:00 |
| `*/30 * * * * *` | Every 30 seconds (6-field) |

Invalid expressions throw at registration time.

#### Options

| Option | Type | Default | Description |
|---|---|---|---|
| `payload` | `unknown` | `{}` | Static data passed as `job.data` on every run |
| `enabled` | `boolean` | `true` | Set `false` to register paused |
| `maxRetries` | `number` | `3` | Retries within a single execution (immediate retry loop, not delayed backoff) |

Use `CronOptionsFor<T>` when you want `payload` typed against your handler data.

#### The `job` object in cron handlers

Cron handlers receive the same `job` shape as regular jobs:

| Property | Type | Description |
|---|---|---|
| `job.id` | `string` | Execution UUID (not the schedule id) |
| `job.taskType` | `string` | The schedule name |
| `job.data` | `T` | Static payload from `options.payload` |
| `job.attempts` | `number` | How many times this execution has run |
| `job.maxRetries` | `number` | Max attempts before permanent failure |

#### `CronHandle` controls

Both `queue.cron()` and `queue.schedule()` return a `CronHandle`:

```typescript
const handle = queue.cron('cleanup-sessions', '0 0 * * *', async (job) => { /* ... */ });

// Run immediately — blocks until done, does not advance next_run_at
const execution = await handle.trigger();

// Pause / resume the schedule (in-flight execution continues)
await handle.pause();
await handle.resume();

// Execution history for this schedule
const history = await handle.executions({ limit: 20 });
```

#### Execution lifecycle

| Step | What happens |
|---|---|
| **Register** | Schedule row written to `lite_q_cron_jobs`; `next_run_at` computed |
| **Tick (due)** | New execution row created; handler runs |
| **Overlap** | If already running, skip the run but advance `next_run_at` |
| **Manual trigger** | New execution via `trigger()`; schedule timing unchanged |
| **Success / failure** | Execution row updated; retries until `maxRetries` exhausted |
| **Pause** | No new scheduled runs; in-flight execution continues |
| **Stale recovery** | Executions stuck in `'processing'` beyond `jobTimeout` are retried or failed |

Cron ticks run in the same poll loop as regular jobs: stale cron recovery → job I/O → cron I/O → job CPU → cron CPU.

#### Best practices

- Handlers must be **idempotent** — lite-q provides at-least-once delivery, and overlap edge cases can skip a scheduled fire while advancing `next_run_at`.
- Use `payload` for static config (batch size, report format). For dynamic per-run data, use `register()` + enqueue instead.
- Use `trigger()` for admin or debug runs. Use `pause()` before deploys if you need to prevent new scheduled fires.
- Re-call `queue.cron()` or `queue.schedule()` at boot to re-bind handlers after a restart — schedule rows persist in SQLite, but handler functions live in memory.

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

#### Cron observability

```typescript
const cronStats = await queue.cronStats();
// {
//   schedules: 3,
//   enabled: 2,
//   disabled: 1,
//   executions: { pending: 0, processing: 1, completed: 48, failed: 2, total: 51 }
// }

const crons = await queue.listCrons();
// [
//   {
//     name: 'cleanup-sessions',
//     expression: '0 0 * * *',
//     type: 'io',
//     enabled: true,
//     nextRunAt: 1719792000000,
//     maxRetries: 3,
//     lastStatus: 'completed',
//     lastStartedAt: 1719705600000,
//     lastCompletedAt: 1719705605123,
//     lastDurationMs: 5123,
//     lastError: null,
//   },
//   ...
// ]

const history = await queue.cronExecutions('cleanup-sessions', { limit: 50 });

await queue.purgeCronExecutions({ olderThan: 30 * 24 * 60 * 60 * 1000 });
// Deletes completed/failed executions older than 30 days
// olderThan is a duration in ms (converted to a cutoff timestamp internally)
```

---

## Recommended Project Setup

```typescript
// queue.ts — create the instance once
import { LiteQ } from '@km-dev/lite-q';
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

export const cleanupSessions = queue.cron<{ batchSize: number }>(
    'cleanup-sessions',
    '0 0 * * *',
    async (job) => { /* ... */ },
    { payload: { batchSize: 500 } },
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

| | lite-q | BullMQ | Bee-Queue |
|---|---|---|---|
| Infrastructure required | **None** | Redis | Redis |
| Persistent jobs | ✅ | ✅ | ❌ |
| Survives crashes | ✅ | ✅ | ❌ |
| CPU thread isolation | ✅ | ❌ | ❌ |
| Delayed scheduling | ✅ | ✅ | ✅ |
| Cron / scheduled jobs | ✅ | ✅ | ❌ |
| Exponential backoff | ✅ | ✅ | ✅ |
| Zero runtime deps | ✅ | ❌ | ❌ |
| TypeScript built-in | ✅ | ✅ | ❌ |
| Multi-machine workers | ❌ | ✅ | ✅ |

**lite-q is the right choice when** you want BullMQ-level reliability without operating Redis. If you need workers across multiple machines, use BullMQ.

---

## Database Internals

lite-q configures SQLite on startup for maximum concurrency and durability:

```sql
PRAGMA journal_mode = WAL;      -- concurrent readers, single writer
PRAGMA busy_timeout = 5000;     -- wait up to 5s on write contention
PRAGMA synchronous = NORMAL;    -- crash-safe without full fsync overhead
```

The `type` column distinguishes I/O jobs (main thread) from CPU jobs (worker thread), so each claim path queries only its own job type.

```sql
CREATE TABLE lite_q_jobs (
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
CREATE INDEX IF NOT EXISTS idx_lite_q_polling
    ON lite_q_jobs (status, type, run_at, priority DESC);
```

Cron schedules and executions use separate tables in the same SQLite file, with the same WAL pragmas:

```sql
CREATE TABLE lite_q_cron_jobs (
    id              TEXT    PRIMARY KEY,
    name            TEXT    NOT NULL UNIQUE,   -- schedule name: 'cleanup-sessions', etc.
    cron_expression TEXT    NOT NULL,
    type            TEXT    NOT NULL,          -- execution type: 'io' or 'worker'
    payload         TEXT    NOT NULL DEFAULT '{}',
    enabled         INTEGER NOT NULL DEFAULT 1,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    next_run_at     INTEGER NOT NULL,          -- epoch ms, next scheduled fire
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE TABLE lite_q_cron_executions (
    id           TEXT    PRIMARY KEY,
    cron_job_id  TEXT    NOT NULL,
    status       TEXT    NOT NULL DEFAULT 'pending',
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_retries  INTEGER NOT NULL DEFAULT 3,
    started_at   INTEGER,
    completed_at INTEGER,
    duration_ms  INTEGER,
    error_log    TEXT,
    FOREIGN KEY (cron_job_id) REFERENCES lite_q_cron_jobs(id)
);

CREATE INDEX IF NOT EXISTS idx_lite_q_cron_due
    ON lite_q_cron_jobs (enabled, next_run_at);

CREATE INDEX IF NOT EXISTS idx_lite_q_cron_executions_job
    ON lite_q_cron_executions (cron_job_id, started_at DESC);
```

---

## Roadmap

### ✅ v1.0 — Core Engine
SQLite WAL persistence · Atomic job locking · I/O + CPU concurrency separation · Generic worker pool with minWorkers/maxWorkers lifecycle · Exponential backoff · Delayed scheduling · Priority queues · Graceful shutdown · Handler modules (no `worker_threads` boilerplate)

### ✅ v1.1 — Scheduled Jobs
`queue.cron()` and `queue.schedule()` · Persistent cron expressions · Per-run execution history · `CronHandle` (trigger, pause, resume) · Overlap skip · `cronStats()`, `listCrons()`, `cronExecutions()`, `purgeCronExecutions()`


### 🔜 v2.0 — Observability Dashboard
Zero-config HTTP dashboard for queue observability — no external UI framework.

---

## FAQ

**Does this work with multiple processes?**
Yes. WAL mode supports concurrent readers and `BEGIN IMMEDIATE TRANSACTION` ensures no two processes ever claim the same job, even across separate OS processes on the same machine.

**What happens if my app crashes mid-job?**
Any job stuck in `'processing'` beyond `jobTimeout` is automatically returned to `'pending'` on the next restart. Because the success may or may not have been committed before the crash, lite-q provides **at least once** delivery — your handler should use an idempotency key (e.g. `job.id`) to detect and skip duplicates.

**When should I use BullMQ instead?**
When you need workers distributed across multiple machines, or throughput above tens of thousands of jobs per second. lite-q is intentionally scoped to single-node deployments.

**Is TypeScript required?**
No — works with plain JavaScript too. TypeScript types are bundled; no separate `@types` package needed.

**When should I use cron vs enqueue?**
Use `queue.cron()` or `queue.schedule()` for **recurring** tasks with a **static** payload (config, batch size, report format). Use `register()` + enqueue for **one-off** jobs with **dynamic** data per call (user id, order id, etc.).

**What if a cron run takes longer than the interval?**
lite-q allows only one execution per schedule at a time. If a run is still `'processing'` when the next tick fires, the scheduled fire is skipped but `next_run_at` is advanced — no overlapping runs pile up.

**Do cron schedules survive restarts?**
Yes. Schedule rows persist in SQLite. Handler functions live in memory, so re-call `queue.cron()` or `queue.schedule()` at boot to re-bind them (same as `register()`).

---

## Links
- **Source:** [github.com/iikareem/liteQ](https://github.com/iikareem/liteQ)

- **Issues:** [github.com/iikareem/liteQ/issues](https://github.com/iikareem/liteQ/issues)

---

## License

MIT © lite-q Contributors
