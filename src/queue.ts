import {randomUUID} from 'node:crypto';
import os from 'node:os';
import {resolve} from 'node:path';
import type {ClaimedJob, ExecType} from './db.js';
import {DB} from './db.js';
import type {EnqueueOptions, Enqueuer, Job, JobHandler, LiteQOptions, PurgeOptions, QueueStats,} from './types.js';
import {WorkerPool} from './worker-pool.js';

const CPU_COUNT = os.cpus().length;
const DEFAULT_CONCURRENCY = 1;
const DEFAULT_POLL_INTERVAL = 500;
const DEFAULT_JOB_TIMEOUT = 60_000;
const DEFAULT_MIN_WORKERS = 1;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_PRIORITY = 10;

export class LiteQ {
    private readonly db: DB;
    private readonly handlers = new Map<string, JobHandler | string>();
    private readonly concurrency: number;
    private readonly pollInterval: number;
    private readonly jobTimeout: number;
    private readonly pool: WorkerPool;
    private activeIo = 0;
    private timer: ReturnType<typeof setInterval> | null = null;

    constructor(options: LiteQOptions) {
        this.db = new DB(options.storagePath);

        this.concurrency = options.concurrency ?? DEFAULT_CONCURRENCY;
        this.pollInterval = options.pollInterval ?? DEFAULT_POLL_INTERVAL;
        this.jobTimeout = options.jobTimeout ?? DEFAULT_JOB_TIMEOUT;

        const minWorkers = options.minWorkers ?? DEFAULT_MIN_WORKERS;
        const maxWorkers = resolveMaxWorkers(options.maxWorkers);
        this.pool = new WorkerPool(minWorkers, maxWorkers);
    }

    register<T>(type: string, handler: JobHandler<T>): Enqueuer<T>;
    register(type: string, workerPath: string): Enqueuer<unknown>;
    register<T>(type: string, handlerOrPath: JobHandler<T> | string): Enqueuer<T> {
        if (typeof handlerOrPath === 'string') {
            this.handlers.set(type, resolve(handlerOrPath));
            return (data: T, options?: EnqueueOptions) =>
                this.enqueue(type, data, options, 'worker');
        }

        this.handlers.set(type, handlerOrPath as JobHandler);
        return (data: T, options?: EnqueueOptions) =>
            this.enqueue(type, data, options, 'io');
    }

    async start(): Promise<void> {
        this.timer = setInterval(() => this.tick(), this.pollInterval);
    }

    async stop(): Promise<void> {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
        await this.pool.stop();
    }

    async stats(): Promise<QueueStats> {
        const rows = this.db.stats();
        let total = 0;
        const stats: QueueStats = {pending: 0, processing: 0, completed: 0, failed: 0, total: 0};

        for (const row of rows) {
            const count = Number(row.count);
            if (row.status === 'pending') stats.pending = count;
            else if (row.status === 'processing') stats.processing = count;
            else if (row.status === 'completed') stats.completed = count;
            else if (row.status === 'failed') stats.failed = count;
            total += count;
        }

        stats.total = total;
        return stats;
    }

    async purge(options: PurgeOptions): Promise<void> {
        this.db.purge(options.olderThan);
    }

    private async enqueue<T>(
        name: string,
        data: T,
        options: EnqueueOptions | undefined,
        execType: ExecType,
    ): Promise<Job<T>> {
        const id = randomUUID();
        const maxRetries = options?.maxRetries ?? DEFAULT_MAX_RETRIES;
        const runAt = Date.now() + (options?.delay ?? 0);

        this.db.enqueue({
            id,
            name,
            type: execType,
            payload: JSON.stringify(data),
            runAt,
            maxRetries,
            priority: options?.priority ?? DEFAULT_PRIORITY,
        });

        return {
            id,
            taskType: name,
            data,
            attempts: 0,
            maxRetries,
            status: 'pending',
        };
    }

    private tick(): void {
        this.tryClaimIo();
        this.tryClaimCpu();
    }

    private tryClaimIo(): void {
        if (this.activeIo >= this.concurrency) return;

        const claimed = this.db.claimNext(Date.now(), this.jobTimeout, 'io');
        if (!claimed) return;

        this.activeIo++;
        this.runJob(claimed).finally(() => this.activeIo--);
    }

    private tryClaimCpu(): void {
        if (!this.pool.canAccept) return;

        const claimed = this.db.claimNext(Date.now(), this.jobTimeout, 'worker');
        if (!claimed) return;

        this.runJob(claimed);
    }

    private async runJob(claimed: ClaimedJob): Promise<void> {
        const handler = this.handlers.get(claimed.name);
        if (!handler) {
            this.db.fail(
                claimed.id,
                `No handler registered for job type: ${claimed.name}`,
                Date.now(),
            );
            return;
        }

        const job = toJob(claimed, JSON.parse(claimed.payload));

        try {
            await this.withTimeout(this.executeHandler(handler, job), this.jobTimeout);
            this.db.complete(claimed.id, Date.now());
        } catch (err) {
            this.handleJobFailure(claimed, err);
        }
    }

    private executeHandler(handler: JobHandler | string, job: Job): Promise<unknown> {
        if (typeof handler === 'function') {
            return handler(job);
        }
        return this.pool.execute(handler, job);
    }

    private handleJobFailure(claimed: ClaimedJob, err: unknown): void {
        const errorMsg = err instanceof Error ? err.message : String(err);

        if (claimed.attempts < claimed.maxRetries) {
            const backoffMs = Math.pow(2, claimed.attempts) * 1000;
            this.db.retry(claimed.id, Date.now() + backoffMs);
            return;
        }

        this.db.fail(claimed.id, errorMsg, Date.now());
    }

    private withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
        let timeoutId: ReturnType<typeof setTimeout>;

        const timeoutPromise = new Promise<never>((_, reject) => {
            timeoutId = setTimeout(
                () => reject(new Error(`Job timed out after ${ms}ms`)),
                ms,
            );
        });

        return Promise.race([promise, timeoutPromise]).finally(() => {
            clearTimeout(timeoutId);
        });
    }
}

function toJob(claimed: ClaimedJob, data: unknown): Job {
    return {
        id: claimed.id,
        taskType: claimed.name,
        data,
        attempts: claimed.attempts,
        maxRetries: claimed.maxRetries,
        status: 'processing',
    };
}

function resolveMaxWorkers(requested?: number): number {
    const osReserved = Math.max(1, CPU_COUNT - 1);

    if (requested === undefined) {
        return Math.max(1, Math.floor(osReserved / 2));
    }

    if (requested > CPU_COUNT) {
        throw new Error(
            `Your machine only has ${CPU_COUNT} cores, so you cannot use ${requested} workers.`,
        );
    }

    if (requested > osReserved) {
        console.warn(
            `Warning: Setting workers to ${requested} leaves no room for the OS. Recommended max is ${osReserved}.`,
        );
    }

    return requested;
}
