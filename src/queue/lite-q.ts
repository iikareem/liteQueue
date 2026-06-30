import {CronScheduler} from '../cron/scheduler.js';
import type {
    CronExecution,
    CronExecutionQuery,
    CronHandle,
    CronJobSummary,
    CronOptions,
    CronOptionsFor,
    CronPurgeOptions,
    CronStats,
} from '../cron/types.js';
import {CronDB, DB} from '../db/index.js';
import type {EnqueueOptions, Enqueuer, JobHandler, LiteQOptions, PurgeOptions, QueueStats} from '../types.js';
import {WorkerPool} from '../worker-pool.js';
import {
    DEFAULT_CONCURRENCY,
    DEFAULT_JOB_TIMEOUT,
    DEFAULT_MIN_WORKERS,
    DEFAULT_POLL_INTERVAL,
    resolveMaxWorkers,
} from './constants.js';
import type {QueueContext} from './context.js';
import {JobRunner} from './job-runner.js';

export class LiteQ {
    private readonly ctx: QueueContext;
    private readonly jobs: JobRunner;
    private readonly cronScheduler: CronScheduler;
    private readonly pollInterval: number;
    private timer: ReturnType<typeof setInterval> | null = null;

    constructor(options: LiteQOptions) {
        const db = new DB(options.storagePath);
        const cronDb = new CronDB(options.storagePath);
        const concurrency = options.concurrency ?? DEFAULT_CONCURRENCY;
        const jobTimeout = options.jobTimeout ?? DEFAULT_JOB_TIMEOUT;
        const minWorkers = options.minWorkers ?? DEFAULT_MIN_WORKERS;
        const maxWorkers = resolveMaxWorkers(options.maxWorkers);

        this.ctx = {
            db,
            cronDb,
            pool: new WorkerPool(minWorkers, maxWorkers),
            handlers: new Map(),
            activeIo: {count: 0},
            concurrency,
            jobTimeout,
        };

        this.jobs = new JobRunner(this.ctx);
        this.cronScheduler = new CronScheduler(this.ctx);
        this.pollInterval = options.pollInterval ?? DEFAULT_POLL_INTERVAL;
    }

    register<T>(type: string, handler: JobHandler<T>): Enqueuer<T>;
    register(type: string, workerPath: string): Enqueuer<unknown>;
    register<T>(type: string, handlerOrPath: JobHandler<T> | string): Enqueuer<T> {
        if (typeof handlerOrPath === 'string') {
            return this.jobs.register(type, handlerOrPath) as Enqueuer<T>;
        }
        return this.jobs.register(type, handlerOrPath as JobHandler) as Enqueuer<T>;
    }

    cron<T>(name: string, expression: string, handler: JobHandler<T>, options?: CronOptionsFor<T>): CronHandle;
    cron<T>(name: string, expression: string, workerPath: string, options?: CronOptionsFor<T>): CronHandle;
    cron<T>(
        name: string,
        expression: string,
        handlerOrPath: JobHandler<T> | string,
        options?: CronOptionsFor<T>,
    ): CronHandle {
        return this.cronScheduler.register(name, expression, handlerOrPath, options);
    }

    schedule(name: string, expression: string, options?: CronOptions): CronHandle {
        return this.cronScheduler.schedule(name, expression, options);
    }

    async cronStats(): Promise<CronStats> {
        return this.cronScheduler.stats();
    }

    async listCrons(): Promise<CronJobSummary[]> {
        return this.cronScheduler.listCrons();
    }

    async cronExecutions(name: string, query?: CronExecutionQuery): Promise<CronExecution[]> {
        return this.cronScheduler.executions(name, query);
    }

    async purgeCronExecutions(options: CronPurgeOptions): Promise<void> {
        return this.cronScheduler.purgeExecutions(options);
    }

    async start(): Promise<void> {
        this.timer = setInterval(() => this.tick(), this.pollInterval);
    }

    async stop(): Promise<void> {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
        await this.ctx.pool.stop();
        this.ctx.db.close();
        this.ctx.cronDb.close();
    }

    async stats(): Promise<QueueStats> {
        return this.jobs.stats();
    }

    async purge(options: PurgeOptions): Promise<void> {
        return this.jobs.purge(options);
    }

    private tick(): void {
        this.cronScheduler.recoverStale();
        this.jobs.tickIo();
        this.cronScheduler.tickIo();
        this.jobs.tickCpu();
        this.cronScheduler.tickCpu();
    }
}
