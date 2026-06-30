import {randomUUID} from 'node:crypto';
import {resolve} from 'node:path';
import {CronHandleImpl} from './handle.js';
import {nextRunAfter} from './parser.js';
import type {
    ClaimedCronExecution,
    CronExecution,
    CronExecutionQuery,
    CronHandle,
    CronJob,
    CronJobSummary,
    CronOptions,
    CronOptionsFor,
    CronPurgeOptions,
    CronStats,
} from './types.js';
import type {ExecType} from '../db/index.js';
import type {JobHandler} from '../types.js';
import {DEFAULT_MAX_RETRIES} from '../queue/constants.js';
import type {QueueContext} from '../queue/context.js';
import {executeHandler, withTimeout} from '../queue/executor.js';
import {toCronJobSummary, toJobFromCron} from '../queue/mappers.js';

export class CronScheduler {
    constructor(private readonly ctx: QueueContext) {}

    register<T>(
        name: string,
        expression: string,
        handlerOrPath: JobHandler<T> | string,
        options?: CronOptionsFor<T>,
    ): CronHandle {
        const execType: ExecType = typeof handlerOrPath === 'string' ? 'worker' : 'io';
        const handler = execType === 'worker'
            ? resolve(handlerOrPath as string)
            : (handlerOrPath as JobHandler);

        return this.registerCron(name, expression, handler, execType, options);
    }

    schedule(name: string, expression: string, options?: CronOptions): CronHandle {
        const handler = this.ctx.handlers.get(name);
        if (!handler) {
            throw new Error(`No handler registered for job type: ${name}`);
        }

        const execType: ExecType = typeof handler === 'string' ? 'worker' : 'io';
        return this.registerCron(name, expression, handler, execType, options);
    }

    async stats(): Promise<CronStats> {
        const {total, enabled} = this.ctx.cronDb.cronJobCounts();
        const rows = this.ctx.cronDb.executionStats();

        const executions = {pending: 0, processing: 0, completed: 0, failed: 0, total: 0};
        let executionTotal = 0;

        for (const row of rows) {
            const count = Number(row.count);
            if (row.status === 'pending') executions.pending = count;
            else if (row.status === 'processing') executions.processing = count;
            else if (row.status === 'completed') executions.completed = count;
            else if (row.status === 'failed') executions.failed = count;
            executionTotal += count;
        }

        executions.total = executionTotal;

        return {
            schedules: total,
            enabled,
            disabled: total - enabled,
            executions,
        };
    }

    async listCrons(): Promise<CronJobSummary[]> {
        return this.ctx.cronDb.listCronJobs().map((job) =>
            toCronJobSummary(job, this.ctx.cronDb.getLastExecution(job.id)),
        );
    }

    async executions(name: string, query?: CronExecutionQuery): Promise<CronExecution[]> {
        const limit = query?.limit ?? 20;
        return this.ctx.cronDb.listExecutionsByCronName(name, limit);
    }

    async purgeExecutions(options: CronPurgeOptions): Promise<void> {
        const before = Date.now() - options.olderThan;
        this.ctx.cronDb.purgeExecutions(before);
    }

    private registerCron(
        name: string,
        expression: string,
        handler: JobHandler | string,
        execType: ExecType,
        options?: CronOptions,
    ): CronHandle {
        this.ctx.handlers.set(name, handler);

        const now = Date.now();
        const nextRunAt = nextRunAfter(expression, now);
        const existing = this.ctx.cronDb.getCronJobByName(name);

        this.ctx.cronDb.upsertCronJob({
            id: existing?.id ?? randomUUID(),
            name,
            cronExpression: expression,
            type: execType,
            payload: JSON.stringify(options?.payload ?? {}),
            enabled: options?.enabled === false ? 0 : 1,
            maxRetries: options?.maxRetries ?? DEFAULT_MAX_RETRIES,
            nextRunAt,
            createdAt: existing?.createdAt ?? now,
            updatedAt: now,
        });

        return new CronHandleImpl(name, expression, this.ctx.cronDb, {
            trigger: () => this.trigger(name),
            pause: () => this.pause(name),
            resume: () => this.resume(name),
        });
    }

    private async trigger(name: string): Promise<CronExecution> {
        const cronJob = this.ctx.cronDb.getCronJobByName(name);
        if (!cronJob) {
            throw new Error(`Cron job not found: ${name}`);
        }
        if (!cronJob.enabled) {
            throw new Error(`Cron job is paused: ${name}`);
        }

        const executionId = randomUUID();
        const now = Date.now();
        const claimed = this.ctx.cronDb.beginManualExecution(cronJob, executionId, now);
        if (!claimed) {
            throw new Error(`Cron job already running: ${name}`);
        }

        await this.runExecution(claimed);

        const execution = this.ctx.cronDb.getExecutionById(executionId);
        if (!execution) {
            throw new Error(`Cron execution not found: ${executionId}`);
        }

        return execution;
    }

    private async pause(name: string): Promise<void> {
        const now = Date.now();
        this.ctx.cronDb.setCronJobEnabled(name, false, now);
    }

    private async resume(name: string): Promise<void> {
        const now = Date.now();
        this.ctx.cronDb.setCronJobEnabled(name, true, now);
    }

    recoverStale(): void {
        const now = Date.now();
        const claimed = this.ctx.cronDb.claimStaleExecution(this.ctx.jobTimeout, now);
        if (!claimed) return;

        if (claimed.type === 'io') {
            if (this.ctx.activeIo.count >= this.ctx.concurrency) return;

            this.ctx.activeIo.count++;
            this.runExecution(claimed).finally(() => this.ctx.activeIo.count--);
            return;
        }

        if (!this.ctx.pool.canAccept) return;

        this.runExecution(claimed);
    }

    tickIo(): void {
        if (this.ctx.activeIo.count >= this.ctx.concurrency) return;

        const now = Date.now();
        const due = this.findDueCronJob(now, 'io');
        if (!due) return;

        this.fireScheduled(due, now);
    }

    tickCpu(): void {
        if (!this.ctx.pool.canAccept) return;

        const now = Date.now();
        const due = this.findDueCronJob(now, 'worker');
        if (!due) return;

        this.fireScheduled(due, now);
    }

    private findDueCronJob(now: number, type: ExecType): CronJob | undefined {
        return this.ctx.cronDb.listDueCronJobs(now).find((job) => job.type === type);
    }

    private fireScheduled(cronJob: CronJob, now: number): void {
        const nextRunAt = nextRunAfter(cronJob.cronExpression, cronJob.nextRunAt);
        const executionId = randomUUID();
        const claimed = this.ctx.cronDb.beginExecution(cronJob, executionId, now, nextRunAt);

        if (!claimed) {
            this.ctx.cronDb.updateNextRunAt(cronJob.id, nextRunAt, now);
            return;
        }

        if (cronJob.type === 'io') {
            this.ctx.activeIo.count++;
            this.runExecution(claimed).finally(() => this.ctx.activeIo.count--);
            return;
        }

        this.runExecution(claimed);
    }

    private async runExecution(claimed: ClaimedCronExecution): Promise<void> {
        let current = claimed;

        while (true) {
            const handler = this.ctx.handlers.get(current.name);
            if (!handler) {
                const now = Date.now();
                this.ctx.cronDb.failExecution(
                    current.executionId,
                    `No handler registered for cron: ${current.name}`,
                    now,
                    now - current.startedAt,
                );
                return;
            }

            const job = toJobFromCron(current);

            try {
                await withTimeout(
                    executeHandler(this.ctx.pool, handler, job),
                    this.ctx.jobTimeout,
                );
                const now = Date.now();
                this.ctx.cronDb.completeExecution(current.executionId, now, now - current.startedAt);
                return;
            } catch (err) {
                const errorMsg = err instanceof Error ? err.message : String(err);

                if (current.attempts < current.maxRetries) {
                    this.ctx.cronDb.retryExecution(current.executionId);
                    const now = Date.now();
                    this.ctx.cronDb.startExecution(current.executionId, now);
                    current = {
                        ...current,
                        attempts: current.attempts + 1,
                        startedAt: now,
                    };
                    continue;
                }

                const now = Date.now();
                this.ctx.cronDb.failExecution(
                    current.executionId,
                    errorMsg,
                    now,
                    now - current.startedAt,
                );
                return;
            }
        }
    }
}
