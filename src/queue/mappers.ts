import type {ClaimedCronExecution, CronExecution, CronJob, CronJobSummary} from '../cron/types.js';
import type {ClaimedJob} from '../db/index.js';
import type {Job} from '../types.js';

export function toJob(claimed: ClaimedJob, data: unknown): Job {
    return {
        id: claimed.id,
        taskType: claimed.name,
        data,
        attempts: claimed.attempts,
        maxRetries: claimed.maxRetries,
        status: 'processing',
    };
}

export function toJobFromCron(claimed: ClaimedCronExecution): Job {
    return {
        id: claimed.executionId,
        taskType: claimed.name,
        data: JSON.parse(claimed.payload),
        attempts: claimed.attempts,
        maxRetries: claimed.maxRetries,
        status: 'processing',
    };
}

export function toCronJobSummary(job: CronJob, last: CronExecution | null): CronJobSummary {
    return {
        name: job.name,
        expression: job.cronExpression,
        type: job.type,
        enabled: job.enabled,
        nextRunAt: job.nextRunAt,
        maxRetries: job.maxRetries,
        lastStatus: last?.status ?? null,
        lastStartedAt: last?.startedAt ?? null,
        lastCompletedAt: last?.completedAt ?? null,
        lastDurationMs: last?.durationMs ?? null,
        lastError: last?.errorLog ?? null,
    };
}
