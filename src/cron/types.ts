import type {ExecType} from '../db/types.js';

export type CronExecutionStatus = 'pending' | 'processing' | 'completed' | 'failed';

export interface CronJob {
    id: string;
    name: string;
    cronExpression: string;
    type: ExecType;
    payload: unknown;
    enabled: boolean;
    maxRetries: number;
    nextRunAt: number;
    createdAt: number;
    updatedAt: number;
}

export interface CronExecution {
    id: string;
    cronJobId: string;
    cronName: string;
    status: CronExecutionStatus;
    attempts: number;
    maxRetries: number;
    startedAt: number | null;
    completedAt: number | null;
    durationMs: number | null;
    errorLog: string | null;
}

export interface CronOptions {
    /** Static data passed to the handler on every run. Defaults to `{}`. */
    payload?: unknown;
    enabled?: boolean;
    maxRetries?: number;
}

/** Typed variant of {@link CronOptions} when you want `payload` inferred from `T`. */
export type CronOptionsFor<T> = Omit<CronOptions, 'payload'> & {
    payload?: T;
};

export interface CronExecutionQuery {
    limit?: number;
}

export interface CronStats {
    schedules: number;
    enabled: number;
    disabled: number;
    executions: {
        pending: number;
        processing: number;
        completed: number;
        failed: number;
        total: number;
    };
}

export interface CronJobSummary {
    name: string;
    expression: string;
    type: ExecType;
    enabled: boolean;
    nextRunAt: number;
    maxRetries: number;
    lastStatus: CronExecutionStatus | null;
    lastStartedAt: number | null;
    lastCompletedAt: number | null;
    lastDurationMs: number | null;
    lastError: string | null;
}

export interface CronPurgeOptions {
    /** Delete completed/failed executions with `completed_at` older than this many ms. */
    olderThan: number;
}

export interface CronHandle {
    readonly name: string;
    readonly expression: string;

    trigger(): Promise<CronExecution>;
    pause(): Promise<void>;
    resume(): Promise<void>;
    executions(query?: CronExecutionQuery): Promise<CronExecution[]>;
}

/** Data returned when a cron execution is claimed and ready to run. */
export interface ClaimedCronExecution {
    executionId: string;
    cronJobId: string;
    name: string;
    type: ExecType;
    payload: string;
    attempts: number;
    maxRetries: number;
    startedAt: number;
}
