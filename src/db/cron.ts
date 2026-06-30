import Database from 'better-sqlite3';
import type {ClaimedCronExecution, CronExecution, CronJob} from '../cron/types.js';
import {applySqlitePragmas} from './pragmas.js';
import type {ExecType} from './types.js';

interface CronJobRow {
    id: string;
    name: string;
    cron_expression: string;
    type: string;
    payload: string;
    enabled: number;
    max_retries: number;
    next_run_at: number;
    created_at: number;
    updated_at: number;
}

interface CronExecutionRow {
    id: string;
    cron_job_id: string;
    status: string;
    attempts: number;
    max_retries: number;
    started_at: number | null;
    completed_at: number | null;
    duration_ms: number | null;
    error_log: string | null;
    cron_name?: string;
}

interface InsertCronJobRow {
    id: string;
    name: string;
    cronExpression: string;
    type: ExecType;
    payload: string;
    enabled: number;
    maxRetries: number;
    nextRunAt: number;
    createdAt: number;
    updatedAt: number;
}

interface InsertExecutionRow {
    id: string;
    cronJobId: string;
    maxRetries: number;
}

const SCHEMA = `
    CREATE TABLE IF NOT EXISTS lite_q_cron_jobs (
        id              TEXT    PRIMARY KEY,
        name            TEXT    NOT NULL UNIQUE,
        cron_expression TEXT    NOT NULL,
        type            TEXT    NOT NULL,
        payload         TEXT    NOT NULL DEFAULT '{}',
        enabled         INTEGER NOT NULL DEFAULT 1,
        max_retries     INTEGER NOT NULL DEFAULT 3,
        next_run_at     INTEGER NOT NULL,
        created_at      INTEGER NOT NULL,
        updated_at      INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS lite_q_cron_executions (
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

    CREATE INDEX IF NOT EXISTS idx_lite_q_cron_executions_status
        ON lite_q_cron_executions (cron_job_id, status);
`;

function toCronJob(row: CronJobRow): CronJob {
    return {
        id: row.id,
        name: row.name,
        cronExpression: row.cron_expression,
        type: row.type as ExecType,
        payload: JSON.parse(row.payload),
        enabled: row.enabled === 1,
        maxRetries: row.max_retries,
        nextRunAt: row.next_run_at,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
    };
}

function toCronExecution(row: CronExecutionRow): CronExecution {
    return {
        id: row.id,
        cronJobId: row.cron_job_id,
        cronName: row.cron_name ?? '',
        status: row.status as CronExecution['status'],
        attempts: row.attempts,
        maxRetries: row.max_retries,
        startedAt: row.started_at,
        completedAt: row.completed_at,
        durationMs: row.duration_ms,
        errorLog: row.error_log,
    };
}

function toClaimedCronExecution(
    row: CronJobRow,
    execution: CronExecutionRow,
    startedAt: number,
): ClaimedCronExecution {
    return {
        executionId: execution.id,
        cronJobId: row.id,
        name: row.name,
        type: row.type as ExecType,
        payload: row.payload,
        attempts: execution.attempts,
        maxRetries: execution.max_retries,
        startedAt,
    };
}

export class CronDB {
    private readonly db: Database.Database;
    private readonly upsertCronJobStmt;
    private readonly getCronJobByNameStmt;
    private readonly getCronJobByIdStmt;
    private readonly listCronJobsStmt;
    private readonly selectDueCronJobsStmt;
    private readonly updateNextRunAtStmt;
    private readonly updateCronJobEnabledStmt;
    private readonly insertExecutionStmt;
    private readonly startExecutionStmt;
    private readonly completeExecutionStmt;
    private readonly failExecutionStmt;
    private readonly retryExecutionStmt;
    private readonly recoverStaleExecutionStmt;
    private readonly getExecutionByIdStmt;
    private readonly listExecutionsByCronJobIdStmt;
    private readonly listExecutionsByCronNameStmt;
    private readonly hasProcessingExecutionStmt;
    private readonly selectStaleExecutionStmt;
    private readonly countCronJobsStmt;
    private readonly executionStatsStmt;
    private readonly purgeExecutionsStmt;

    constructor(storagePath: string) {
        this.db = new Database(storagePath);
        this.initialize();

        this.upsertCronJobStmt = this.db.prepare(`
            INSERT INTO lite_q_cron_jobs (
                id, name, cron_expression, type, payload,
                enabled, max_retries, next_run_at, created_at, updated_at
            ) VALUES (
                @id, @name, @cronExpression, @type, @payload,
                @enabled, @maxRetries, @nextRunAt, @createdAt, @updatedAt
            )
            ON CONFLICT(name) DO UPDATE SET
                cron_expression = excluded.cron_expression,
                type = excluded.type,
                payload = excluded.payload,
                enabled = excluded.enabled,
                max_retries = excluded.max_retries,
                updated_at = excluded.updated_at,
                next_run_at = CASE
                    WHEN lite_q_cron_jobs.cron_expression != excluded.cron_expression
                    THEN excluded.next_run_at
                    ELSE lite_q_cron_jobs.next_run_at
                END
        `);

        this.getCronJobByNameStmt = this.db.prepare(`
            SELECT * FROM lite_q_cron_jobs WHERE name = ?
        `);

        this.getCronJobByIdStmt = this.db.prepare(`
            SELECT * FROM lite_q_cron_jobs WHERE id = ?
        `);

        this.listCronJobsStmt = this.db.prepare(`
            SELECT * FROM lite_q_cron_jobs ORDER BY name ASC
        `);

        this.selectDueCronJobsStmt = this.db.prepare(`
            SELECT * FROM lite_q_cron_jobs
            WHERE enabled = 1 AND next_run_at <= ?
            ORDER BY next_run_at ASC
        `);

        this.updateNextRunAtStmt = this.db.prepare(`
            UPDATE lite_q_cron_jobs
            SET next_run_at = ?, updated_at = ?
            WHERE id = ?
        `);

        this.updateCronJobEnabledStmt = this.db.prepare(`
            UPDATE lite_q_cron_jobs
            SET enabled = ?, updated_at = ?
            WHERE name = ?
        `);

        this.insertExecutionStmt = this.db.prepare(`
            INSERT INTO lite_q_cron_executions (id, cron_job_id, max_retries)
            VALUES (@id, @cronJobId, @maxRetries)
        `);

        this.startExecutionStmt = this.db.prepare(`
            UPDATE lite_q_cron_executions
            SET status = 'processing', started_at = ?
            WHERE id = ? AND status IN ('pending', 'processing')
        `);

        this.completeExecutionStmt = this.db.prepare(`
            UPDATE lite_q_cron_executions
            SET status = 'completed', completed_at = ?, duration_ms = ?
            WHERE id = ?
        `);

        this.failExecutionStmt = this.db.prepare(`
            UPDATE lite_q_cron_executions
            SET status = 'failed',
                error_log = ?,
                completed_at = ?,
                duration_ms = ?
            WHERE id = ?
        `);

        this.retryExecutionStmt = this.db.prepare(`
            UPDATE lite_q_cron_executions
            SET status = 'pending',
                attempts = attempts + 1,
                started_at = NULL,
                completed_at = NULL,
                duration_ms = NULL,
                error_log = NULL
            WHERE id = ?
        `);

        this.recoverStaleExecutionStmt = this.db.prepare(`
            UPDATE lite_q_cron_executions
            SET status = 'pending',
                attempts = attempts + 1,
                started_at = NULL
            WHERE id = ? AND status = 'processing'
        `);

        this.getExecutionByIdStmt = this.db.prepare(`
            SELECT e.*, j.name AS cron_name
            FROM lite_q_cron_executions e
            JOIN lite_q_cron_jobs j ON j.id = e.cron_job_id
            WHERE e.id = ?
        `);

        this.listExecutionsByCronJobIdStmt = this.db.prepare(`
            SELECT e.*, j.name AS cron_name
            FROM lite_q_cron_executions e
            JOIN lite_q_cron_jobs j ON j.id = e.cron_job_id
            WHERE e.cron_job_id = ?
            ORDER BY COALESCE(e.started_at, e.completed_at, 0) DESC
            LIMIT ?
        `);

        this.listExecutionsByCronNameStmt = this.db.prepare(`
            SELECT e.*, j.name AS cron_name
            FROM lite_q_cron_executions e
            JOIN lite_q_cron_jobs j ON j.id = e.cron_job_id
            WHERE j.name = ?
            ORDER BY COALESCE(e.started_at, e.completed_at, 0) DESC
            LIMIT ?
        `);

        this.hasProcessingExecutionStmt = this.db.prepare(`
            SELECT 1
            FROM lite_q_cron_executions
            WHERE cron_job_id = ? AND status = 'processing'
            LIMIT 1
        `);

        this.selectStaleExecutionStmt = this.db.prepare(`
            SELECT e.*, j.name AS cron_name
            FROM lite_q_cron_executions e
            JOIN lite_q_cron_jobs j ON j.id = e.cron_job_id
            WHERE e.status = 'processing'
              AND e.started_at IS NOT NULL
              AND e.started_at + ? <= ?
            ORDER BY e.started_at ASC
            LIMIT 1
        `);

        this.countCronJobsStmt = this.db.prepare(`
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) AS enabled
            FROM lite_q_cron_jobs
        `);

        this.executionStatsStmt = this.db.prepare(`
            SELECT status, COUNT(*) AS count
            FROM lite_q_cron_executions
            GROUP BY status
        `);

        this.purgeExecutionsStmt = this.db.prepare(`
            DELETE FROM lite_q_cron_executions
            WHERE status IN ('completed', 'failed') AND completed_at < ?
        `);
    }

    initialize(): void {
        applySqlitePragmas(this.db);
        this.db.exec(SCHEMA);
    }

    upsertCronJob(row: InsertCronJobRow): void {
        this.upsertCronJobStmt.run(row);
    }

    getCronJobByName(name: string): CronJob | null {
        const row = this.getCronJobByNameStmt.get(name) as CronJobRow | undefined;
        return row ? toCronJob(row) : null;
    }

    listCronJobs(): CronJob[] {
        const rows = this.listCronJobsStmt.all() as CronJobRow[];
        return rows.map(toCronJob);
    }

    listDueCronJobs(now: number): CronJob[] {
        const rows = this.selectDueCronJobsStmt.all(now) as CronJobRow[];
        return rows.map(toCronJob);
    }

    updateNextRunAt(cronJobId: string, nextRunAt: number, updatedAt: number): void {
        this.updateNextRunAtStmt.run(nextRunAt, updatedAt, cronJobId);
    }

    setCronJobEnabled(name: string, enabled: boolean, updatedAt: number): void {
        this.updateCronJobEnabledStmt.run(enabled ? 1 : 0, updatedAt, name);
    }

    startExecution(executionId: string, startedAt: number): void {
        this.startExecutionStmt.run(startedAt, executionId);
    }

    completeExecution(executionId: string, completedAt: number, durationMs: number): void {
        this.completeExecutionStmt.run(completedAt, durationMs, executionId);
    }

    failExecution(
        executionId: string,
        errorLog: string,
        completedAt: number,
        durationMs: number,
    ): void {
        this.failExecutionStmt.run(errorLog, completedAt, durationMs, executionId);
    }

    retryExecution(executionId: string): void {
        this.retryExecutionStmt.run(executionId);
    }

    getExecutionById(executionId: string): CronExecution | null {
        const row = this.getExecutionByIdStmt.get(executionId) as CronExecutionRow | undefined;
        return row ? toCronExecution(row) : null;
    }

    listExecutionsByCronName(name: string, limit: number): CronExecution[] {
        const rows = this.listExecutionsByCronNameStmt.all(name, limit) as CronExecutionRow[];
        return rows.map(toCronExecution);
    }

    cronJobCounts(): { total: number; enabled: number } {
        const row = this.countCronJobsStmt.get() as { total: number; enabled: number | null };
        return {
            total: Number(row.total),
            enabled: Number(row.enabled ?? 0),
        };
    }

    executionStats(): { status: string; count: number }[] {
        return this.executionStatsStmt.all() as { status: string; count: number }[];
    }

    purgeExecutions(before: number): void {
        this.purgeExecutionsStmt.run(before);
    }

    getLastExecution(cronJobId: string): CronExecution | null {
        const rows = this.listExecutionsByCronJobIdStmt.all(cronJobId, 1) as CronExecutionRow[];
        return rows[0] ? toCronExecution(rows[0]) : null;
    }

    claimStaleExecution(timeout: number, now: number): ClaimedCronExecution | null {
        const row = this.selectStaleExecutionStmt.get(timeout, now) as CronExecutionRow | undefined;
        if (!row) return null;

        const cronJob = this.getCronJobByIdStmt.get(row.cron_job_id) as CronJobRow | undefined;
        if (!cronJob) return null;

        if (row.attempts >= row.max_retries) {
            const durationMs = row.started_at ? now - row.started_at : 0;
            this.failExecutionStmt.run('Execution timed out', now, durationMs, row.id);
            return null;
        }

        this.recoverStaleExecutionStmt.run(row.id);
        this.startExecutionStmt.run(now, row.id);

        const updated = {...row, attempts: row.attempts + 1};
        return toClaimedCronExecution(cronJob, updated, now);
    }

    beginExecution(
        cronJob: CronJob,
        executionId: string,
        now: number,
        nextRunAt: number,
    ): ClaimedCronExecution | null {
        const claim = this.db.transaction(() => {
            if (this.hasProcessingExecutionStmt.get(cronJob.id) !== undefined) {
                return null;
            }

            this.insertExecutionStmt.run({
                id: executionId,
                cronJobId: cronJob.id,
                maxRetries: cronJob.maxRetries,
            });

            this.startExecutionStmt.run(now, executionId);
            this.updateNextRunAtStmt.run(nextRunAt, now, cronJob.id);

            const execution = this.getExecutionByIdStmt.get(executionId) as CronExecutionRow;
            const jobRow = this.getCronJobByIdStmt.get(cronJob.id) as CronJobRow;

            return toClaimedCronExecution(jobRow, execution, now);
        })();

        return claim;
    }

    beginManualExecution(
        cronJob: CronJob,
        executionId: string,
        now: number,
    ): ClaimedCronExecution | null {
        const claim = this.db.transaction(() => {
            if (this.hasProcessingExecutionStmt.get(cronJob.id) !== undefined) {
                return null;
            }

            this.insertExecutionStmt.run({
                id: executionId,
                cronJobId: cronJob.id,
                maxRetries: cronJob.maxRetries,
            });

            this.startExecutionStmt.run(now, executionId);

            const execution = this.getExecutionByIdStmt.get(executionId) as CronExecutionRow;
            const jobRow = this.getCronJobByIdStmt.get(cronJob.id) as CronJobRow;

            return toClaimedCronExecution(jobRow, execution, now);
        })();

        return claim;
    }

    close(): void {
        this.db.close();
    }
}
