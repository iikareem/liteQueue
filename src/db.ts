import Database from 'better-sqlite3';

export type ExecType = 'io' | 'worker';

interface EnqueueRow {
    id: string;
    name: string;
    type: ExecType;
    payload: string;
    runAt: number;
    maxRetries: number;
    priority: number;
}

interface DbRow {
    id: string;
    name: string;
    type: string;
    payload: string;
    status: string;
    attempts: number;
    max_retries: number;
    priority: number;
    run_at: number;
    locked_at: number | null;
    started_at: number | null;
    completed_at: number | null;
    error_log: string | null;
}

export interface ClaimedJob {
    id: string;
    name: string;
    type: ExecType;
    payload: string;
    attempts: number;
    maxRetries: number;
}

const SCHEMA = `
    DROP TABLE IF EXISTS liteq_jobs;

    CREATE TABLE IF NOT EXISTS liteq_jobs
    (
        id
        TEXT
        PRIMARY
        KEY,
        name
        TEXT
        NOT
        NULL,
        type
        TEXT
        NOT
        NULL,
        payload
        TEXT
        NOT
        NULL,
        status
        TEXT
        NOT
        NULL
        DEFAULT
        'pending',
        attempts
        INTEGER
        DEFAULT
        0,
        max_retries
        INTEGER
        DEFAULT
        3,
        priority
        INTEGER
        DEFAULT
        10,
        run_at
        INTEGER
        NOT
        NULL,
        locked_at
        INTEGER,
        started_at
        INTEGER,
        completed_at
        INTEGER,
        error_log
        TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_liteq_polling
        ON liteq_jobs (status, type, run_at, priority DESC);
`;

function toClaimedJob(row: DbRow): ClaimedJob {
    return {
        id: row.id,
        name: row.name,
        type: row.type as ExecType,
        payload: row.payload,
        attempts: row.attempts,
        maxRetries: row.max_retries,
    };
}

export class DB {
    private db: Database.Database;
    private readonly enqueueStmt;
    private readonly selectNextStmt;
    private readonly lockJobStmt;
    private readonly lockCrashedJobStmt;
    private readonly completeStmt;
    private readonly failStmt;
    private readonly retryStmt;

    constructor(storagePath: string) {
        this.db = new Database(storagePath);
        this.initialize();

        this.enqueueStmt = this.db.prepare(`
            INSERT INTO liteq_jobs (id, name, type, payload, run_at, max_retries, priority)
            VALUES (@id, @name, @type, @payload, @runAt, @maxRetries, @priority)
        `);

        this.selectNextStmt = this.db.prepare(`
            SELECT *
            FROM liteq_jobs
            WHERE (status = 'pending' AND run_at <= ? AND type = ?)
               OR (status = 'processing' AND started_at + ? <= ? AND type = ? AND attempts < max_retries)
            ORDER BY priority DESC, run_at ASC LIMIT 1
        `);

        this.lockJobStmt = this.db.prepare(`
            UPDATE liteq_jobs
            SET status     = 'processing',
                locked_at  = ?,
                started_at = ?
            WHERE id = ?
        `);


        this.lockCrashedJobStmt = this.db.prepare(`
            UPDATE liteq_jobs
            SET locked_at  = ?,
                started_at = ?,
                attempts   = ?
            WHERE id = ?
        `);

        this.completeStmt = this.db.prepare(`
            UPDATE liteq_jobs
            SET status       = 'completed',
                completed_at = ?
            WHERE id = ?
        `);

        this.failStmt = this.db.prepare(`
            UPDATE liteq_jobs
            SET status       = 'failed',
                error_log    = ?,
                completed_at = ?
            WHERE id = ?
        `);

        this.retryStmt = this.db.prepare(`
            UPDATE liteq_jobs
            SET status       = 'pending',
                attempts     = attempts + 1,
                locked_at    = NULL,
                started_at   = NULL,
                completed_at = NULL,
                run_at       = ?
            WHERE id = ?
        `);
    }

    initialize(): void {
        this.db.pragma('journal_mode = WAL');
        this.db.pragma('busy_timeout = 5000');
        this.db.pragma('synchronous = NORMAL');
        this.db.exec(SCHEMA);
    }

    enqueue(row: EnqueueRow): void {
        this.enqueueStmt.run(row);
    }

    claimNext(now: number, timeout: number, type: ExecType): ClaimedJob | null {
        const claim = this.db.transaction(() => {
            const row = this.selectNextStmt.get(now, type, timeout, now, type) as DbRow | undefined;
            if (!row) return null;

            if (row.status === 'processing') {
                this.lockCrashedJobStmt.run(now, now, row.attempts + 1, row.id);
            } else {
                this.lockJobStmt.run(now, now, row.id);
            }

            return row;
        })();

        return claim ? toClaimedJob(claim) : null;
    }

    complete(jobId: string, completedAt: number): void {
        this.completeStmt.run(completedAt, jobId);
    }

    fail(jobId: string, errorLog: string, completedAt: number): void {
        this.failStmt.run(errorLog, completedAt, jobId);
    }

    retry(jobId: string, nextRunAt: number): void {
        this.retryStmt.run(nextRunAt, jobId);
    }

    close(): void {
        this.db.close();
    }
}
