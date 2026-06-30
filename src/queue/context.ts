import type {CronDB, DB} from '../db/index.js';
import type {JobHandler} from '../types.js';
import type {WorkerPool} from '../worker-pool.js';

export interface QueueContext {
    readonly db: DB;
    readonly cronDb: CronDB;
    readonly pool: WorkerPool;
    readonly handlers: Map<string, JobHandler | string>;
    readonly activeIo: { count: number };
    readonly concurrency: number;
    readonly jobTimeout: number;
}
