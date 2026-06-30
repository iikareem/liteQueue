import type {Job, JobHandler} from '../types.js';
import type {WorkerPool} from '../worker-pool.js';

export function executeHandler(
    pool: WorkerPool,
    handler: JobHandler | string,
    job: Job,
): Promise<unknown> {
    if (typeof handler === 'function') {
        return handler(job);
    }
    return pool.execute(handler, job);
}

export function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
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
