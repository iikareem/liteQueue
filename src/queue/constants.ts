import os from 'node:os';

const CPU_COUNT = os.cpus().length;

export const DEFAULT_CONCURRENCY = 1;
export const DEFAULT_POLL_INTERVAL = 500;
export const DEFAULT_JOB_TIMEOUT = 60_000;
export const DEFAULT_MIN_WORKERS = 1;
export const DEFAULT_MAX_RETRIES = 3;
export const DEFAULT_PRIORITY = 10;

export function resolveMaxWorkers(requested?: number): number {
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
