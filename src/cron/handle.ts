import type {CronDB} from '../db/index.js';
import type {CronExecution, CronExecutionQuery, CronHandle} from './types.js';

export interface CronHandleActions {
    trigger(): Promise<CronExecution>;
    pause(): Promise<void>;
    resume(): Promise<void>;
}

export class CronHandleImpl implements CronHandle {
    readonly name: string;
    readonly expression: string;

    constructor(
        name: string,
        expression: string,
        private readonly cronDb: CronDB,
        private readonly actions: CronHandleActions,
    ) {
        this.name = name;
        this.expression = expression;
    }

    trigger(): Promise<CronExecution> {
        return this.actions.trigger();
    }

    pause(): Promise<void> {
        return this.actions.pause();
    }

    resume(): Promise<void> {
        return this.actions.resume();
    }

    executions(query?: CronExecutionQuery): Promise<CronExecution[]> {
        const limit = query?.limit ?? 20;
        return Promise.resolve(this.cronDb.listExecutionsByCronName(this.name, limit));
    }
}
