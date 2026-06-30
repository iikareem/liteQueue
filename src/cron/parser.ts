import {CronExpressionParser} from 'cron-parser';

function assertValidCronExpression(expression: string): void {
    try {
        CronExpressionParser.parse(expression);
    } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        throw new Error(`Invalid cron expression "${expression}": ${message}`);
    }
}

export function nextRunAfter(expression: string, from = Date.now()): number {
    assertValidCronExpression(expression);

    const interval = CronExpressionParser.parse(expression, {currentDate: from});
    return interval.next().getTime();
}
