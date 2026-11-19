import { randomUUID } from 'node:crypto';

/** One hop record (README §4.8). */
export function makeHop({ hopId = randomUUID(), correlationId, causationId = null, service, operation,
  status = 'success', attempt = 1, messageId, startedAt, finishedAt = new Date(), detail }) {
  return {
    hopId, correlationId, causationId, service, operation, status, attempt, messageId,
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    durationMs: Math.max(0, Math.round(finishedAt.getTime() - startedAt.getTime())),
    detail,
  };
}

/** Publish hop records to the wire tap. Never blocks and never fails the business operation (R-4.8.2). */
export function emitHops(broker, hops) {
  for (const hop of hops) {
    try {
      broker
        .publish('trace.events', '', Buffer.from(JSON.stringify(hop)), {
          contentType: 'application/json', persistent: true, messageId: randomUUID(),
        })
        .catch(() => {});
    } catch { /* a lost hop must never break the order */ }
  }
}
