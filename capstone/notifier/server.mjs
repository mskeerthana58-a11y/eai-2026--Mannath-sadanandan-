import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import express from 'express';
import { Broker, healthHandler } from '../shared/broker.mjs';
import { JsonStore } from '../shared/store.mjs';
import { makeHop, emitHops } from '../shared/trace.mjs';
import { getRetryCount, withoutRetryHistory } from '../shared/retry.mjs';

const env = process.env;
const PORT = Number(env.PORT || 8803);
const DATA_DIR = env.DATA_DIR || '/data';
const NOTIFICATION_URL = (env.NOTIFICATION_URL || 'http://notification:4004').replace(/\/+$/, '');
const TIMEOUT_MS = Number(env.REQUEST_TIMEOUT_MS || 2500);
const MAX_RETRIES = Number(env.MAX_RETRIES || 3);
const QUEUE = 'notifications.queue';
const LOG_FILE = path.join(DATA_DIR, 'notification.log');

// ---- the idempotent receiver's memory: /data/processed-ids.json (R-4.5.3) -------------------
const processed = new JsonStore(path.join(DATA_DIR, 'processed-ids.json'), []);
processed.load();
if (!Array.isArray(processed.data)) processed.data = [];
const done = new Set(processed.data.filter((id) => typeof id === 'string' && id));
processed.data = [...done];
processed.save(); // the file exists from the first start, as `[]`

const broker = new Broker('notifier');

function flowOf(cid, causation, attempt) {
  return {
    last: causation,
    add(operation, o = {}) {
      const hopId = o.hopId || randomUUID();
      emitHops(broker, [makeHop({
        hopId, correlationId: cid, causationId: o.cause !== undefined ? o.cause : this.last,
        service: 'notifier', operation, status: o.status || 'success', attempt,
        messageId: o.messageId, startedAt: o.start || new Date(), finishedAt: new Date(), detail: o.detail,
      })]);
      if (!o.noAdvance) this.last = hopId;
      return hopId;
    },
  };
}

/** One send to the notification provider. ok | failed, with a short reason for lastError. */
async function send(event, cid, causationId) {
  try {
    const res = await fetch(`${NOTIFICATION_URL}/notification/send`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'X-Correlation-Id': cid, 'X-Causation-Id': causationId },
      body: JSON.stringify({
        orderId: event.orderId,
        recipient: event.recipient,
        kind: 'order_not_completed',
        ...(typeof event.code === 'string' ? { reason: event.code } : {}),
      }),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    await res.text().catch(() => {});
    if (res.ok) return { ok: true };
    return { ok: false, lastError: `notification ${res.status}`, code: `http_${res.status}` };
  } catch (e) {
    const timeout = e?.name === 'TimeoutError' || e?.name === 'AbortError';
    return { ok: false, timeout, lastError: timeout ? 'timeout' : 'unreachable', code: timeout ? 'timeout' : 'unreachable' };
  }
}

const isEvent = (e) => e && typeof e === 'object' && typeof e.orderId === 'string' && e.orderId !== ''
  && typeof e.recipient === 'string' && e.recipient !== '';

async function onMessage(msg, ch) {
  const started = new Date();
  const headers = msg.properties.headers || {};
  const cid = typeof headers.correlationId === 'string' && headers.correlationId ? headers.correlationId : undefined;
  const attempt = getRetryCount(msg) + 1;

  let event;
  try { event = JSON.parse(msg.content.toString('utf8')); } catch { event = null; }

  // A message that can never succeed: park it as it came, once, and never retry it (R-4.5.8).
  if (!cid || !isEvent(event)) {
    try {
      await broker.publish('orders.invalid.exchange', '', msg.content, { persistent: true, headers: msg.properties.headers, messageId: randomUUID() });
    } catch { ch.nack(msg, false, true); return; }
    if (cid) flowOf(cid, headers.causationId ?? null, attempt).add('invalid.quarantine', { status: 'quarantined', start: started });
    ch.ack(msg);
    return;
  }

  const flow = flowOf(cid, typeof headers.causationId === 'string' ? headers.causationId : null, attempt);
  const consumeId = flow.add('notify.consume', { start: started, messageId: msg.properties.messageId });

  // Dedupe on correlationId: check before the send, record after it (R-4.5.3).
  if (done.has(cid)) {
    flow.add('notify.duplicate', { status: 'duplicate' });
    ch.ack(msg);
    return;
  }

  const sendHop = randomUUID();
  const t0 = new Date();
  const r = await send(event, cid, sendHop);
  flow.add('notify.send', { hopId: sendHop, start: t0, status: r.ok ? 'success' : r.timeout ? 'timeout' : 'failed', detail: r.ok ? undefined : { code: r.code } });

  if (r.ok) {
    fs.appendFileSync(LOG_FILE, JSON.stringify({
      correlationId: cid, orderId: event.orderId, recipient: event.recipient, kind: 'order_not_completed',
      reason: event.code ?? null, timestamp: new Date().toISOString(), message: 'Order not completed',
    }) + '\n');
    done.add(cid);
    processed.data = [...done];
    await processed.save();
    ch.ack(msg);
    return;
  }

  // A failed send is temporary: the retry path, then the DLQ after MAX_RETRIES attempts (R-4.5.7).
  if (attempt < MAX_RETRIES) {
    flow.add('notify.retry', { status: 'retried', cause: consumeId, noAdvance: true });
    ch.nack(msg, false, false);
    return;
  }
  const dlHop = flow.add('notify.dead_letter', { status: 'dead_lettered' });
  try {
    await broker.publish('orders.dlq.exchange', '', msg.content, {
      persistent: true,
      contentType: 'application/json',
      messageId: randomUUID(),
      headers: {
        ...withoutRetryHistory(headers),
        correlationId: cid,
        causationId: dlHop,
        originQueue: QUEUE,
        lastError: r.lastError,
        attempts: attempt,
        deadLetteredAt: new Date().toISOString(),
      },
    });
    ch.ack(msg); // only after the broker confirmed the DLQ publish
  } catch (e) {
    console.error(`[notifier] cannot dead-letter ${cid}: ${e.message}`);
    ch.nack(msg, false, true);
  }
}

const app = express();
app.get('/health', healthHandler(broker));
process.on('unhandledRejection', (e) => console.error('[notifier] unhandled rejection:', e));
broker.consume(QUEUE, onMessage, 1);
app.listen(PORT, () => console.log(`[notifier] listening on ${PORT}`));
broker.start();
