import path from 'node:path';
import { randomUUID } from 'node:crypto';
import express from 'express';
import { Broker, healthHandler } from '../shared/broker.mjs';
import { JsonStore } from '../shared/store.mjs';
import { makeHop, emitHops } from '../shared/trace.mjs';
import { getRetryCount } from '../shared/retry.mjs';
import { loadOrderValidator } from '../shared/canonical.mjs';

const env = process.env;
const PORT = Number(env.PORT || 8802);
const TIMEOUT_MS = Number(env.REQUEST_TIMEOUT_MS || 2500);
const MAX_RETRIES = Number(env.MAX_RETRIES || 3);
const CONCURRENCY = Number(env.SAGA_CONCURRENCY || 5);
const DATA_DIR = env.DATA_DIR || '/data';
const URLS = {
  payment: (env.PAYMENT_URL || 'http://payment:4001').replace(/\/+$/, ''),
  inventory: (env.INVENTORY_URL || 'http://inventory:4002').replace(/\/+$/, ''),
  shipping: (env.SHIPPING_URL || 'http://shipping:4003').replace(/\/+$/, ''),
  notification: (env.NOTIFICATION_URL || 'http://notification:4004').replace(/\/+$/, ''),
};

const RUN_ID = randomUUID(); // a new id at every start: how a crash is detected (§4.7)
const validateOrder = loadOrderValidator();
const iso = () => new Date().toISOString();

// ---- store ----------------------------------------------------------------------------------
const store = new JsonStore(path.join(DATA_DIR, 'saga-store.json'), { sagas: {}, businessKeys: {} });
store.load();
store.data.sagas ??= {};
store.data.businessKeys ??= {};
const sagas = store.data.sagas;
const businessKeys = store.data.businessKeys;

const broker = new Broker('orchestrator');
const running = new Set(); // correlationIds whose steps are running in this process
const opRunning = new Set(); // sagas under operator compensation

// States that already have an outcome event (R-4.2.4).
const WITH_OUTCOME = new Set(['completed', 'failed', 'compensated', 'compensation_failed', 'needs_attention', 'duplicate']);

// ---- the steps and their compensations (§4.2) -----------------------------------------------
const STEPS = {
  payment: { svc: 'payment', route: '/payment/authorize', body: (o) => ({ orderId: o.orderId, amount: amountOf(o) }), key: (bk) => `authorize:${bk}` },
  inventory: { svc: 'inventory', route: '/inventory/reserve', body: (o) => ({ orderId: o.orderId, items: o.items }), key: (bk) => `reserve:${bk}` },
  shipping: { svc: 'shipping', route: '/shipping/create', body: (o) => ({ orderId: o.orderId }) },
  notification: { svc: 'notification', route: '/notification/send', body: (o) => ({ orderId: o.orderId, recipient: o.customer.email }) },
};
const COMPENSATIONS = {
  payment: { name: 'payment_refund', svc: 'payment', route: '/payment/refund', key: (bk) => `refund:${bk}` },
  inventory: { name: 'inventory_release', svc: 'inventory', route: '/inventory/release', key: (bk) => `release:${bk}` },
};

/** Sum of unitPrice x quantity, in whole cents, as a two-decimal string (R-4.2.10). */
function amountOf(order) {
  let cents = 0;
  for (const it of order.items) {
    const [whole, frac = ''] = String(it.unitPrice).split('.');
    cents += (parseInt(whole, 10) * 100 + parseInt((frac + '00').slice(0, 2), 10)) * it.quantity;
  }
  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, '0')}`;
}

/** One HTTP call to a provider. ok | permanent (4xx) | temporary (5xx, timeout, no connection). */
async function call(svc, route, body, { cid, cause, key }) {
  try {
    const res = await fetch(URLS[svc] + route, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'X-Correlation-Id': cid,
        'X-Causation-Id': cause,
        ...(key ? { 'Idempotency-Key': key } : {}),
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    const text = await res.text();
    if (res.ok) return { ok: true };
    let json;
    try { json = JSON.parse(text); } catch { /* not JSON */ }
    return { ok: false, kind: res.status >= 500 ? 'temporary' : 'permanent', code: json?.code || `http_${res.status}`, status: res.status };
  } catch (e) {
    const timeout = e?.name === 'TimeoutError' || e?.name === 'AbortError';
    return { ok: false, kind: 'temporary', code: timeout ? 'timeout' : 'unreachable', timeout };
  }
}

// ---- trace flow -----------------------------------------------------------------------------
function newFlow(cid, causation, attempt = 1) {
  return {
    cid,
    last: causation,
    attempt,
    add(operation, o = {}) {
      const hopId = o.hopId || randomUUID();
      emitHops(broker, [makeHop({
        hopId, correlationId: cid, causationId: o.cause !== undefined ? o.cause : this.last,
        service: 'orchestrator', operation, status: o.status || 'success', attempt: this.attempt,
        messageId: o.messageId, startedAt: o.start || new Date(), finishedAt: new Date(), detail: o.detail,
      })]);
      if (!o.noAdvance) this.last = hopId;
      return hopId;
    },
  };
}

// ---- outcome events -------------------------------------------------------------------------
/** Persist the new state with its outcome event, then publish the event (R-4.2.18). */
async function emitOutcome(cid, base, state, extra, flow) {
  const saga = sagas[cid];
  const envelope = {
    correlationId: cid, businessKey: saga.businessKey, orderId: base.orderId, source: base.source,
    orderType: base.orderType, recipient: base.recipient, state,
    ...(extra.code !== undefined ? { code: extra.code } : {}),
    ...(extra.duplicateOf !== undefined ? { duplicateOf: extra.duplicateOf } : {}),
    occurredAt: iso(),
  };
  const hopId = flow.add('saga.outcome');
  saga.state = state;
  saga.outcome = { routingKey: `order.${state}`, envelope };
  saga.updatedAt = envelope.occurredAt;
  saga.lastHopId = hopId;
  await store.save();
  await publishEnvelope(saga.outcome, saga.businessKey, cid, hopId);
}

function publishEnvelope(outcome, businessKey, cid, causationId) {
  return broker.publish('orders.outcome', outcome.routingKey, Buffer.from(JSON.stringify(outcome.envelope)), {
    persistent: true, contentType: 'application/json', messageId: randomUUID(),
    headers: { correlationId: cid, causationId, businessKey },
  });
}

const baseOf = (order) => ({ orderId: order.orderId, source: order.source, orderType: order.orderType, recipient: order.customer.email });
const orderIdOf = (bk) => bk.slice(bk.indexOf(':') + 1);

// ---- compensation ---------------------------------------------------------------------------
/** Run the outstanding compensations in reverse order. Returns {failed?: {name, code}}. */
async function compensate(saga, cid, flow) {
  const completed = saga.steps.filter((s) => s.status === 'completed').map((s) => s.step).reverse();
  const done = new Set(saga.compensations.filter((c) => c.status === 'completed').map((c) => c.step));
  const orderId = orderIdOf(saga.businessKey);
  for (const step of completed) {
    const comp = COMPENSATIONS[step];
    if (!comp || done.has(comp.name)) continue;
    const hopId = randomUUID();
    const started = new Date();
    const r = await call(comp.svc, comp.route, { orderId }, { cid, cause: hopId, key: comp.key(saga.businessKey) });
    flow.add(comp.name, { hopId, start: started, status: r.ok ? 'success' : r.timeout ? 'timeout' : 'failed', detail: r.ok ? undefined : { code: r.code } });
    saga.compensations.push({ step: comp.name, status: r.ok ? 'completed' : 'failed', at: iso() });
    await store.save();
    if (!r.ok) return { failed: { name: comp.name, code: r.code } };
  }
  return {};
}

// ---- consuming saga.queue -------------------------------------------------------------------
async function quarantine(msg, ch, cid, started, attempt) {
  await broker.publish('orders.invalid.exchange', '', msg.content, { persistent: true, headers: msg.properties.headers });
  if (cid) {
    const flow = newFlow(cid, msg.properties.headers?.causationId ?? null, attempt);
    flow.add('invalid.quarantine', { status: 'quarantined', start: started });
  }
  ch.ack(msg);
}

async function onMessage(msg, ch) {
  const started = new Date();
  const headers = msg.properties.headers || {};
  const cid = typeof headers.correlationId === 'string' && headers.correlationId ? headers.correlationId : undefined;
  const attempt = getRetryCount(msg) + 1;

  let order;
  try {
    order = JSON.parse(msg.content.toString('utf8'));
    if (!cid || !validateOrder(order)) throw new Error('permanent');
  } catch {
    return quarantine(msg, ch, cid, started, attempt);
  }

  const bk = `${order.source}:${order.orderId}`;
  const flow = newFlow(cid, headers.causationId ?? null, attempt);
  const consumeId = flow.add('saga.consume', { start: started });

  if (running.has(cid)) { ch.ack(msg); return; } // a redelivery of a saga still running here

  let saga = sagas[cid];

  // Redelivery of a saga that already has an outcome: republish it, run nothing (R-4.2.4).
  if (saga && WITH_OUTCOME.has(saga.state)) {
    const id = flow.add('saga.dedupe', { status: 'duplicate' });
    await publishEnvelope(saga.outcome, saga.businessKey, cid, id);
    ch.ack(msg);
    return;
  }

  // Steps were running under a run that no longer exists: stuck, run nothing (R-4.2.5).
  if (saga && saga.state === 'processing' && saga.steps.length > 0 && saga.owner !== RUN_ID) {
    await markStuck(cid, flow);
    ch.ack(msg);
    return;
  }

  // The same business key under another correlationId: a duplicate (R-4.2.3).
  if (!saga && businessKeys[bk] && businessKeys[bk] !== cid) {
    const first = businessKeys[bk];
    saga = sagas[cid] = { businessKey: bk, state: 'duplicate', duplicateOf: first, steps: [], compensations: [], owner: RUN_ID, updatedAt: iso(), lastHopId: consumeId };
    flow.add('saga.dedupe', { status: 'duplicate' });
    await emitOutcome(cid, baseOf(order), 'duplicate', { duplicateOf: first }, flow);
    ch.ack(msg);
    return;
  }

  running.add(cid);
  try {
    await runSaga(cid, bk, order, msg, ch, flow, consumeId, attempt, saga);
  } finally {
    running.delete(cid);
  }
}

async function runSaga(cid, bk, order, msg, ch, flow, consumeId, attempt, existing) {
  const saga = existing || (sagas[cid] = { businessKey: bk, state: 'processing', steps: [], compensations: [], owner: RUN_ID, updatedAt: iso() });
  businessKeys[bk] = cid;
  const wasProcessing = saga.state === 'processing' && existing;
  saga.steps = [];
  saga.compensations = [];
  saga.owner = RUN_ID;
  saga.lastHopId = consumeId;
  delete saga.failureCode;

  // The semantic lock (R-4.2.7). A retry attempt is already `processing`: no new event.
  if (!wasProcessing) await emitOutcome(cid, baseOf(order), 'processing', {}, flow);
  else { saga.updatedAt = iso(); await store.save(); }

  const route = order.orderType === 'b2b' ? ['inventory', 'shipping', 'notification'] : ['payment', 'inventory', 'shipping', 'notification'];
  let failure = null;
  for (let i = 0; i < route.length; i++) {
    const name = route[i];
    const spec = STEPS[name];
    const hopId = randomUUID();
    const started = new Date();
    const entry = { step: name, status: 'started', at: iso() };
    saga.steps.push(entry);
    await store.save();
    const r = await call(spec.svc, spec.route, spec.body(order), { cid, cause: hopId, key: spec.key?.(bk) });
    flow.add(name, { hopId, start: started, status: r.ok ? 'success' : r.timeout ? 'timeout' : 'failed', detail: r.ok ? undefined : { code: r.code } });
    if (r.ok) { entry.status = 'completed'; entry.at = iso(); await store.save(); continue; }
    entry.status = 'failed';
    failure = { name, index: i, r };
    break;
  }

  if (!failure) {
    await emitOutcome(cid, baseOf(order), 'completed', {}, flow);
    ch.ack(msg);
    return;
  }

  const { name, index, r } = failure;
  const isFirst = index === 0;

  // First step, permanent failure: nothing to compensate (R-4.2.15).
  if (isFirst && r.kind === 'permanent') {
    await emitOutcome(cid, baseOf(order), 'failed', { code: r.code }, flow);
    ch.ack(msg);
    return;
  }

  // First step, temporary failure: the provider may have acted. Retry path, same key (R-4.2.12-14).
  if (isFirst) {
    saga.steps.pop();
    if (attempt < MAX_RETRIES) {
      await store.save();
      flow.add('saga.retry', { status: 'retried', cause: consumeId, noAdvance: true });
      ch.nack(msg, false, false);
      return;
    }
    const dlId = flow.add('saga.dead_letter', { status: 'dead_lettered', cause: consumeId });
    await broker.publish('orders.dlq.exchange', '', msg.content, {
      persistent: true, contentType: 'application/json', messageId: randomUUID(),
      headers: { ...(msg.properties.headers || {}), originQueue: 'saga.queue', lastError: `${name} ${r.status ?? r.code}`, causationId: dlId },
    });
    await emitOutcome(cid, baseOf(order), 'parked', { code: r.code }, flow);
    ch.ack(msg);
    return;
  }

  // A later step failed: compensate the completed ones in reverse order (R-4.2.16, R-4.2.17).
  saga.failureCode = r.code;
  const out = await compensate(saga, cid, flow);
  if (out.failed) await emitOutcome(cid, baseOf(order), 'compensation_failed', { code: out.failed.code }, flow);
  else await emitOutcome(cid, baseOf(order), 'compensated', { code: r.code }, flow);
  ch.ack(msg);
}

// ---- stuck sagas (§4.7) ---------------------------------------------------------------------
async function markStuck(cid, flow) {
  const saga = sagas[cid];
  if (!saga || saga.state !== 'processing') return;
  saga.state = 'needs_attention'; // set before any await: the scan and a redelivery cannot both mark it
  flow.add('saga.stuck');
  const base = saga.outcome?.envelope || { orderId: orderIdOf(saga.businessKey), source: saga.businessKey.split(':')[0], orderType: 'standard', recipient: '' };
  await emitOutcome(cid, base, 'needs_attention', {}, flow);
}

async function scanStuck() {
  for (const [cid, saga] of Object.entries(sagas)) {
    if (saga.state === 'processing' && saga.steps.length > 0 && saga.owner !== RUN_ID && !running.has(cid)) {
      try { await markStuck(cid, newFlow(cid, saga.lastHopId ?? null)); } catch (e) { console.error(`[orchestrator] stuck scan failed for ${cid}: ${e.message}`); }
    }
  }
}
const scanTimer = setInterval(() => { if (broker.up) { clearInterval(scanTimer); scanStuck(); } }, 200);

// ---- HTTP -----------------------------------------------------------------------------------
const app = express();
app.get('/health', healthHandler(broker));

app.get('/sagas/stuck', (_req, res) => {
  const list = Object.entries(sagas)
    .filter(([, s]) => s.state === 'needs_attention' || s.state === 'compensation_failed')
    .map(([cid, s]) => ({
      correlationId: cid,
      businessKey: s.businessKey,
      state: s.state,
      completedSteps: s.steps.filter((x) => x.status === 'completed').map((x) => x.step),
      compensatedSteps: s.compensations.filter((x) => x.status === 'completed').map((x) => x.step),
      since: s.updatedAt,
    }));
  res.status(200).json({ sagas: list });
});

app.post('/sagas/:cid/compensate', async (req, res) => {
  const cid = req.params.cid;
  const saga = sagas[cid];
  if (!saga) return res.status(404).json({ code: 'not_found', message: 'unknown correlationId' });
  if (opRunning.has(cid)) return res.status(409).json({ code: 'compensation_in_progress', message: 'an operator compensation is already running' });
  if (saga.state === 'compensated' && saga.operatorCompensation) return res.status(200).json(saga.operatorCompensation.response);
  if (saga.state !== 'needs_attention' && saga.state !== 'compensation_failed') {
    return res.status(409).json({ code: 'saga_not_stuck', message: `the saga is ${saga.state}` });
  }
  opRunning.add(cid);
  try {
    const from = saga.state;
    const flow = newFlow(cid, saga.lastHopId ?? null);
    flow.add('saga.operator_compensate');
    const out = await compensate(saga, cid, flow);
    const state = out.failed ? 'compensation_failed' : 'compensated';
    const code = out.failed ? out.failed.code : from === 'needs_attention' ? 'operator_compensated' : (saga.failureCode || 'operator_compensated');
    saga.operatorCompensation = { at: iso(), response: { correlationId: cid, state } };
    await emitOutcome(cid, saga.outcome.envelope, state, { code }, flow);
    return res.status(200).json({ correlationId: cid, state });
  } catch (e) {
    console.error('[orchestrator] operator compensation failed:', e.message);
    return res.status(503).json({ code: 'broker_unavailable', message: e.message });
  } finally {
    opRunning.delete(cid);
  }
});

process.on('unhandledRejection', (e) => console.error('[orchestrator] unhandled rejection:', e));
broker.consume('saga.queue', onMessage, CONCURRENCY);
app.listen(PORT, () => console.log(`[orchestrator] listening on ${PORT}`));
broker.start();
