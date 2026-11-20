import fs from 'node:fs';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import express from 'express';
import Ajv from 'ajv';
import Ajv2019 from 'ajv/dist/2019.js';
import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { Broker, healthHandler } from '../shared/broker.mjs';
import { JsonStore } from '../shared/store.mjs';
import { makeHop, emitHops } from '../shared/trace.mjs';
import { translateB2B, translateMobile, translateWeb } from './translate.mjs';
import { getRetryCount, withoutRetryHistory } from '../shared/retry.mjs';

const PORT = Number(process.env.PORT || 8800);
const PRICING_URL = (process.env.PRICING_URL || 'http://pricing:4100').replace(/\/+$/, '');
const PRICING_API_KEY = process.env.PRICING_API_KEY || '';
const TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 2500);
const ITEMS_MAX = Number(process.env.ORDER_ITEMS_MAX || 50);
const DATA_DIR = process.env.DATA_DIR || '/data';
const KEY_RE = /^[A-Za-z0-9._:-]{1,128}$/;

// ---- the canonical schema (read-only mount) -------------------------------------------------
const schema = JSON.parse(fs.readFileSync('/canonical/order.schema.json', 'utf8'));
function compileSchema() {
  const dialect = String(schema.$schema || '');
  const Cls = dialect.includes('2020-12') ? Ajv2020 : dialect.includes('2019-09') ? Ajv2019 : Ajv;
  try {
    const ajv = new Cls({ allErrors: true, strict: false });
    addFormats(ajv);
    return { validate: ajv.compile(schema), ajv };
  } catch (e) {
    console.error(`[intake] schema compile with ${Cls.name} failed (${e.message}); retrying as draft-07`);
    const copy = { ...schema };
    delete copy.$schema;
    const ajv = new Ajv({ allErrors: true, strict: false });
    addFormats(ajv);
    return { validate: ajv.compile(copy), ajv };
  }
}
const { validate: validateOrder, ajv } = compileSchema();

// ---- stores ---------------------------------------------------------------------------------
const idem = new JsonStore(path.join(DATA_DIR, 'idempotency-store.json'), {});
const orders = new JsonStore(path.join(DATA_DIR, 'orders.json'), {});
idem.load();
orders.load();
for (const [k, v] of Object.entries(idem.data)) if (v.state !== 'done') delete idem.data[k]; // no request survives a restart
const inFlight = new Map(); // key -> bodySha256

const broker = new Broker('intake');

// ---- helpers --------------------------------------------------------------------------------
const reply = (res, status, body, retryAfter) => {
  if (retryAfter) res.set('Retry-After', String(retryAfter));
  return res.status(status).json(body);
};
const bad = (message) => ({ status: 400, body: { code: 'validation_error', message } });
const charsetParam = (ct) => (/charset\s*=\s*"?([^";\s]+)/i.exec(ct || '') || [])[1];

/** One pricing call. ok | unknown (404) | error (anything else, timeout, no connection). */
async function price(productId, correlationId, causationId) {
  try {
    const res = await fetch(`${PRICING_URL}/pricing/${encodeURIComponent(productId)}`, {
      headers: { 'X-API-Key': PRICING_API_KEY, 'X-Correlation-Id': correlationId, 'X-Causation-Id': causationId },
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (res.status === 404) { await res.text().catch(() => {}); return { status: 'unknown' }; }
    if (!res.ok) { await res.text().catch(() => {}); return { status: 'error' }; }
    const b = await res.json();
    if (typeof b.unitPrice !== 'number' || !Number.isFinite(b.unitPrice) || typeof b.currency !== 'string' || typeof b.taxRate !== 'number') {
      return { status: 'error' };
    }
    return { status: 'ok', unitPrice: b.unitPrice.toFixed(2), currency: b.currency, taxRate: b.taxRate };
  } catch {
    return { status: 'error' };
  }
}

async function runPipeline(req, raw, key, sha, startedAt) {
  const contentType = req.get('content-type') || '';
  const mediaType = contentType.split(';')[0].trim().toLowerCase();
  const isXml = mediaType === 'application/xml' || mediaType === 'text/xml';
  const isJson = mediaType === 'application/json';
  if (!isXml && !isJson) {
    return { status: 415, body: { code: 'unsupported_media_type', message: 'use application/json or application/xml' } };
  }

  const ids = { receive: randomUUID(), translate: randomUUID(), enrich: randomUUID(), publish: randomUUID() };
  const correlationId = randomUUID();

  // 1. detect and translate (PA4)
  const tTranslate = new Date();
  let translated;
  try {
    if (isXml) {
      translated = translateB2B(raw, charsetParam(contentType));
    } else {
      let json;
      try { json = JSON.parse(raw.toString('utf8').replace(/^﻿/, '')); } catch { return bad('the body is not valid JSON'); }
      if (typeof json !== 'object' || json === null || Array.isArray(json)) return bad('the body must be a JSON object');
      if (json.orderId !== undefined && typeof json.customer === 'object' && json.customer !== null) translated = translateWeb(json);
      else if (json.oid !== undefined) translated = translateMobile(json);
      else return bad('the JSON body is neither a web order (orderId, customer) nor a mobile order (oid)');
    }
  } catch (e) {
    return bad(`cannot translate the order: ${e.message}`);
  }

  // 2. enrich every item from pricing (each distinct product once)
  const tEnrich = new Date();
  const unique = [...new Set(translated.lines.map((l) => l.productId))];
  const found = new Map(await Promise.all(unique.map(async (id) => [id, await price(id, correlationId, ids.enrich)])));
  if ([...found.values()].some((r) => r.status === 'error')) {
    return { status: 503, body: { code: 'pricing_unavailable', message: 'the pricing service is unavailable' }, retryAfter: 1 };
  }
  const unknown = unique.filter((id) => found.get(id).status === 'unknown');
  if (unknown.length > 0) {
    const body = { code: 'unknown_product', productIds: unknown };
    idem.data[key] = { bodySha256: sha, state: 'done', response: { status: 422, body }, createdAt: new Date().toISOString() };
    await idem.save();
    return { status: 422, body };
  }

  // 3. build the canonical order, then validate it
  const items = translated.lines.map((l) => {
    const p = found.get(l.productId);
    return { productId: l.productId, productName: l.productName, quantity: l.quantity, unitPrice: p.unitPrice, currency: p.currency, taxRate: p.taxRate };
  });
  const order = { ...translated.base, receivedAt: new Date().toISOString(), items };
  if (items.length > ITEMS_MAX) return bad(`an order may have at most ${ITEMS_MAX} items`);
  if (!validateOrder(order)) return bad(`the order is not a valid canonical order: ${ajv.errorsText(validateOrder.errors)}`);

  // 4. publish (answer only after the broker's confirm)
  const businessKey = `${order.source}:${order.orderId}`;
  const now = new Date().toISOString();
  orders.data[correlationId] = { correlationId, businessKey, orderId: order.orderId, source: order.source, orderType: order.orderType, state: 'accepted', updatedAt: now };
  const tPublish = new Date();
  const messageId = randomUUID();
  try {
    await broker.publish('orders.exchange', '', Buffer.from(JSON.stringify(order)), {
      persistent: true,
      contentType: 'application/json',
      messageId,
      headers: { correlationId, causationId: ids.publish, businessKey },
    });
  } catch (e) {
    delete orders.data[correlationId];
    return { status: 503, body: { code: 'broker_unavailable', message: 'the broker did not confirm the publish' }, retryAfter: 1 };
  }
  const tDone = new Date();

  const body = { correlationId, status: 'accepted' };
  idem.data[key] = { bodySha256: sha, state: 'done', response: { status: 202, body }, correlationId, createdAt: now };
  await Promise.all([idem.save(), orders.save()]);

  emitHops(broker, [
    makeHop({ hopId: ids.receive, correlationId, causationId: null, service: 'intake', operation: 'intake.receive', startedAt, finishedAt: new Date(), detail: { source: order.source } }),
    makeHop({ hopId: ids.translate, correlationId, causationId: ids.receive, service: 'intake', operation: 'intake.translate', startedAt: tTranslate, finishedAt: tEnrich }),
    makeHop({ hopId: ids.enrich, correlationId, causationId: ids.translate, service: 'intake', operation: 'intake.enrich', startedAt: tEnrich, finishedAt: tPublish }),
    makeHop({ hopId: ids.publish, correlationId, causationId: ids.enrich, service: 'intake', operation: 'intake.publish', messageId, startedAt: tPublish, finishedAt: tDone }),
  ]);
  return { status: 202, body };
}

// ---- HTTP -----------------------------------------------------------------------------------
const app = express();
app.get('/health', healthHandler(broker));

app.post('/orders', express.raw({ type: () => true, limit: '5mb' }), async (req, res) => {
  const startedAt = new Date();
  const raw = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
  const key = req.get('idempotency-key');
  if (typeof key !== 'string' || !KEY_RE.test(key)) {
    const b = bad('the Idempotency-Key header is required: 1-128 characters from [A-Za-z0-9._:-]');
    return reply(res, b.status, b.body);
  }
  if (!broker.up) return reply(res, 503, { code: 'broker_unavailable', message: 'the broker is down' }, 1);

  const sha = createHash('sha256').update(raw).digest('hex');
  const done = idem.data[key];
  const flying = inFlight.get(key);
  if (done || flying) {
    if ((done ? done.bodySha256 : flying) !== sha) {
      return reply(res, 409, { code: 'idempotency_payload_mismatch', message: 'this key was used with a different body' });
    }
    if (!done) return reply(res, 409, { code: 'idempotency_conflict', message: 'a request with this key is still being processed' });
    return reply(res, done.response.status, done.response.body);
  }

  inFlight.set(key, sha);
  try {
    const out = await runPipeline(req, raw, key, sha, startedAt);
    return reply(res, out.status, out.body, out.retryAfter);
  } catch (e) {
    console.error('[intake] unexpected error:', e);
    return reply(res, 500, { code: 'internal_error', message: 'unexpected error' });
  } finally {
    inFlight.delete(key);
  }
});

app.get('/orders/:correlationId', (req, res) => {
  const view = orders.data[req.params.correlationId];
  if (!view) return res.status(404).json({ code: 'not_found', message: 'unknown correlationId' });
  return res.status(200).json(view);
});


// ---- DLQ operations (README §4.6) -----------------------------------------------------------
const DLQ_QUEUE = 'orders.dlq';
let dlqChain = Promise.resolve();
/** One DLQ operation at a time: a peek holds every message unacked, so two must not overlap. */
const exclusive = (fn) => { const run = dlqChain.then(fn, fn); dlqChain = run.catch(() => {}); return run; };

async function takeAll(ch) {
  const msgs = [];
  for (let i = 0; i < 10000; i++) {
    const m = await ch.get(DLQ_QUEUE, { noAck: false });
    if (!m) break;
    msgs.push(m);
  }
  return msgs;
}

function entryOf(m) {
  const h = m.properties.headers || {};
  const str = (v) => (typeof v === 'string' && v !== '' ? v : undefined);
  const attempts = Number.isInteger(h.attempts) && h.attempts >= 1 ? h.attempts : getRetryCount(m) + 1;
  const at = str(h.deadLetteredAt) || (m.properties.timestamp ? new Date(m.properties.timestamp * 1000).toISOString() : new Date().toISOString());
  return {
    messageId: str(m.properties.messageId) || '',
    correlationId: str(h.correlationId) || '',
    originQueue: str(h.originQueue) || 'unknown',
    attempts,
    lastError: str(h.lastError) || 'unknown',
    deadLetteredAt: at,
  };
}

const confirmPublish = (ch, exchange, key, content, options) =>
  new Promise((resolve, reject) => ch.publish(exchange, key, content, options, (err) => (err ? reject(err) : resolve())));

async function withDlqChannel(fn) {
  if (!broker.up || !broker.conn) { const e = new Error('broker_unavailable'); e.status = 503; throw e; }
  const ch = await broker.conn.createConfirmChannel();
  ch.on('error', () => {}); // a closed channel hands its unacked messages back
  try { return await fn(ch); } finally { await ch.close().catch(() => {}); }
}

app.get('/dlq', async (_req, res) => {
  try {
    const entries = await exclusive(() => withDlqChannel(async (ch) => {
      const msgs = await takeAll(ch);
      const list = msgs.map(entryOf);
      for (const m of msgs) ch.nack(m, false, true); // look, do not take (R-4.6.2)
      return list;
    }));
    res.status(200).json({ count: entries.length, entries });
  } catch (e) {
    res.status(e.status || 503).json({ code: 'broker_unavailable', message: e.message });
  }
});

app.post('/dlq/replay', express.json({ limit: '100kb' }), async (req, res) => {
  const body = req.body && typeof req.body === 'object' && !Array.isArray(req.body) ? req.body : {};
  if (body.correlationId !== undefined && (typeof body.correlationId !== 'string' || body.correlationId === '')) {
    return res.status(400).json({ code: 'validation_error', message: 'correlationId must be a non-empty string' });
  }
  try {
    const out = await exclusive(() => withDlqChannel(async (ch) => {
      const msgs = await takeAll(ch);
      let replayed = 0;
      let remaining = 0;
      let failed = false;
      for (const m of msgs) {
        const e = entryOf(m);
        const wanted = body.correlationId === undefined || e.correlationId === body.correlationId;
        if (failed || !wanted || e.originQueue === 'unknown') { ch.nack(m, false, true); remaining++; continue; }
        const h = m.properties.headers || {};
        const started = new Date();
        const hopId = randomUUID();
        const headers = withoutRetryHistory(h);
        delete headers.attempts;
        delete headers.deadLetteredAt;
        headers.causationId = hopId;
        try {
          await confirmPublish(ch, 'orders.return.exchange', e.originQueue, m.content, {
            persistent: true, contentType: m.properties.contentType, messageId: randomUUID(), headers,
          });
        } catch {
          ch.nack(m, false, true); remaining++; failed = true; continue;
        }
        ch.ack(m); // only after the broker confirmed the republish (R-4.6.5)
        replayed++;
        if (e.correlationId) {
          emitHops(broker, [makeHop({
            hopId, correlationId: e.correlationId, causationId: typeof h.causationId === 'string' ? h.causationId : null,
            service: 'intake', operation: 'dlq.replay', startedAt: started, finishedAt: new Date(),
            detail: { originQueue: e.originQueue },
          })]);
        }
      }
      return { replayed, remaining, failed };
    }));
    if (out.failed) return res.status(503).json({ code: 'broker_unavailable', replayed: out.replayed, remaining: out.remaining });
    return res.status(200).json({ replayed: out.replayed, remaining: out.remaining });
  } catch (e) {
    return res.status(e.status || 503).json({ code: 'broker_unavailable', message: e.message });
  }
});

app.use((err, _req, res, _next) => {
  console.error('[intake] request error:', err.message);
  res.status(400).json({ code: 'validation_error', message: err.message });
});


// ---- following the saga's lifecycle (README §4.4) ----------------------------------------------
const STATUS_QUEUE = 'intake.status';
const TERMINAL = new Set(['completed', 'failed', 'compensated', 'duplicate']);
const FROM_PROCESSING = ['parked', 'completed', 'failed', 'compensated', 'compensation_failed', 'needs_attention'];

function canMove(from, to) {
  if (from === to || TERMINAL.has(from)) return false;
  switch (from) {
    case 'accepted': return true;
    case 'processing': return FROM_PROCESSING.includes(to);
    case 'parked': return to === 'processing' || FROM_PROCESSING.includes(to);
    case 'compensation_failed':
    case 'needs_attention': return to === 'compensated' || to === 'compensation_failed';
    default: return false;
  }
}

async function onOutcome(msg, ch) {
  let env;
  try { env = JSON.parse(msg.content.toString('utf8')); } catch { ch.ack(msg); return; }
  const view = env && orders.data[env.correlationId];
  const to = String(msg.fields.routingKey || '').replace(/^order\./, '') || env?.state;
  if (view && canMove(view.state, to)) {
    view.state = to;
    view.updatedAt = env.occurredAt || new Date().toISOString();
    if (env.code !== undefined) view.code = env.code; else delete view.code;
    if (env.duplicateOf !== undefined) view.duplicateOf = env.duplicateOf;
    await orders.save();
  }
  ch.ack(msg);
}

broker.onReady(async (ch) => {
  await ch.assertQueue(STATUS_QUEUE, { durable: true });
  await ch.bindQueue(STATUS_QUEUE, 'orders.outcome', '#');
});
broker.consume(STATUS_QUEUE, onOutcome, 20);

process.on('unhandledRejection', (e) => console.error('[intake] unhandled rejection:', e));
app.listen(PORT, () => console.log(`[intake] listening on ${PORT}`));
broker.start();
