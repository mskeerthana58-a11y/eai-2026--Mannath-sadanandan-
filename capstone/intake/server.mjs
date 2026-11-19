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

app.use((err, _req, res, _next) => {
  console.error('[intake] request error:', err.message);
  res.status(400).json({ code: 'validation_error', message: err.message });
});

process.on('unhandledRejection', (e) => console.error('[intake] unhandled rejection:', e));
app.listen(PORT, () => console.log(`[intake] listening on ${PORT}`));
broker.start();
