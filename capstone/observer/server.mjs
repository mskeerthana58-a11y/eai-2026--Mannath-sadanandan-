import fs from 'node:fs';
import path from 'node:path';
import express from 'express';
import { Broker, healthHandler } from '../shared/broker.mjs';

const env = process.env;
const PORT = Number(env.PORT || 8801);
const DATA_DIR = env.DATA_DIR || '/data';
const ORCH_URL = (env.ORCHESTRATOR_URL || 'http://orchestrator:8802').replace(/\/+$/, '');

// Alert thresholds (justified in the runbook, README §6).
const SAGA_LAG_THRESHOLD = 10;
const NOTIFIER_LAG_THRESHOLD = 5;

// The management API: same host as the broker, same credentials.
const brokerUrl = new URL(env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672');
const MGMT_URL = (env.RABBITMQ_MGMT_URL || `http://${brokerUrl.hostname}:15672`).replace(/\/+$/, '');
const MGMT_AUTH = 'Basic ' + Buffer.from(`${decodeURIComponent(brokerUrl.username)}:${decodeURIComponent(brokerUrl.password)}`).toString('base64');
const VHOST = brokerUrl.pathname && brokerUrl.pathname !== '/' ? decodeURIComponent(brokerUrl.pathname.slice(1)) : '/';

const QUEUES = ['saga.queue', 'notifications.queue', 'orders.dlq', 'orders.invalid', 'trace.store'];

// ---- the trace store: /data/trace-store.jsonl, one hop per line ----------------------------
const FILE = path.join(DATA_DIR, 'trace-store.jsonl');
fs.mkdirSync(DATA_DIR, { recursive: true });
const byCid = new Map(); // correlationId -> hops
const seen = new Set(); // hopId: a redelivered hop is stored once

function index(hop) {
  if (seen.has(hop.hopId)) return false;
  seen.add(hop.hopId);
  if (!byCid.has(hop.correlationId)) byCid.set(hop.correlationId, []);
  byCid.get(hop.correlationId).push(hop);
  return true;
}

try {
  for (const line of fs.readFileSync(FILE, 'utf8').split('\n')) {
    if (!line.trim()) continue;
    try { const hop = JSON.parse(line); if (hop?.hopId && hop?.correlationId) index(hop); } catch { /* a torn last line */ }
  }
  console.log(`[observer] loaded ${seen.size} hop(s)`);
} catch (e) {
  if (e.code !== 'ENOENT') console.error(`[observer] cannot read ${FILE}: ${e.message}`);
}

const broker = new Broker('observer');

function isHop(h) {
  return h && typeof h === 'object' && typeof h.hopId === 'string' && typeof h.correlationId === 'string' && h.correlationId !== ''
    && typeof h.service === 'string' && typeof h.operation === 'string' && typeof h.startedAt === 'string';
}

async function onHop(msg, ch) {
  let hop;
  try { hop = JSON.parse(msg.content.toString('utf8')); } catch { hop = null; }
  if (!isHop(hop)) {
    // A permanent failure: park the original bytes where an operator can see them.
    try {
      await broker.publish('orders.invalid.exchange', '', msg.content, { persistent: true, headers: msg.properties.headers });
      ch.ack(msg);
    } catch { ch.nack(msg, false, true); }
    return;
  }
  if (index(hop)) fs.appendFileSync(FILE, JSON.stringify(hop) + '\n');
  ch.ack(msg);
}

// ---- metrics --------------------------------------------------------------------------------
async function queueStats(name) {
  try {
    const res = await fetch(`${MGMT_URL}/api/queues/${encodeURIComponent(VHOST)}/${encodeURIComponent(name)}`, {
      headers: { authorization: MGMT_AUTH }, signal: AbortSignal.timeout(2000),
    });
    if (!res.ok) { await res.text().catch(() => {}); return { ready: 0, unacked: 0, consumers: 0 }; }
    const q = await res.json();
    return { ready: Number(q.messages_ready) || 0, unacked: Number(q.messages_unacknowledged) || 0, consumers: Number(q.consumers) || 0 };
  } catch {
    return { ready: 0, unacked: 0, consumers: 0 };
  }
}

let lastStuck = 0;
async function stuckSagas() {
  try {
    const res = await fetch(`${ORCH_URL}/sagas/stuck`, { signal: AbortSignal.timeout(1500) });
    if (res.ok) { const b = await res.json(); lastStuck = Array.isArray(b.sagas) ? b.sagas.length : 0; }
  } catch { /* keep the last known value */ }
  return lastStuck;
}

const alert = (key, value, threshold, fires) => ({ key, firing: fires(value), value, threshold });

async function metrics() {
  const [stats, stuck] = await Promise.all([Promise.all(QUEUES.map(queueStats)), stuckSagas()]);
  const queues = Object.fromEntries(QUEUES.map((q, i) => [q, stats[i]]));
  const dlqDepth = queues['orders.dlq'].ready + queues['orders.dlq'].unacked;
  const invalidDepth = queues['orders.invalid'].ready + queues['orders.invalid'].unacked;
  const gt0 = (v) => v > 0;
  return {
    generatedAt: new Date().toISOString(),
    queues,
    dlqDepth,
    invalidDepth,
    stuckSagas: stuck,
    alerts: [
      alert('dlq_not_empty', dlqDepth, null, gt0),
      alert('invalid_not_empty', invalidDepth, null, gt0),
      alert('saga_consumer_missing', queues['saga.queue'].consumers, null, (v) => v === 0),
      alert('notifier_consumer_missing', queues['notifications.queue'].consumers, null, (v) => v === 0),
      alert('saga_consumer_lag', queues['saga.queue'].ready, SAGA_LAG_THRESHOLD, (v) => v > SAGA_LAG_THRESHOLD),
      alert('notifier_consumer_lag', queues['notifications.queue'].ready, NOTIFIER_LAG_THRESHOLD, (v) => v > NOTIFIER_LAG_THRESHOLD),
      alert('stuck_sagas', stuck, null, gt0),
    ],
  };
}

// ---- HTTP -----------------------------------------------------------------------------------
const app = express();
app.get('/health', healthHandler(broker));

app.get('/trace/:correlationId', (req, res) => {
  const hops = byCid.get(req.params.correlationId);
  if (!hops || hops.length === 0) return res.status(404).json({ code: 'not_found', message: 'no hops for this correlationId' });
  const sorted = [...hops].sort((a, b) => Date.parse(a.startedAt) - Date.parse(b.startedAt) || Date.parse(a.finishedAt) - Date.parse(b.finishedAt));
  return res.status(200).json({ correlationId: req.params.correlationId, hops: sorted });
});

app.get('/ops/metrics', async (_req, res) => {
  try {
    res.status(200).json(await metrics());
  } catch (e) {
    console.error('[observer] metrics failed:', e.message);
    res.status(503).json({ code: 'metrics_unavailable', message: e.message });
  }
});

process.on('unhandledRejection', (e) => console.error('[observer] unhandled rejection:', e));
broker.consume('trace.store', onHop, 20);
app.listen(PORT, () => console.log(`[observer] listening on ${PORT}`));
broker.start();
