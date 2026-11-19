import amqp from 'amqplib';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export class Broker {
  constructor(name) {
    this.name = name;
    this.url = process.env.RABBITMQ_URL;
    this.conn = null;
    this.ch = null;
    this.up = false;
    this.setups = [];
    this.expected = new Set();
    this.active = new Set();
    this.connecting = false;
  }

  /** Work to redo on every (re)connect, e.g. a consumer. */
  onReady(fn) {
    this.setups.push(fn);
    if (this.up) fn(this.ch).catch((e) => console.error(`[${this.name}] setup failed: ${e.message}`));
  }

  consume(queue, handler, prefetch = 1) {
    this.expected.add(queue);
    this.onReady(async (ch) => {
      await ch.prefetch(prefetch);
      await ch.consume(queue, (msg) => {
        if (!msg) return;
        Promise.resolve(handler(msg, ch)).catch((e) => {
          console.error(`[${this.name}] handler error: ${e.message}`);
          try { ch.nack(msg, false, false); } catch {}
        });
      });
      this.active.add(queue);
    });
  }

  async start() {
    if (this.connecting) return;
    this.connecting = true;
    let delay = 500;
    for (;;) {
      let conn = null;
      try {
        conn = await amqp.connect(this.url);
        const ch = await conn.createConfirmChannel();
        conn.on('error', (e) => console.error(`[${this.name}] connection error: ${e.message}`));
        conn.on('close', () => {
          if (this.conn !== conn) return;
          console.warn(`[${this.name}] broker connection closed, reconnecting`);
          this.up = false; this.active.clear(); this.conn = null; this.ch = null;
          this.connecting = false;
          this.start();
        });
        ch.on('error', (e) => console.error(`[${this.name}] channel error: ${e.message}`));
        ch.on('close', () => {
          if (this.ch === ch) { this.up = false; conn.close().catch(() => {}); }
        });
        for (const fn of this.setups) await fn(ch);
        this.conn = conn; this.ch = ch; this.up = true; this.connecting = false;
        console.log(`[${this.name}] connected to broker`);
        return;
      } catch (e) {
        console.error(`[${this.name}] broker connect failed: ${e.message}`);
        this.active.clear();
        try { if (conn) await conn.close(); } catch {}
        await sleep(delay);
        delay = Math.min(delay * 2, 2000);
      }
    }
  }

  /** Resolves only after the broker has confirmed the publish. */
  publish(exchange, key, content, options = {}) {
    return new Promise((resolve, reject) => {
      if (!this.up || !this.ch) return reject(new Error('broker_unavailable'));
      try {
        this.ch.publish(exchange, key, content, options, (err) => (err ? reject(err) : resolve()));
      } catch (e) { reject(e); }
    });
  }
}

export function healthHandler(broker) {
  return (_req, res) => {
    const consumers = {};
    for (const q of broker.expected) consumers[q] = broker.active.has(q) ? 1 : 0;
    const ok = broker.up && [...broker.expected].every((q) => broker.active.has(q));
    if (ok) res.status(200).json({ status: 'ok', checks: { broker: 'up', consumers } });
    else res.status(503).json({ status: 'degraded', checks: { broker: broker.up ? 'up' : 'down', consumers } });
  };
}
