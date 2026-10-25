/**
 * order-service
 *
 *   POST /orders                  accept a canonical order, publish it to orders.exchange
 *   GET  /orders/:correlationId   look an accepted order up again
 *   POST /dlq/replay              send every message in orders.dlq back to the consumer that failed it
 *   GET  /health                  liveness
 *
 * The correlationId lives ONLY in the AMQP custom header `headers.correlationId`
 * (see docs/adr-004.md); the order body is republished exactly as received.
 */

import { randomUUID } from "node:crypto";
import express from "express";
import type { Channel, GetMessage } from "amqplib";
import { connectWithRetry, withoutRetryHistory } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;

const ORDERS_EXCHANGE = "orders.exchange";
const RETURN_EXCHANGE = "orders.return.exchange";
const DLQ_QUEUE = "orders.dlq";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";

// Accepted orders, keyed by correlationId, so GET can find them again.
const orders = new Map<string, CanonicalOrder>();

// Set once connectWithRetry resolves. The HTTP listener starts immediately so
// /health answers while the broker is still coming up; routes that need the
// broker return 503 until then.
let channel: Channel | null = null;

const app = express();
app.use(express.json());

app.get("/health", (_req, res) => {
  res.status(200).json({ status: "ok" });
});

app.post("/orders", (req, res) => {
  const ch = channel;
  if (!ch) {
    res.status(503).json({ error: "message broker not connected yet" });
    return;
  }

  const body: unknown = req.body;
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    res.status(400).json({ error: "body must be a canonical order object" });
    return;
  }
  const order = body as CanonicalOrder;

  // Generated here, carried ONLY in the message header — never added to the order.
  const correlationId = randomUUID();

  try {
    ch.publish(ORDERS_EXCHANGE, "", Buffer.from(JSON.stringify(order)), {
      headers: { correlationId },
      contentType: "application/json",
      persistent: true,
    });
  } catch (err) {
    console.error("[Order] publish failed:", (err as Error).message);
    res.status(503).json({ error: "could not publish order" });
    return;
  }

  orders.set(correlationId, order);
  res.status(201).json({ correlationId, status: "accepted" });
});

app.get("/orders/:correlationId", (req, res) => {
  const correlationId = req.params.correlationId;
  const order = correlationId === undefined ? undefined : orders.get(correlationId);
  if (!order) {
    res.status(404).json({ error: "order not found" });
    return;
  }
  res.status(200).json({ correlationId, order });
});

app.post("/dlq/replay", async (_req, res) => {
  const ch = channel;
  if (!ch) {
    res.status(503).json({ error: "message broker not connected yet" });
    return;
  }

  let replayed = 0;
  const unroutable: GetMessage[] = [];

  try {
    // One message at a time from the dead letter queue.
    for (;;) {
      const msg = await ch.get(DLQ_QUEUE, { noAck: false });
      if (!msg) break; // queue is empty

      const originQueue: unknown = msg.properties.headers?.["originQueue"];
      if (typeof originQueue !== "string" || originQueue === "") {
        // No way to know which consumer failed it: leave it in the DLQ.
        // Held back and requeued after the loop so ch.get doesn't hand it straight back.
        console.warn("[Order] DLQ message has no originQueue header; leaving it in the DLQ");
        unroutable.push(msg);
        continue;
      }

      // Original bytes, back to ONLY the consumer that failed it, with a fresh
      // set of attempts (correlationId kept, x-death dropped).
      ch.publish(RETURN_EXCHANGE, originQueue, msg.content, {
        headers: withoutRetryHistory(msg.properties.headers),
        persistent: true,
      });

      // Ack from the DLQ only after republishing.
      ch.ack(msg);
      replayed += 1;
    }

    for (const msg of unroutable) {
      ch.nack(msg, false, true);
    }

    res.status(200).json({ replayed });
  } catch (err) {
    console.error("[Order] DLQ replay failed:", (err as Error).message);
    res.status(500).json({ error: "dlq replay failed", replayed });
  }
});

app.listen(PORT, () => {
  console.log(`[Order] listening on ${PORT}`);
});

async function connect(): Promise<void> {
  const { channel: ch } = await connectWithRetry(process.env.RABBITMQ_URL!);

  // Declare the dead letter channel so replay's ch.get never hits a queue
  // nobody has declared (NOT_FOUND closes the channel).
  // ASSUMPTION: durable fanout exchanges, durable queues, empty binding key,
  // and the queue name "orders.invalid" — these must match README.md
  // "Declare what you publish to" EXACTLY, or RabbitMQ refuses with
  // PRECONDITION_FAILED. Fix any mismatch here.
  await ch.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await ch.assertQueue(DLQ_QUEUE, { durable: true });
  await ch.bindQueue(DLQ_QUEUE, DLQ_EXCHANGE, "");

  await ch.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await ch.assertQueue("orders.invalid", { durable: true });
  await ch.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  channel = ch;
  console.log("[Order] connected to RabbitMQ");
}

connect().catch((err) => {
  console.error("[Order] fatal:", err);
  process.exit(1);
});
