/**
 * notification-service
 *
 * Consumes from `notifications.queue` and acts as an idempotent receiver:
 * each correlationId is processed at most once, even across restarts, because
 * processed ids are persisted to /data/processed-ids.json (see docs/adr-004.md).
 *
 * Per message:
 *   - correlationId already processed -> ack silently, log "duplicate skipped",
 *                                        do NOT touch notification.log.
 *   - body not valid JSON (permanent) -> orders.invalid.exchange, ack, never retried.
 *   - otherwise                       -> append a line to /data/notification.log,
 *                                        record + persist the id, ack, then publish
 *                                        a result event to results.notification.
 *   - any other thrown error          -> retry via nack(requeue=false), or DLQ_EXCHANGE
 *                                        once retries are exhausted.
 */

import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import type { Channel, ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "notifications.queue";
const RESULTS_EXCHANGE = "results.notification";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");

const PROCESSED_IDS_FILE = "/data/processed-ids.json";
const NOTIFICATION_LOG_FILE = "/data/notification.log";

/** A body that can never succeed however often it is retried (not an order). */
class PermanentFailure extends Error {}

/** Load the processed ids, creating the file as `[]` on first start. */
function loadProcessedIds(): Set<string> {
  mkdirSync(dirname(PROCESSED_IDS_FILE), { recursive: true });
  if (!existsSync(PROCESSED_IDS_FILE)) {
    writeFileSync(PROCESSED_IDS_FILE, "[]");
    return new Set();
  }
  const parsed: unknown = JSON.parse(readFileSync(PROCESSED_IDS_FILE, "utf8"));
  if (!Array.isArray(parsed)) {
    // Refuse to start rather than silently forgetting what was processed.
    throw new Error(`${PROCESSED_IDS_FILE} does not contain a JSON array`);
  }
  return new Set(parsed.filter((id): id is string => typeof id === "string"));
}

/**
 * Persist the full array. Written in place (no temp file + rename) because the
 * file may be a single-file bind mount, where rename fails with EBUSY.
 */
function persistProcessedIds(ids: Set<string>): void {
  writeFileSync(PROCESSED_IDS_FILE, JSON.stringify([...ids]));
}

/** Publish the result event. Never throws: the message is already acked by now. */
function publishResult(channel: Channel, correlationId: string | undefined, timestamp: string): void {
  try {
    channel.publish(
      RESULTS_EXCHANGE,
      "",
      Buffer.from(
        JSON.stringify({
          correlationId,
          source: "notification",
          status: "success",
          timestamp,
          details: { message: "Order received" },
        })
      ),
      {
        headers: { correlationId },
        contentType: "application/json",
        persistent: true,
      }
    );
  } catch (err) {
    console.error(`[Notification] could not publish result for ${correlationId}: ${(err as Error).message}`);
  }
}

async function main(): Promise<void> {
  const processedIds = loadProcessedIds();
  console.log(`[Notification] Loaded ${processedIds.size} processed id(s)`);

  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);

  // Declare what we publish to. Same declarations as payment-service.
  // ASSUMPTION: durable fanout exchanges, durable queues, empty binding key,
  // and the queue name "orders.invalid" — these must match README.md
  // "Declare what you publish to" EXACTLY, or RabbitMQ refuses with
  // PRECONDITION_FAILED. Fix any mismatch here.
  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");

  await channel.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  console.log(`[Notification] Consuming from ${QUEUE}`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    console.log(`[Notification] Processing ${correlationId} (attempt ${retryCount + 1})`);

    try {
      // Idempotent receiver: already handled -> ack silently, touch nothing.
      if (correlationId !== undefined && processedIds.has(correlationId)) {
        console.log(`[Notification] duplicate skipped ${correlationId}`);
        channel.ack(msg);
        return;
      }

      // Parsed inside the try so a bad body lands in the catch as a SyntaxError.
      const order = JSON.parse(msg.content.toString()) as CanonicalOrder;
      // Valid JSON that is not an order (null, [], {}) fails identically on every
      // attempt, so it is permanent too: this consumer needs orderId and customer.email.
      if (typeof order?.orderId !== "string" || typeof order?.customer?.email !== "string") {
        throw new PermanentFailure("body is not a canonical order");
      }
      const timestamp = new Date().toISOString();

      appendFileSync(
        NOTIFICATION_LOG_FILE,
        JSON.stringify({
          correlationId,
          orderId: order.orderId,
          customerEmail: order.customer.email,
          timestamp,
          message: "Order received",
        }) + "\n"
      );

      if (correlationId !== undefined) {
        processedIds.add(correlationId);
        persistProcessedIds(processedIds);
      }

      channel.ack(msg);
      publishResult(channel, correlationId, timestamp);
      console.log(`[Notification] Done ${correlationId}`);
    } catch (err) {
      // Permanent failure: a body that is not valid JSON (or not an order) never succeeds, however
      // often it is retried. Original bytes and headers go to the invalid
      // exchange, persistent; ack it; no retry, no DLQ.
      if (err instanceof SyntaxError || err instanceof PermanentFailure) {
        channel.publish(INVALID_EXCHANGE, "", msg.content, {
          headers: msg.properties.headers,
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Notification] -> Invalid message ${correlationId}: ${err.message}`);
        return;
      }

      // Retry / DLQ logic, same as payment-service.
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          // originQueue tells a later replay which queue to send it back to.
          headers: { ...msg.properties.headers, originQueue: QUEUE },
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Notification] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Notification] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`);
      }
    }
  });
}

main().catch((err) => {
  console.error("[Notification] fatal:", err);
  process.exit(1);
});
