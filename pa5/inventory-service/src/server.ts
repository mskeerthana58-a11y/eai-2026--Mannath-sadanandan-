/**
 * inventory-service
 *
 * Consumes order messages from `inventory.queue`, simulates a stock check
 * (random failure controlled by INVENTORY_FAIL_RATE), and publishes a result
 * event to the `results.inventory` exchange. Same pattern as payment-service.
 *
 * Failure handling:
 *   - Permanent (body is not valid JSON) -> orders.invalid.exchange, ack, never retried.
 *   - Temporary, retries left            -> nack(requeue=false) so the broker's
 *                                           retry-delay route handles it.
 *   - Temporary, retries exhausted       -> DLQ_EXCHANGE, then ack.
 */

import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "inventory.queue";
const RESULTS_EXCHANGE = "results.inventory";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
// `?? "10"`, not `|| 10`: a configured fail rate of 0 must stay 0.
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");
const FAIL_RATE = Number(process.env.INVENTORY_FAIL_RATE ?? "10");

async function main(): Promise<void> {
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

  console.log(`[Inventory] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    console.log(`[Inventory] Processing ${correlationId} (attempt ${retryCount + 1})`);

    try {
      // Parsed inside the try so a bad body lands in the catch as a SyntaxError.
      // The simulated stock check does not need the order's contents.
      JSON.parse(msg.content.toString()) as CanonicalOrder;

      // Simulated stock check: a random number 0-100 below FAIL_RATE is a failure.
      if (Math.random() * 100 < FAIL_RATE) {
        throw new Error("Stock check failed (simulated)");
      }

      channel.ack(msg);

      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(
          JSON.stringify({
            correlationId,
            source: "inventory",
            status: "success",
            timestamp: new Date().toISOString(),
            details: { message: "Stock check passed" },
          })
        ),
        {
          headers: { correlationId },
          contentType: "application/json",
          persistent: true,
        }
      );
      console.log(`[Inventory] Done ${correlationId}`);
    } catch (err) {
      // Permanent failure: a body that is not valid JSON never succeeds, however
      // often it is retried. Original bytes and headers go to the invalid
      // exchange, persistent; ack it; no retry, no DLQ.
      if (err instanceof SyntaxError) {
        channel.publish(INVALID_EXCHANGE, "", msg.content, {
          headers: msg.properties.headers,
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Inventory] -> Invalid message ${correlationId}: ${err.message}`);
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
        console.log(`[Inventory] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Inventory] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`);
      }
    }
  });
}

main().catch((err) => {
  console.error("[Inventory] fatal:", err);
  process.exit(1);
});
