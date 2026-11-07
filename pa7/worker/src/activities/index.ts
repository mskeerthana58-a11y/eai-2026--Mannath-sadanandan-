/**
 * The six activities. Each makes one HTTP call to one mock service, exactly
 * the calls the PA6 orchestrator made (README.md §4).
 *
 * Failure classification, the decision PA6 never had to make:
 *   - 4xx: the service said no and will say no again. Non-retryable, with the
 *     mock's own `code` from the response body as the failure type.
 *   - 5xx, or no answer at all (network error): temporary. Retryable.
 *   - Too slow: not handled here. Temporal fails the attempt itself when
 *     startToCloseTimeout runs out. Deliberately, no AbortSignal is passed to
 *     fetch, so a call that outlives its attempt still completes at the mock
 *     (the "ran, but Temporal could not know" case that the crash test and
 *     the ADR are about).
 *
 * The signatures are fixed: the workflow calls these by name.
 */
import { ApplicationFailure } from "@temporalio/activity";
import type { CanonicalItem } from "../types";

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

// docker-compose.yml sets these. Never hardcode a downstream URL.
const PAYMENT_URL = requiredEnv("PAYMENT_URL");
const INVENTORY_URL = requiredEnv("INVENTORY_URL");
const SHIPPING_URL = requiredEnv("SHIPPING_URL");
const NOTIFICATION_URL = requiredEnv("NOTIFICATION_URL");

async function call(baseUrl: string, route: string, body: unknown): Promise<void> {
  // A network error (connection refused, reset) makes fetch throw a plain
  // TypeError, which the retry policy treats as retryable. That is what we want.
  const res = await fetch(`${baseUrl}${route}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.ok) return;

  const text = await res.text();
  let code = `http_${res.status}`;
  let message = text.slice(0, 200) || `HTTP ${res.status}`;
  try {
    const parsed = JSON.parse(text) as { code?: unknown; message?: unknown };
    if (typeof parsed.code === "string" && parsed.code) code = parsed.code;
    if (typeof parsed.message === "string" && parsed.message) message = parsed.message;
  } catch {
    // body was not JSON; keep the fallbacks
  }

  const summary = `${route} answered ${res.status} ${code}: ${message}`;
  if (res.status >= 400 && res.status < 500) {
    throw ApplicationFailure.nonRetryable(summary, code);
  }
  throw ApplicationFailure.retryable(summary, code);
}

/** POST {PAYMENT_URL}/payment/authorize with { orderId, amount }. */
export async function authorizePayment(orderId: string, amount: string): Promise<void> {
  await call(PAYMENT_URL, "/payment/authorize", { orderId, amount });
}

/** POST {INVENTORY_URL}/inventory/reserve with { orderId, items }. */
export async function reserveInventory(orderId: string, items: CanonicalItem[]): Promise<void> {
  await call(INVENTORY_URL, "/inventory/reserve", { orderId, items });
}

/** POST {SHIPPING_URL}/shipping/create with { orderId }. */
export async function createShipment(orderId: string): Promise<void> {
  await call(SHIPPING_URL, "/shipping/create", { orderId });
}

/** POST {NOTIFICATION_URL}/notification/send with { orderId, recipient }. */
export async function sendNotification(orderId: string, recipient: string): Promise<void> {
  await call(NOTIFICATION_URL, "/notification/send", { orderId, recipient });
}

/** Compensates authorizePayment: POST {PAYMENT_URL}/payment/refund with { orderId }. */
export async function refundPayment(orderId: string): Promise<void> {
  await call(PAYMENT_URL, "/payment/refund", { orderId });
}

/** Compensates reserveInventory: POST {INVENTORY_URL}/inventory/release with { orderId }. */
export async function releaseInventory(orderId: string): Promise<void> {
  await call(INVENTORY_URL, "/inventory/release", { orderId });
}
