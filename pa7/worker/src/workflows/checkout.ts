/**
 * The checkout workflow: the PA6 saga, ported.
 *
 * Four forward steps (payment, inventory, shipping, notification). On a
 * failure, compensate what completed, newest first (inventory_release, then
 * payment_refund). Every outcome is a returned CheckoutResult; the workflow
 * never throws for a business or downstream failure.
 *
 * Workflow code is deterministic and replayed from history: no I/O here, only
 * activities through the proxies below, and only types from ../activities.
 */
import { ActivityFailure, ApplicationFailure, TimeoutFailure, proxyActivities } from "@temporalio/workflow";
import type * as activities from "../activities";
import type { CanonicalOrder, CheckoutResult } from "../types";

type TraceItem = CheckoutResult["trace"][number];

// Forward steps. 2.5 s per attempt sits between the 2 s shipping delay that
// must succeed and the 9 s delay that must time out. Three attempts, with
// 0.2 s then 0.4 s of backoff: the slowest correct saga (three timed-out
// shipping attempts, then compensation) stays near 10 s, well inside the
// api's 45 s wait. A 4xx never gets here twice: the activity marks it
// non-retryable, so the policy is not consulted.
const act = proxyActivities<typeof activities>({
  startToCloseTimeout: "2500 ms",
  retry: {
    maximumAttempts: 3,
    initialInterval: "200 ms",
    backoffCoefficient: 2,
  },
});

// Compensations use the same retry policy as the forward steps (README §4):
// bounded, so the workflow always finishes with a result.
const compensate = act;

type Outcome = { ok: true } | { ok: false; status: "timeout" | "failed"; code: string };

/** Turns the failure of an activity (after its retries) into a code. */
function classify(err: unknown): Outcome {
  if (err instanceof ActivityFailure) {
    const cause = err.cause;
    if (cause instanceof TimeoutFailure) return { ok: false, status: "timeout", code: "timeout" };
    if (cause instanceof ApplicationFailure) {
      return { ok: false, status: "failed", code: cause.type ?? "activity_failed" };
    }
    return { ok: false, status: "failed", code: "activity_failed" };
  }
  // Not an activity failure (for example cancellation): let it propagate.
  throw err;
}

/** Runs one activity call and records it in the trace. */
async function run(trace: TraceItem[], step: string, call: () => Promise<void>): Promise<Outcome> {
  const startedAt = Date.now();
  let outcome: Outcome = { ok: true };
  try {
    await call();
  } catch (err) {
    outcome = classify(err);
  }
  const finishedAt = Date.now();
  trace.push({
    step,
    status: outcome.ok ? "success" : outcome.status,
    startedAt: new Date(startedAt).toISOString(),
    finishedAt: new Date(finishedAt).toISOString(),
    durationMs: Math.max(0, finishedAt - startedAt),
  });
  return outcome;
}

/** unitPrice x quantity summed in whole cents, as a two-digit decimal string. */
function derivedAmount(order: CanonicalOrder): string {
  let cents = 0;
  for (const item of order.items) {
    const [whole = "0", fraction = ""] = item.unitPrice.split(".");
    cents += (Number(whole) * 100 + Number(fraction.padEnd(2, "0").slice(0, 2))) * item.quantity;
  }
  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}

export async function checkout(order: CanonicalOrder): Promise<CheckoutResult> {
  const { orderId } = order;
  const trace: TraceItem[] = [];

  let paymentTaken = false;
  let inventoryReserved = false;
  let failure: Extract<Outcome, { ok: false }> | undefined;

  const forward: Array<[string, () => Promise<void>, () => void]> = [
    ["payment", () => act.authorizePayment(orderId, derivedAmount(order)), () => (paymentTaken = true)],
    ["inventory", () => act.reserveInventory(orderId, order.items), () => (inventoryReserved = true)],
    ["shipping", () => act.createShipment(orderId), () => undefined],
    ["notification", () => act.sendNotification(orderId, order.customer.email), () => undefined],
  ];

  for (const [step, call, onSuccess] of forward) {
    const outcome = await run(trace, step, call);
    if (outcome.ok) {
      onSuccess();
    } else {
      failure = outcome;
      break;
    }
  }

  if (!failure) {
    return { orderId, status: "completed", trace };
  }

  // Nothing to undo: the very first step was refused.
  if (!paymentTaken && !inventoryReserved) {
    return { orderId, status: "failed", code: failure.code, trace };
  }

  // Compensate newest first. A failed compensation is recorded and does not
  // stop the ones after it: a stuck release must not keep the refund from
  // being tried.
  let compensationFailed = false;
  if (inventoryReserved) {
    const outcome = await run(trace, "inventory_release", () => compensate.releaseInventory(orderId));
    if (!outcome.ok) compensationFailed = true;
  }
  if (paymentTaken) {
    const outcome = await run(trace, "payment_refund", () => compensate.refundPayment(orderId));
    if (!outcome.ok) compensationFailed = true;
  }

  if (compensationFailed) {
    return { orderId, status: "failed", code: "compensation_failed", trace };
  }
  return { orderId, status: "compensated", code: failure.code, trace };
}
