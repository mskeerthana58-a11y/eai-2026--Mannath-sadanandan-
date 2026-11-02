/**
 * PA6 orchestrator: checkout saga.
 *
 * POST /checkout takes a canonical order and runs four downstream calls in
 * strict sequence: payment, inventory, shipping, notification. The first
 * failure or timeout stops the saga and undoes the steps that completed and
 * have an undo (inventory release, then payment refund). Shipping and
 * notification have no undo endpoint.
 *
 * Draft written against the PA6 README spec. Reconcile it with the scaffold
 * you were given before submitting.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import express, { type NextFunction, type Request, type Response } from "express";
import AjvImport from "ajv";
import addFormatsImport from "ajv-formats";

// ajv and ajv-formats are CommonJS; depending on the loader the usable value
// is either the default import or its `.default`.
const Ajv: any = (AjvImport as any).default ?? AjvImport;
const addFormats: any = (addFormatsImport as any).default ?? addFormatsImport;

const app = express();
app.use(express.json());
app.use(express.static(path.join(process.cwd(), "public")));

interface Config {
  port: number;
  paymentUrl: string;
  inventoryUrl: string;
  shippingUrl: string;
  notificationUrl: string;
  requestTimeoutMs: number;
}

function readRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function loadConfig(): Config {
  return {
    port: Number(process.env.ORCHESTRATOR_PORT || 3000),
    paymentUrl: readRequiredEnv("PAYMENT_URL"),
    inventoryUrl: readRequiredEnv("INVENTORY_URL"),
    shippingUrl: readRequiredEnv("SHIPPING_URL"),
    notificationUrl: readRequiredEnv("NOTIFICATION_URL"),
    requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 2500),
  };
}

const config = loadConfig();

const DATA_DIR = "/data";
const IDEMPOTENCY_STORE_PATH = path.join(DATA_DIR, "idempotency-store.json");
const SAGA_STORE_PATH = path.join(DATA_DIR, "saga-store.json");
const CANONICAL_SCHEMA_PATH = process.env.CANONICAL_SCHEMA_PATH || "/canonical/order.schema.json";

// The contract (openapi.yaml) promises at most this many line items. The
// canonical schema has no upper bound, so the bound is enforced here.
const MAX_ITEMS = 100;

// ---------------------------------------------------------------- contract --

export interface TraceItem {
  step: string;
  status: "success" | "failed" | "timeout";
  startedAt: string;
  finishedAt: string;
  durationMs: number;
}

export type SagaState = "completed" | "failed" | "compensated";
export type IdempotencyState = "in_progress" | SagaState;

interface IdempotencyRecord {
  requestHash: string;
  state: IdempotencyState;
  httpStatus: number;
  response: unknown;
  updatedAt: string;
}

interface IdempotencyStore {
  records: Record<string, IdempotencyRecord>;
}

interface SagaRecord {
  idempotencyKey: string;
  state: SagaState;
  steps: TraceItem[];
  updatedAt: string;
}

interface SagaStore {
  sagas: Record<string, SagaRecord>;
}

interface CanonicalItem {
  productId: string;
  productName: string;
  quantity: number;
  unitPrice: string;
  currency: string;
  taxRate: number;
}

interface CanonicalOrder {
  orderId: string;
  customer: { name: string; email: string };
  items: CanonicalItem[];
}

// ------------------------------------------------------------- persistence --

function nowIso(): string {
  return new Date().toISOString();
}

function ensureJsonFile(filePath: string, initialData: unknown): void {
  const dirPath = path.dirname(filePath);
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, JSON.stringify(initialData, null, 2), "utf8");
  }
}

function readJsonFile<T>(filePath: string, initialData: unknown): T {
  ensureJsonFile(filePath, initialData);
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw || "{}") as T;
}

// Write to a temporary file in the same directory and rename it over the
// target, so a crash mid-write leaves the old file intact instead of a
// half-written one.
function writeJsonFile(filePath: string, value: unknown): void {
  const tmpPath = `${filePath}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(value, null, 2), "utf8");
  fs.renameSync(tmpPath, filePath);
}

function loadIdempotencyStore(): IdempotencyStore {
  const store = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH, { records: {} });
  if (!store.records) store.records = {};
  return store;
}

function loadSagaStore(): SagaStore {
  const store = readJsonFile<SagaStore>(SAGA_STORE_PATH, { sagas: {} });
  if (!store.sagas) store.sagas = {};
  return store;
}

function bootstrapStores(): void {
  ensureJsonFile(IDEMPOTENCY_STORE_PATH, { records: {} });
  ensureJsonFile(SAGA_STORE_PATH, { sagas: {} });
}

// Key order must not change the hash: the same order serialised with its
// fields in another order is the same request.
function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((v) => stableStringify(v)).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj).sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function payloadHash(payload: unknown): string {
  const hash = crypto.createHash("sha256").update(stableStringify(payload)).digest("hex");
  return `sha256:${hash}`;
}

// -------------------------------------------------------------- validation --

const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const canonicalSchema = JSON.parse(fs.readFileSync(CANONICAL_SCHEMA_PATH, "utf8"));
const validateOrder = ajv.compile(canonicalSchema);

function describeSchemaErrors(errors: Array<{ instancePath?: string; message?: string }> | null | undefined): string {
  if (!errors || errors.length === 0) return "Request body is not a valid canonical order";
  const first = errors[0];
  const where = first?.instancePath ? first.instancePath : "body";
  return `Invalid canonical order: ${where} ${first?.message ?? "is invalid"}`;
}

// ------------------------------------------------------------------- money --

// Whole cents as BigInt, never floats: parseFloat("18.90") * 100 is
// 1889.9999999999998.
function toCents(price: string): bigint {
  const match = /^(-?)(\d+)\.(\d{2})$/.exec(price);
  if (!match) throw new Error(`Not a two-digit decimal string: ${price}`);
  const sign = match[1] === "-" ? -1n : 1n;
  return sign * (BigInt(match[2] as string) * 100n + BigInt(match[3] as string));
}

function formatCents(total: bigint): string {
  const whole = total / 100n;
  const fraction = (total % 100n).toString().padStart(2, "0");
  return `${whole}.${fraction}`;
}

function deriveAmount(items: CanonicalItem[]): bigint {
  let total = 0n;
  for (const item of items) {
    total += toCents(item.unitPrice) * BigInt(item.quantity);
  }
  return total;
}

// -------------------------------------------------------------------- saga --

interface UndoCall {
  name: "payment_refund" | "inventory_release";
  url: string;
  body: Record<string, unknown>;
}

interface ForwardStep {
  name: "payment" | "inventory" | "shipping" | "notification";
  url: string;
  body: Record<string, unknown>;
  undo?: UndoCall;
}

interface CallResult {
  item: TraceItem;
  outcome: "success" | "failed" | "timeout";
  code: string;
}

// One downstream call. A call that has not answered within
// REQUEST_TIMEOUT_MS is a timeout; an answer that is not a success is a
// failure carrying the service's own code; a refused or reset connection is
// a failure with the code downstream_unreachable.
async function callDownstream(
  step: string,
  url: string,
  body: Record<string, unknown>,
  correlationId: string,
): Promise<CallResult> {
  const startedMs = Date.now();
  const startedAt = new Date(startedMs).toISOString();

  const finish = (outcome: CallResult["outcome"], code: string): CallResult => {
    const finishedMs = Date.now();
    return {
      outcome,
      code,
      item: {
        step,
        status: outcome,
        startedAt,
        finishedAt: new Date(finishedMs).toISOString(),
        durationMs: Math.max(0, finishedMs - startedMs),
      },
    };
  };

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-correlation-id": correlationId,
        "x-order-id": String(body.orderId ?? ""),
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(config.requestTimeoutMs),
    });
    const data: any = await response.json().catch((err: unknown) => {
      if (err && typeof err === "object" && (err as { name?: string }).name === "TimeoutError") throw err;
      return null;
    });
    if (response.ok && data?.ok !== false) {
      return finish("success", "");
    }
    return finish("failed", typeof data?.code === "string" ? data.code : `${step}_failed`);
  } catch (err) {
    const name = err && typeof err === "object" ? (err as { name?: string }).name : undefined;
    if (name === "TimeoutError" || name === "AbortError") {
      return finish("timeout", "timeout");
    }
    return finish("failed", "downstream_unreachable");
  }
}

interface SagaOutcome {
  httpStatus: number;
  state: SagaState;
  response: Record<string, unknown>;
}

async function runSaga(
  order: CanonicalOrder,
  amount: string,
  correlationId: string,
  trace: TraceItem[],
): Promise<SagaOutcome> {
  const orderId = order.orderId;
  const steps: ForwardStep[] = [
    {
      name: "payment",
      url: `${config.paymentUrl}/payment/authorize`,
      body: { orderId, amount },
      undo: { name: "payment_refund", url: `${config.paymentUrl}/payment/refund`, body: { orderId } },
    },
    {
      name: "inventory",
      url: `${config.inventoryUrl}/inventory/reserve`,
      body: { orderId, items: order.items },
      undo: { name: "inventory_release", url: `${config.inventoryUrl}/inventory/release`, body: { orderId } },
    },
    { name: "shipping", url: `${config.shippingUrl}/shipping/create`, body: { orderId } },
    {
      name: "notification",
      url: `${config.notificationUrl}/notification/send`,
      body: { orderId, recipient: order.customer.email },
    },
  ];

  // Strictly one after another: each call waits for the previous one.
  const completed: ForwardStep[] = [];
  let failure: CallResult | null = null;
  for (const step of steps) {
    const result = await callDownstream(step.name, step.url, step.body, correlationId);
    trace.push(result.item);
    if (result.outcome === "success") {
      completed.push(step);
    } else {
      failure = result;
      break;
    }
  }

  if (failure === null) {
    return {
      httpStatus: 200,
      state: "completed",
      response: { orderId, status: "completed", trace },
    };
  }

  // Undo what completed, in reverse order. The failed or timed-out step
  // itself is not undone: it did not complete. An undo that fails does not
  // stop the remaining undos, so one failed release does not also leave the
  // payment un-refunded.
  let compensationFailed = false;
  for (const step of [...completed].reverse()) {
    if (!step.undo) continue;
    const undo = await callDownstream(step.undo.name, step.undo.url, step.undo.body, correlationId);
    trace.push(undo.item);
    if (undo.outcome !== "success") compensationFailed = true;
  }

  if (compensationFailed) {
    return {
      httpStatus: 422,
      state: "failed",
      response: { orderId, status: "failed", code: "compensation_failed", trace },
    };
  }

  const timedOut = failure.outcome === "timeout";
  const state: SagaState = completed.length > 0 ? "compensated" : "failed";
  return {
    httpStatus: timedOut ? 504 : 422,
    state,
    response: { orderId, status: state, code: timedOut ? "timeout" : failure.code, trace },
  };
}

// ---------------------------------------------------------------- endpoints --

app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({ status: "ok" });
});

app.get("/debug/trace/:orderId", (req: Request, res: Response) => {
  const orderIdParam = req.params.orderId ?? "";
  const saga = loadSagaStore().sagas[orderIdParam];
  if (!saga) {
    res.status(404).json({ code: "not_found", message: "No saga found for this orderId" });
    return;
  }
  res.status(200).json(saga);
});

app.post("/checkout", async (req: Request, res: Response) => {
  const idempotencyKey = req.header("Idempotency-Key");
  if (!idempotencyKey || idempotencyKey.trim() === "") {
    res.status(400).json({ code: "validation_error", message: "Idempotency-Key header is required" });
    return;
  }

  if (!validateOrder(req.body)) {
    res.status(400).json({ code: "validation_error", message: describeSchemaErrors(validateOrder.errors) });
    return;
  }
  const order = req.body as CanonicalOrder;

  if (order.items.length > MAX_ITEMS) {
    res.status(400).json({
      code: "validation_error",
      message: `An order may have at most ${MAX_ITEMS} items`,
    });
    return;
  }

  const total = deriveAmount(order.items);
  if (total < 0n) {
    res.status(400).json({ code: "validation_error", message: "The order total must not be negative" });
    return;
  }
  const amount = formatCents(total);

  const requestHash = payloadHash(req.body);

  // Check-and-set. There is no `await` between reading the store and writing
  // the in_progress record, and Node runs this synchronously, so two requests
  // with the same key cannot both pass this block.
  const store = loadIdempotencyStore();
  const existing = store.records[idempotencyKey];
  if (existing) {
    if (existing.requestHash !== requestHash) {
      res.status(409).json({
        code: "idempotency_payload_mismatch",
        message: "This Idempotency-Key is already used for a different payload",
      });
      return;
    }
    if (existing.state === "in_progress") {
      res.status(409).json({
        code: "idempotency_conflict",
        message: "A request with this Idempotency-Key is still being processed",
      });
      return;
    }
    // Same key, same payload, finished: replay the stored result exactly.
    res.status(existing.httpStatus).json(existing.response);
    return;
  }

  const orderId = order.orderId;
  store.records[idempotencyKey] = {
    requestHash,
    state: "in_progress",
    httpStatus: 202,
    response: { orderId, status: "in_progress" },
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, store);

  const trace: TraceItem[] = [];
  let outcome: SagaOutcome;
  try {
    outcome = await runSaga(order, amount, idempotencyKey, trace);
  } catch (err) {
    // A bug, not a business outcome. Do not leave the key in_progress forever.
    console.error("[orchestrator] unexpected error in saga", err);
    outcome = {
      httpStatus: 422,
      state: "failed",
      response: { orderId, status: "failed", code: "internal_error", trace },
    };
  }

  // Saga store first, then the idempotency record that makes the result
  // replayable. Both are re-read so nothing written meanwhile is lost.
  const sagaStore = loadSagaStore();
  sagaStore.sagas[orderId] = {
    idempotencyKey,
    state: outcome.state,
    steps: trace,
    updatedAt: nowIso(),
  };
  writeJsonFile(SAGA_STORE_PATH, sagaStore);

  const finalStore = loadIdempotencyStore();
  finalStore.records[idempotencyKey] = {
    requestHash,
    state: outcome.state,
    httpStatus: outcome.httpStatus,
    response: outcome.response,
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, finalStore);

  res.status(outcome.httpStatus).json(outcome.response);
});

// A body that is not valid JSON never reaches the handler above.
app.use((err: unknown, _req: Request, res: Response, next: NextFunction) => {
  if (err && typeof err === "object" && (err as { type?: string }).type === "entity.parse.failed") {
    res.status(400).json({ code: "validation_error", message: "Request body is not valid JSON" });
    return;
  }
  next(err);
});

bootstrapStores();

app.listen(config.port, () => {
  console.log(`[orchestrator] listening on port ${config.port}`);
  console.log("[orchestrator] downstream targets loaded from env", {
    paymentUrl: config.paymentUrl,
    inventoryUrl: config.inventoryUrl,
    shippingUrl: config.shippingUrl,
    notificationUrl: config.notificationUrl,
    requestTimeoutMs: config.requestTimeoutMs,
  });
});
