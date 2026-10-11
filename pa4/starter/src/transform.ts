/**
 * PA4 — three sources to one canonical order model.
 *
 * translator (shape mapping) + enricher (pricing API) + filter (no payment data).
 * The filter is done by construction: every canonical object is built from
 * explicitly named fields (an allow-list). Source objects are never spread or
 * copied wholesale, so payment fields cannot leak.
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { XMLParser } from "fast-xml-parser";

// ---------------------------------------------------------------- contract --

export interface Address {
  street: string;
  city: string;
  postalCode: string;
  country: string;
}

export interface CanonicalCustomer {
  name: string;
  email: string;
  address: Address;
}

export interface CanonicalItem {
  productId: string;
  productName: string;
  quantity: number;
  /** Decimal STRING, e.g. "24.99" — see toDecimalAmount() below. Never a number. */
  unitPrice: string;
  currency: string;
  taxRate: number;
}

export type OrderStatus = "new" | "processing" | "shipped" | "delivered";

export interface CanonicalOrder {
  orderId: string;
  orderType: "standard" | "express" | "b2b";
  source: "web" | "mobile" | "b2b";
  receivedAt: string;
  orderDate: string;
  customer: CanonicalCustomer;
  items: CanonicalItem[];
  currency: string;
  status: OrderStatus;
}

export type TransformWarningCode = "UNKNOWN_PRODUCT" | "PRICING_API_ERROR";

export interface TransformWarning {
  code: TransformWarningCode;
  productId: string;
  message: string;
}

export interface TransformResult {
  order: CanonicalOrder | null;
  warnings: TransformWarning[];
}

export interface TranslateOptions {
  pricingBaseUrl: string;
  apiKey?: string;
  now?: () => Date;
}

export const DEFAULT_PRICING_API_KEY = "pa4-pricing-key-2026";

// ------------------------------------------------------------------- paths --

const PA4_ROOT = fileURLToPath(new URL("../../", import.meta.url));

export const DEFAULT_WEB_ORDER_PATH = path.join(PA4_ROOT, "data", "web-order.json");
export const DEFAULT_MOBILE_ORDER_PATH = path.join(PA4_ROOT, "data", "mobile-order.json");
export const DEFAULT_B2B_ORDER_PATH = path.join(PA4_ROOT, "data", "b2b-order.xml");
export const DEFAULT_PRICING_BASE_URL = "http://localhost:4100";

const OUT_DIR = path.join(PA4_ROOT, "out");

// ------------------------------------------------------------- raw shapes --

export interface WebOrderInput {
  orderId: string;
  orderType: string;
  customer: {
    name: string;
    email: string;
    address: Address;
    payment: {
      method: string;
      cardHolder: string;
      cardNumber: string;
      expiryMonth: number;
      expiryYear: number;
    };
  };
  items: Array<{ productId: string; productName: string; quantity: number }>;
  orderDate: string;
  status: string;
  currency: string;
}

export interface MobileOrderInput {
  oid: string;
  ot: string;
  cust_name: string;
  cust_email: string;
  addr: string;
  items: Array<{ pid: string; pname: string; qty: number }>;
  ts: number;
  st: number;
  cur: number;
  pm: string;
  pan: string;
  pexp: string;
}

export interface ParsedB2BOrder {
  PurchaseOrder: {
    "@_orderId": string;
    "@_orderType": string;
    "@_orderDate": string;
    BuyerParty: {
      Name: string;
      ContactEmail: string;
      ShipToAddress: {
        "@_country": string;
        Street: string;
        City: string;
        PostalCode: string;
      };
      [key: string]: unknown;
    };
    LineItems: {
      "@_currency": string;
      LineItem: unknown;
    };
    Status: string;
    [key: string]: unknown;
  };
}

const xmlParser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });

// ------------------------------------------------------------------ helpers --

type Rec = Record<string, unknown>;

/** Treat an unknown value as an object, or an empty one. */
function asRec(x: unknown): Rec {
  return typeof x === "object" && x !== null && !Array.isArray(x) ? (x as Rec) : {};
}

/** fast-xml-parser gives an object for one element and an array for several. */
function asArray(x: unknown): unknown[] {
  if (x === undefined || x === null) return [];
  return Array.isArray(x) ? x : [x];
}

/** Text values may be parsed into numbers by the XML parser; force a string. */
function str(x: unknown): string {
  return x === undefined || x === null ? "" : String(x);
}

function readJson(filePath: string): unknown {
  const text = readFileSync(filePath, "utf8").replace(/^\uFEFF/, "");
  return JSON.parse(text);
}

/** Re-serialize so all three sources give byte-identical ISO strings. */
function toIso(value: string): string {
  return new Date(value).toISOString();
}

export function normalizeProductId(source: "web" | "mobile" | "b2b", rawId: string): string {
  const id = rawId.trim();
  switch (source) {
    case "web":
      return id;
    case "mobile":
      return /^\d+$/.test(id) ? `PROD-${id}` : id;
    case "b2b":
      return id.startsWith("SKU-") ? id.slice("SKU-".length) : id;
  }
}

/**
 * Format a number from the pricing API as a two-decimal string. The JSON
 * parser has already dropped trailing zeros (22.50 -> 22.5), so String()
 * would give "4.5"; toFixed(2) gives "4.50".
 */
export function toDecimalAmount(price: number): string {
  return price.toFixed(2);
}

export type EnrichResult =
  | { status: "ok"; unitPrice: string; currency: string; taxRate: number; productName: string }
  | { status: "unknown_product" }
  | { status: "error"; httpStatus: number };

export async function enrichFromPricing(
  productId: string,
  options: TranslateOptions,
): Promise<EnrichResult> {
  const base = options.pricingBaseUrl.replace(/\/+$/, "");
  const url = `${base}/pricing/${encodeURIComponent(productId)}`;

  let res: Response;
  try {
    res = await fetch(url, {
      headers: { "X-API-Key": options.apiKey ?? DEFAULT_PRICING_API_KEY },
      signal: AbortSignal.timeout(10_000),
    });
  } catch {
    // network failure or timeout: no HTTP status exists, use 0
    return { status: "error", httpStatus: 0 };
  }

  if (res.status === 404) return { status: "unknown_product" };
  if (!res.ok) return { status: "error", httpStatus: res.status };

  let body: Rec;
  try {
    body = asRec(await res.json());
  } catch {
    return { status: "error", httpStatus: res.status };
  }

  const { unitPrice, currency, taxRate, productName } = body;
  if (
    typeof unitPrice !== "number" ||
    !Number.isFinite(unitPrice) ||
    typeof currency !== "string" ||
    typeof taxRate !== "number"
  ) {
    return { status: "error", httpStatus: res.status };
  }

  return {
    status: "ok",
    unitPrice: toDecimalAmount(unitPrice),
    currency,
    taxRate,
    productName: typeof productName === "string" ? productName : "",
  };
}

export async function enrichItems(
  lines: Array<{ productId: string; productName: string; quantity: number }>,
  options: TranslateOptions,
): Promise<{ items: CanonicalItem[]; warnings: TransformWarning[] }> {
  // Promise.all keeps results in input order, so item order matches the source.
  const results = await Promise.all(lines.map((l) => enrichFromPricing(l.productId, options)));

  const items: CanonicalItem[] = [];
  const warnings: TransformWarning[] = [];

  lines.forEach((line, i) => {
    const r = results[i];
    if (r === undefined) return;
    if (r.status === "ok") {
      items.push({
        productId: line.productId,
        productName: line.productName, // from the SOURCE line
        quantity: line.quantity,
        unitPrice: r.unitPrice,
        currency: r.currency,
        taxRate: r.taxRate,
      });
    } else if (r.status === "unknown_product") {
      warnings.push({
        code: "UNKNOWN_PRODUCT",
        productId: line.productId,
        message: `Pricing API does not know product ${line.productId}; item dropped.`,
      });
    } else {
      warnings.push({
        code: "PRICING_API_ERROR",
        productId: line.productId,
        message: `Pricing API failed for ${line.productId} (HTTP ${r.httpStatus}); item dropped.`,
      });
    }
  });

  return { items, warnings };
}

export function epochSecondsToIso(epochSeconds: number): string {
  return new Date(epochSeconds * 1000).toISOString();
}

/** Unknown codes pass through as a string (tolerant reader) rather than crash. */
export function mapCurrencyCode(numericCode: number): string {
  switch (numericCode) {
    case 978:
      return "EUR";
    case 840:
      return "USD";
    case 826:
      return "GBP";
    default:
      return String(numericCode);
  }
}

export function mapMobileStatus(code: number): OrderStatus {
  switch (code) {
    case 1:
      return "new";
    case 2:
      return "processing";
    case 3:
      return "shipped";
    case 4:
      return "delivered";
    default:
      throw new Error(`Unknown mobile status code: ${code}`);
  }
}

export function mapB2BStatus(raw: string): OrderStatus {
  const s = raw.trim().toLowerCase();
  if (s === "new" || s === "processing" || s === "shipped" || s === "delivered") return s;
  throw new Error(`Unknown B2B status: ${raw}`);
}

/**
 * "Street, City, PostalCode, Country". Counting from the END means a comma
 * inside the street name does not shift the other three fields.
 */
export function parseMobileAddress(addr: string): Address {
  const parts = addr.split(", ").map((p) => p.trim());
  if (parts.length < 4) {
    throw new Error(`Mobile address needs 4 comma-separated parts, got ${parts.length}: ${addr}`);
  }
  const country = parts[parts.length - 1] ?? "";
  const postalCode = parts[parts.length - 2] ?? "";
  const city = parts[parts.length - 3] ?? "";
  const street = parts.slice(0, parts.length - 3).join(", ");
  return { street, city, postalCode, country };
}

export function decodeB2BXmlBytes(bytes: Buffer): string {
  return new TextDecoder("windows-1257").decode(bytes);
}

// -------------------------------------------------------------- translators --

export async function translateWeb(
  webOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
  const raw = readJson(webOrderPath) as WebOrderInput;

  const lines = raw.items.map((it) => ({
    productId: normalizeProductId("web", it.productId),
    productName: it.productName,
    quantity: it.quantity,
  }));
  const { items, warnings } = await enrichItems(lines, options);

  if (items.length === 0) return { order: null, warnings };

  const order: CanonicalOrder = {
    orderId: raw.orderId,
    orderType: raw.orderType as CanonicalOrder["orderType"],
    source: "web",
    receivedAt: (options.now?.() ?? new Date()).toISOString(),
    orderDate: toIso(raw.orderDate),
    customer: {
      name: raw.customer.name,
      email: raw.customer.email,
      address: {
        street: raw.customer.address.street,
        city: raw.customer.address.city,
        postalCode: raw.customer.address.postalCode,
        country: raw.customer.address.country,
      },
    },
    items,
    currency: raw.currency,
    status: raw.status as OrderStatus,
  };
  return { order, warnings };
}

export async function translateMobile(
  mobileOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
  const raw = readJson(mobileOrderPath) as MobileOrderInput;

  const lines = raw.items.map((it) => ({
    productId: normalizeProductId("mobile", String(it.pid)),
    productName: it.pname,
    quantity: it.qty,
  }));
  const { items, warnings } = await enrichItems(lines, options);

  if (items.length === 0) return { order: null, warnings };

  const order: CanonicalOrder = {
    orderId: raw.oid,
    orderType: raw.ot as CanonicalOrder["orderType"],
    source: "mobile",
    receivedAt: (options.now?.() ?? new Date()).toISOString(),
    orderDate: epochSecondsToIso(raw.ts),
    customer: {
      name: raw.cust_name,
      email: raw.cust_email,
      address: parseMobileAddress(raw.addr),
    },
    items,
    currency: mapCurrencyCode(raw.cur),
    status: mapMobileStatus(raw.st),
  };
  return { order, warnings };
}

export async function translateB2B(
  b2bOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
  // Read BYTES, then decode: the file is windows-1257, not UTF-8.
  const text = decodeB2BXmlBytes(readFileSync(b2bOrderPath));
  const parsed = xmlParser.parse(text) as unknown;

  const po = asRec(asRec(parsed)["PurchaseOrder"]);
  const buyer = asRec(po["BuyerParty"]);
  const ship = asRec(buyer["ShipToAddress"]);
  const lineItems = asRec(po["LineItems"]);

  const lines = asArray(lineItems["LineItem"]).map((li) => {
    const item = asRec(li);
    return {
      productId: normalizeProductId("b2b", str(item["@_sku"])),
      productName: str(item["Description"]),
      quantity: Number(item["@_quantity"]),
      // UnitListPrice is intentionally ignored: legacy list price, not live.
    };
  });
  const { items, warnings } = await enrichItems(lines, options);

  if (items.length === 0) return { order: null, warnings };

  const order: CanonicalOrder = {
    orderId: str(po["@_orderId"]),
    orderType: str(po["@_orderType"]) as CanonicalOrder["orderType"],
    source: "b2b",
    receivedAt: (options.now?.() ?? new Date()).toISOString(),
    orderDate: toIso(str(po["@_orderDate"])),
    customer: {
      name: str(buyer["Name"]),
      email: str(buyer["ContactEmail"]),
      address: {
        street: str(ship["Street"]),
        city: str(ship["City"]),
        postalCode: str(ship["PostalCode"]),
        country: str(ship["@_country"]),
      },
    },
    items,
    currency: str(lineItems["@_currency"]),
    status: mapB2BStatus(str(po["Status"])),
  };
  return { order, warnings };
}

// -------------------------------------------------------------------- main --

export async function main(): Promise<void> {
  const options: TranslateOptions = { pricingBaseUrl: DEFAULT_PRICING_BASE_URL };

  const results = {
    web: await translateWeb(DEFAULT_WEB_ORDER_PATH, options),
    mobile: await translateMobile(DEFAULT_MOBILE_ORDER_PATH, options),
    b2b: await translateB2B(DEFAULT_B2B_ORDER_PATH, options),
  };

  mkdirSync(OUT_DIR, { recursive: true });
  for (const [name, result] of Object.entries(results)) {
    const outPath = path.join(OUT_DIR, `${name}.json`);
    writeFileSync(outPath, JSON.stringify(result, null, 2) + "\n", "utf8");
    console.log(`wrote ${outPath}`);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
