// PA4's translators, now working on the bytes of an HTTP body instead of a file path.
// Every canonical object is built from explicitly named fields (an allow-list), so
// payment data cannot leak. Missing fields come out as undefined and fail the schema (400).
import iconv from 'iconv-lite';
import { XMLParser, XMLValidator } from 'fast-xml-parser';

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  parseTagValue: false,
  parseAttributeValue: false,
});

const rec = (x) => (typeof x === 'object' && x !== null && !Array.isArray(x) ? x : {});
const arr = (x) => (x === undefined || x === null ? [] : Array.isArray(x) ? x : [x]);
const str = (x) => (x === undefined || x === null ? '' : String(x));

function toIso(value) {
  const d = new Date(value);
  if (value === undefined || Number.isNaN(d.getTime())) throw new Error(`invalid date: ${value}`);
  return d.toISOString();
}

export function normalizeProductId(source, rawId) {
  if (typeof rawId !== 'string' && typeof rawId !== 'number') throw new Error('a product id is missing');
  const id = String(rawId).trim();
  if (!id) throw new Error('a product id is empty');
  if (source === 'mobile') return /^\d+$/.test(id) ? `PROD-${id}` : id;
  if (source === 'b2b') return id.startsWith('SKU-') ? id.slice(4) : id;
  return id;
}

function mapCurrencyCode(code) {
  if (code === undefined || code === null) throw new Error('currency is missing');
  switch (code) {
    case 978: return 'EUR';
    case 840: return 'USD';
    case 826: return 'GBP';
    default: return String(code);
  }
}

function mapMobileStatus(code) {
  switch (code) {
    case 1: return 'new';
    case 2: return 'processing';
    case 3: return 'shipped';
    case 4: return 'delivered';
    default: throw new Error(`unknown mobile status code: ${code}`);
  }
}

function mapB2BStatus(raw) {
  const s = str(raw).trim().toLowerCase();
  if (['new', 'processing', 'shipped', 'delivered'].includes(s)) return s;
  throw new Error(`unknown B2B status: ${raw}`);
}

/** "Street, City, PostalCode, Country": counting from the end, so a comma in the street is safe. */
function parseMobileAddress(addr) {
  if (typeof addr !== 'string') throw new Error('mobile address is missing');
  const parts = addr.split(', ').map((p) => p.trim());
  if (parts.length < 4) throw new Error(`mobile address needs 4 comma-separated parts, got ${parts.length}`);
  return {
    street: parts.slice(0, parts.length - 3).join(', '),
    city: parts[parts.length - 3],
    postalCode: parts[parts.length - 2],
    country: parts[parts.length - 1],
  };
}

export function translateWeb(raw) {
  if (!Array.isArray(raw.items) || raw.items.length === 0) throw new Error('the order has no items');
  const customer = rec(raw.customer);
  const addr = rec(customer.address);
  const lines = raw.items.map((it) => ({
    productId: normalizeProductId('web', rec(it).productId),
    productName: rec(it).productName,
    quantity: rec(it).quantity,
  }));
  return {
    lines,
    base: {
      orderId: raw.orderId,
      orderType: raw.orderType,
      source: 'web',
      orderDate: toIso(raw.orderDate),
      customer: {
        name: customer.name,
        email: customer.email,
        address: { street: addr.street, city: addr.city, postalCode: addr.postalCode, country: addr.country },
      },
      currency: raw.currency,
      status: raw.status,
    },
  };
}

export function translateMobile(raw) {
  if (!Array.isArray(raw.items) || raw.items.length === 0) throw new Error('the order has no items');
  if (typeof raw.ts !== 'number') throw new Error('timestamp is missing');
  const lines = raw.items.map((it) => ({
    productId: normalizeProductId('mobile', rec(it).pid),
    productName: rec(it).pname,
    quantity: rec(it).qty,
  }));
  return {
    lines,
    base: {
      orderId: raw.oid,
      orderType: raw.ot,
      source: 'mobile',
      orderDate: new Date(raw.ts * 1000).toISOString(),
      customer: { name: raw.cust_name, email: raw.cust_email, address: parseMobileAddress(raw.addr) },
      currency: mapCurrencyCode(raw.cur),
      status: mapMobileStatus(raw.st),
    },
  };
}

/** Decode by the XML declaration's encoding; a charset on the Content-Type wins (RFC 7303). */
export function decodeXml(bytes, charsetParam) {
  let enc = charsetParam;
  if (!enc) {
    const head = bytes.subarray(0, 200).toString('latin1');
    const m = /<\?xml[^>]*encoding\s*=\s*["']([^"']+)["']/i.exec(head);
    enc = m ? m[1] : 'utf-8';
  }
  if (!iconv.encodingExists(enc)) throw new Error(`unsupported XML encoding: ${enc}`);
  return iconv.decode(bytes, enc).replace(/^﻿/, '');
}

export function translateB2B(bytes, charsetParam) {
  const text = decodeXml(bytes, charsetParam);
  if (XMLValidator.validate(text) !== true) throw new Error('the body is not well-formed XML');
  const po = rec(rec(xmlParser.parse(text)).PurchaseOrder);
  const buyer = rec(po.BuyerParty);
  const ship = rec(buyer.ShipToAddress);
  const lineItems = rec(po.LineItems);
  const items = arr(lineItems.LineItem);
  if (items.length === 0) throw new Error('the order has no items');
  const lines = items.map((li) => {
    const item = rec(li);
    return {
      productId: normalizeProductId('b2b', item['@_sku']),
      productName: str(item.Description),
      quantity: item['@_quantity'] === undefined ? undefined : Number(item['@_quantity']),
      // UnitListPrice is ignored on purpose: a legacy list price, not the live one.
    };
  });
  return {
    lines,
    base: {
      orderId: po['@_orderId'],
      orderType: po['@_orderType'],
      source: 'b2b',
      orderDate: toIso(po['@_orderDate']),
      customer: {
        name: buyer.Name === undefined ? undefined : str(buyer.Name),
        email: buyer.ContactEmail === undefined ? undefined : str(buyer.ContactEmail),
        address: {
          street: ship.Street === undefined ? undefined : str(ship.Street),
          city: ship.City === undefined ? undefined : str(ship.City),
          postalCode: ship.PostalCode === undefined ? undefined : str(ship.PostalCode),
          country: ship['@_country'],
        },
      },
      currency: lineItems['@_currency'],
      status: mapB2BStatus(po.Status),
    },
  };
}
