import fs from 'node:fs';
import Ajv from 'ajv';
import Ajv2019 from 'ajv/dist/2019.js';
import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';

/** Compile the canonical order schema (read-only mount at /canonical). */
export function loadOrderValidator(file = '/canonical/order.schema.json') {
  const schema = JSON.parse(fs.readFileSync(file, 'utf8'));
  const dialect = String(schema.$schema || '');
  const Cls = dialect.includes('2020-12') ? Ajv2020 : dialect.includes('2019-09') ? Ajv2019 : Ajv;
  try {
    const ajv = new Cls({ allErrors: true, strict: false });
    addFormats(ajv);
    return ajv.compile(schema);
  } catch {
    const copy = { ...schema };
    delete copy.$schema;
    const ajv = new Ajv({ allErrors: true, strict: false });
    addFormats(ajv);
    return ajv.compile(copy);
  }
}
