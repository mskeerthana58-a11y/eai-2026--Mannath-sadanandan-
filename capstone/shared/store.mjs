import fs from 'node:fs';
import path from 'node:path';

/** A JSON file written atomically: temp file in the same folder, then rename (R-3.5.3). */
export class JsonStore {
  constructor(file, initial) {
    this.file = file;
    this.data = initial;
    this.chain = Promise.resolve();
  }

  load() {
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    try {
      this.data = JSON.parse(fs.readFileSync(this.file, 'utf8'));
    } catch (e) {
      if (e.code !== 'ENOENT') console.error(`[store] cannot read ${this.file}: ${e.message}; starting empty`);
    }
    return this.data;
  }

  save() {
    const snapshot = JSON.stringify(this.data);
    this.chain = this.chain
      .then(() => {
        const tmp = `${this.file}.tmp`;
        fs.writeFileSync(tmp, snapshot);
        fs.renameSync(tmp, this.file);
      })
      .catch((e) => console.error(`[store] cannot write ${this.file}: ${e.message}`));
    return this.chain;
  }
}
