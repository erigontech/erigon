// An independent reader for docs-deploy.yml's on.push.branches, used only to
// cross-check docs-deploy-trigger.py's fixtures against a YAML implementation
// that shares no code with PyYAML.
//
// Why this exists: fixtures written from what one parser does are evidence
// about that parser, not about YAML. Four review rounds in a row found the
// earlier hand-rolled matcher disagreeing with real parsers on a shape its own
// test suite said was fine, because the suite and the parser were the same
// opinion twice.
//
//   node docs-deploy-trigger.oracle.mjs <file>
//   -> {"status":"ok","branches":[...]}    a branch list
//      {"status":"nolist","detail":...}    no on.push.branches sequence
//      {"status":"nonstring","detail":...} entries that are not strings
//      {"status":"invalid","detail":...}   not a document YAML accepts
//
// js-yaml is not a dependency of this repo. Set JS_YAML_FROM to any directory
// whose node_modules has one -- resolution is relative to it, not to NODE_PATH,
// which ES module imports ignore:
//   JS_YAML_FROM=docs/site node .../docs-deploy-trigger.oracle.mjs <file>
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';

const from = process.env.JS_YAML_FROM;
const require = createRequire(
  from ? pathToFileURL(resolve(from) + '/') : import.meta.url,
);

let yaml;
try {
  yaml = require('js-yaml');
} catch {
  console.error(
    'js-yaml not found. Set JS_YAML_FROM to a directory whose node_modules has it.',
  );
  process.exit(2);
}

const path = process.argv[2];
if (!path) {
  console.error('usage: docs-deploy-trigger.oracle.mjs <file>');
  process.exit(2);
}

// Reading is NOT inside the try. This script's whole job is to answer "does an
// unrelated parser accept this document", so an unreadable file reported as
// `invalid` would be the oracle agreeing with PyYAML for a reason that has
// nothing to do with YAML -- the same "one opinion twice" failure it exists to
// prevent. An I/O failure is an operator error and exits 2 like the others.
let text;
try {
  text = readFileSync(path, 'utf8');
} catch (e) {
  console.error(`cannot read ${path}: ${e.message}`);
  process.exit(2);
}

let doc;
try {
  doc = yaml.load(text);
} catch (e) {
  console.log(JSON.stringify({ status: 'invalid', detail: e.name }));
  process.exit(0);
}

// js-yaml reads YAML 1.2, where `on` is the string it looks like; PyYAML reads
// 1.1, where a bare `on` is boolean true. Accept either so that the comparison
// is about the branch list and not about that difference.
const on = doc && typeof doc === 'object' ? doc.on ?? doc[true] : undefined;
const push = on && typeof on === 'object' ? on.push : undefined;
const branches = push && typeof push === 'object' ? push.branches : undefined;

if (!Array.isArray(branches)) {
  console.log(JSON.stringify({
    status: 'nolist',
    detail: branches === undefined ? 'absent' : typeof branches,
  }));
} else if (!branches.every((b) => typeof b === 'string')) {
  console.log(JSON.stringify({
    status: 'nonstring',
    detail: branches.map((b) => typeof b),
  }));
} else {
  console.log(JSON.stringify({ status: 'ok', branches }));
}
