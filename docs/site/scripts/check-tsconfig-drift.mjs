// Guards the inlined tsconfig against upstream drift.
//
// docs/site/tsconfig.json inlines @docusaurus/tsconfig's compilerOptions instead
// of extending them, because the upstream config sets `baseUrl` and TypeScript 7
// removed that option (an inherited option can't be unset by the extending file).
// The cost of inlining is that upstream changes no longer reach us. This script
// pays that cost down: it fails if upstream's options stop matching our copy,
// modulo the deltas declared below.
//
// Run: npm run check:tsconfig

// Deliberately depends on nothing but Node: a guard must not break when the
// thing it guards changes. The first version imported `typescript` to parse
// JSONC and died under TS 7 (`ts.parseConfigFileTextToJson is not a function` —
// the native port does not expose the old namespace API), taking CI with it.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const siteDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const OURS = path.join(siteDir, 'tsconfig.json');
const UPSTREAM = path.join(siteDir, 'node_modules/@docusaurus/tsconfig/tsconfig.json');

// Deltas we intend to carry. Keep this list minimal and justified.
const DROPPED = new Set(['baseUrl']); // removed in TS 7; see tsconfig.json comment
const OURS_ONLY = new Set(['strict']); // this site's own choice, predates inlining

// Minimal JSONC -> JSON. String-aware, so it does not maul the `//` inside the
// "$schema": "https://..." value, and it drops trailing commas the way tsc does.
function stripJsonc(text) {
  let out = '';
  let inString = false;
  let escaped = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    const next = text[i + 1];

    if (inLineComment) {
      if (c === '\n') { inLineComment = false; out += c; }
      continue;
    }
    if (inBlockComment) {
      if (c === '*' && next === '/') { inBlockComment = false; i++; }
      continue;
    }
    if (inString) {
      out += c;
      if (escaped) escaped = false;
      else if (c === '\\') escaped = true;
      else if (c === '"') inString = false;
      continue;
    }
    if (c === '"') { inString = true; out += c; continue; }
    if (c === '/' && next === '/') { inLineComment = true; i++; continue; }
    if (c === '/' && next === '*') { inBlockComment = true; i++; continue; }

    // Trailing comma: rewind past whitespace and drop it before } or ].
    if (c === '}' || c === ']') {
      const trimmed = out.replace(/\s+$/, '');
      if (trimmed.endsWith(',')) out = trimmed.slice(0, -1);
    }
    out += c;
  }
  return out;
}

// Order-insensitive comparison key. Object key order carries no meaning in
// tsconfig, so upstream reordering must not read as drift. Array order IS
// meaningful — `paths` entries are an ordered fallback list — so arrays keep
// their order and a genuine reordering there still reports.
function canon(v) {
  const norm = (x) =>
    Array.isArray(x)
      ? x.map(norm)
      : x && typeof x === 'object'
        ? Object.fromEntries(Object.keys(x).sort().map((k) => [k, norm(x[k])]))
        : x;
  return JSON.stringify(norm(v));
}

function readOptions(file) {
  if (!fs.existsSync(file)) {
    console.error(`check-tsconfig-drift: missing ${file}\nRun \`npm ci\` first.`);
    process.exit(2);
  }
  const raw = fs.readFileSync(file, 'utf8');
  try {
    return JSON.parse(stripJsonc(raw)).compilerOptions ?? {};
  } catch (err) {
    console.error(`check-tsconfig-drift: cannot parse ${file}: ${err.message}`);
    process.exit(2);
  }
}

const ours = readOptions(OURS);
const upstream = readOptions(UPSTREAM);

const problems = [];
const seen = new Set([...Object.keys(ours), ...Object.keys(upstream)]);

for (const key of [...seen].sort()) {
  const inOurs = Object.hasOwn(ours, key);
  const inUpstream = Object.hasOwn(upstream, key);

  if (inUpstream && DROPPED.has(key)) {
    if (inOurs) problems.push(`  ${key}: expected to be dropped from our copy, but it is present`);
    continue; // upstream still ships it — that's the status quo this file documents
  }
  if (inOurs && OURS_ONLY.has(key)) continue;

  if (inUpstream && !inOurs) {
    problems.push(`  ${key}: added upstream (${JSON.stringify(upstream[key])}) — missing from our copy`);
  } else if (inOurs && !inUpstream) {
    problems.push(`  ${key}: removed upstream — still in our copy (${JSON.stringify(ours[key])})`);
  } else if (canon(ours[key]) !== canon(upstream[key])) {
    problems.push(`  ${key}: upstream ${JSON.stringify(upstream[key])} != ours ${JSON.stringify(ours[key])}`);
  }
}

// If upstream ever drops baseUrl, the whole workaround can go away.
if (!Object.hasOwn(upstream, 'baseUrl')) {
  console.log(
    'check-tsconfig-drift: @docusaurus/tsconfig no longer sets baseUrl.\n' +
    'The inlining workaround is obsolete — restore `"extends": "@docusaurus/tsconfig"`\n' +
    'in docs/site/tsconfig.json (keeping "strict": true) and delete this script.',
  );
}

if (problems.length > 0) {
  console.error(
    'check-tsconfig-drift: docs/site/tsconfig.json has drifted from @docusaurus/tsconfig:\n' +
    problems.join('\n') +
    '\n\nReconcile the inline copy against node_modules/@docusaurus/tsconfig/tsconfig.json,\n' +
    'or update the DROPPED / OURS_ONLY sets in this script if the delta is intentional.',
  );
  process.exit(1);
}

console.log('check-tsconfig-drift: inline tsconfig matches @docusaurus/tsconfig (modulo declared deltas).');
