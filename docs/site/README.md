# Erigon Documentation Site

Built with [Docusaurus 3](https://docusaurus.io/). Deployed automatically to [docs.erigon.tech](https://docs.erigon.tech) via GitHub Actions on push to `release/3.5`.

## Local Development

All commands run from `docs/site/`:

```bash
npm install       # Install dependencies
npm run start     # Dev server → http://localhost:3000 (hot reload)
npm run build     # Production build → build/
npm run serve     # Serve the production build locally
npm run typecheck # TypeScript check without emit
```

## Deployment

Deployment is handled automatically by `.github/workflows/docs-deploy.yml` on every push to `release/3.5`. Do not use manual `yarn deploy` — it is not configured for this site.

## Disk size data flow

The "Current Disk Usage" figures on the hardware-requirements page are **static
text** held between MDX-comment markers, e.g.:

```mdx
| Archive | {/* ds:mainnet:archive */}1.77 TB{/* ds:end */} | 4 TB | ... |
_... measured as of {/* ds-date:mainnet */}2025-09-01{/* ds:end */}; ..._
```

- `src/data/disk-sizes.json` is the machine-readable source of record. CI
  (`.github/workflows/update-disk-sizes.yml`) writes `full`/`minimal` rows from
  sync-from-scratch artifacts via `scripts/update-disk-sizes.py`; `archive` rows
  are entered manually.
- `scripts/render-disk-sizes.py` projects that JSON onto the page's markers. CI
  runs it after updating the JSON and commits the page; `--check` (run in
  `docs-site-build.yml`) fails the build if the two ever drift.

**Why static and not a runtime `import diskSizes from '@site/...'`?** A runtime
import is shared across all doc versions, so a `docusaurus docs:version` snapshot
would silently keep showing the *current* version's numbers instead of the ones
measured for that release. Static markers make each version snapshot correct by
construction, and remove a silent `?? '—'` fallback. `docs-site-build.yml` guards
this: it fails if any page under `docs/` or `versioned_docs/` references
`diskSizes` or imports `disk-sizes.json`.

## Cutting a new docs version

When a new Erigon minor release ships (e.g. cutting `v3.5` because trunk moves to
`v3.6`), snapshot the current docs into a frozen version:

```bash
# from docs/site/
npm run docusaurus docs:version v3.5   # creates versioned_docs/version-v3.5/
npm run build                          # verify
```

No extra step is needed for disk sizes: because the current page is already
static (see above), the snapshot freezes the correct values automatically.
