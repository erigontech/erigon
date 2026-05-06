# Erigon Documentation Site

Built with [Docusaurus 3](https://docusaurus.io/). Deployed automatically to [docs.erigon.tech](https://docs.erigon.tech) via GitHub Actions on push to `release/3.4`.

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

Deployment to [docs.erigon.tech](https://docs.erigon.tech) is handled automatically via GitHub Actions on every push to `release/3.4`. Do not use manual `yarn deploy` — it is not configured for this site.
