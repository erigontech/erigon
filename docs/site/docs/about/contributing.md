---
title: "Contributing to these docs"
description: "How to contribute code, report bugs, write docs, and submit pull requests to the Erigon project."
sidebar_position: 1
---

# Contributing to these docs

The Erigon documentation is built with [Docusaurus](https://docusaurus.io) and lives in the [erigontech/erigon](https://github.com/erigontech/erigon) repository under `docs/site/`.

## Reporting issues

Open an issue in the repository to suggest corrections, flag outdated content, or request new pages.

## Editing locally

1. Clone the repository and install dependencies:
   ```bash
   git clone https://github.com/erigontech/erigon.git
   cd erigon/docs/site
   npm install
   ```
2. Start the local dev server:
   ```bash
   npm run start
   ```
3. Open `http://localhost:3000` in your browser — changes to Markdown files hot-reload automatically.

## Submitting a pull request

1. Create a new branch from `release/3.4`.
2. Edit or add `.md` / `.mdx` files under `docs/site/docs/`.
3. Verify your changes render correctly in the local dev server.
4. Open a pull request against `release/3.4` with a clear description of what changed and why.

