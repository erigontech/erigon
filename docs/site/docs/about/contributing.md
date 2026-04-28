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

## Contributing to the Erigon codebase

The Erigon node software is developed in the same [erigontech/erigon](https://github.com/erigontech/erigon) repository. Code contributions follow the standard GitHub fork-and-PR workflow.

### Getting started

1. Fork the repository and clone your fork:
   ```bash
   git clone https://github.com/<your-username>/erigon.git
   cd erigon
   ```
2. Build the project (requires Go 1.24+ and a C++ compiler):
   ```bash
   make erigon
   ```
3. Run the test suite:
   ```bash
   go test ./...
   ```

### Opening a pull request

- Target the `main` branch for new features and non-critical fixes.
- Target the relevant `release/x.y` branch for release-specific backports.
- Keep PRs focused — one logical change per PR makes review faster.
- Include a clear description of what changed, why, and how it was tested.
- For significant changes, open an issue first to discuss the approach before writing code.

### Code style and guidelines

- Follow the existing Go conventions in the codebase.
- Run `make lint` before submitting — the CI will enforce it.
- All new code should be covered by tests where practical.

For questions, join the [Erigon Discord](https://discord.gg/e8MBWss7uJ) or open a discussion on GitHub.

