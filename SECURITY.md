# Security Policy

## Reporting a vulnerability

**Please don't open a public issue for a security vulnerability** — a public report reaches attackers
before node operators have a fix to upgrade to.

Report it privately through
[Security → Report a vulnerability](https://github.com/erigontech/erigon/security/advisories/new).
That route reaches the maintainers directly and gives us a private space to prepare a fix with you.
It needs only a GitHub account, and nothing becomes public until we publish an advisory together.

Please include what an attacker can achieve with the issue, the Erigon version you're running, the
chain and any flags relevant to it, steps to reproduce, and any disclosure deadline you're working
to.

We aim to acknowledge a report within three business days and will keep you posted while we work on
it. We'll credit you in the advisory unless you'd rather stay anonymous, and we ask that you give us
a reasonable window to ship a fix before disclosing publicly. Erigon doesn't run a bug bounty
programme, so reports aren't paid.

## Supported versions

Security fixes land on `main` and in the most recent release series; earlier series aren't patched,
so the fix arrives by upgrading. See [Releases](https://github.com/erigontech/erigon/releases) for
the current series.

For anything that isn't a security issue, open a normal
[issue](https://github.com/erigontech/erigon/issues) instead.
