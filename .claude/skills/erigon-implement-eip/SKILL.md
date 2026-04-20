---
name: erigon-implement-eip
description: Implement a new EIP for a hardfork under development in Erigon. Use when the user asks to implement, port, or wire up an EIP — covers spec lookup, dep analysis, prior-work check, implementation, lint, tests, and a wrap-up saved to `agentspecs/`.
argument-hint: "<fork> <eip-number>"
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, WebFetch, Skill
---

# Implement EIP-$1 in the $0 fork

## Step 1 — Locate and understand the EIP specification

Fetch the specification from:

```
https://eips.ethereum.org/EIPS/eip-$1
```

Read the spec end-to-end and make sure you understand it. If there are any ambiguities or things that do not make sense in the EIP, bring these up and ask for clarifications. Once understood, map the EIP to the codebase — identify which packages, files, and existing constructs the EIP will touch.

## Step 2 — Find dependent and referenced EIPs

Identify any other EIPs that EIP-$1 depends on or references. Each such EIP can be fetched using the same URL pattern as in Step 1, substituting its number:

```
https://eips.ethereum.org/EIPS/eip-<number>
```

Try to understand these additional EIPs and map them to the code. If there are any ambiguities or contradictions with the new EIP, make sure to raise these and ask questions.

## Step 3 — Identify the hardforks of referenced / interplaying EIPs

Identify which hardforks contain the referenced EIPs, as well as any other EIPs already in the codebase that interplay with the new one. This provides more context and knowledge when analysing the code.

A list of forks and the EIPs they contain can be found at:

```
https://eips.ethereum.org/meta
```

They are organised as "meta EIPs" and usually start with `Hardfork Meta` followed by the name of the hardfork.

### Fork naming

More recent forks use portmanteau naming to combine the execution-layer (EL) fork name and the consensus-layer (CL) fork name. EL forks are named after cities; CL forks are named after stars. For example, **Glamsterdam** is a portmanteau of **Gloas** (CL) and **Amsterdam** (EL).

### EL vs CL scope

Some EIPs only affect the EL, others only the CL, and some affect both. Erigon has both an EL implementation (under the `execution` package) and a CL implementation (under the `cl` package), so all EIPs apply to one or both layers in Erigon.

### Reading a meta EIP

In some cases fork meta EIPs list which EIPs got included; in other cases they provide a summary of the changes instead of an EIP list.

The meta EIP for the **current fork under development** usually has these lists:
- **CFI** — EIPs Considered For Inclusion
- **SFI** — EIPs Scheduled For Inclusion
- **PFI** — EIPs Proposed For Inclusion
- **DFI** — EIPs Declined For Inclusion

## Step 4 — Reference the Ethereum Yellow Paper

Reference the Ethereum Yellow Paper for deeper information about the Ethereum protocol. It can be found at:

```
https://ethereum.github.io/yellowpaper/paper.pdf
```

Note however that the current version of it covers up to and including the Shanghai hard fork. Changes to the protocol after Shanghai can be inferred via the meta EIPs for the subsequent forks after Shanghai.

The yellow paper provides deep base knowledge that can be used to confirm correctness up to Shanghai, and later for parts that haven't been changed by subsequent hard forks. For parts that have been changed in subsequent hard forks, the corresponding EIP specs from those hardforks — read in chronological order — become the main source of truth.

## Step 5 — Check for previous work on this EIP

Check whether any previous work for this EIP has already been done in the codebase. This may happen if we've previously implemented this EIP but against an older / outdated spec for a previous devnet.

In this case, analyse the current code and compare it to the latest specs. Implement the necessary changes to address any discrepancies that you find.

## Step 6 — Implement the change

Implement the EIP changes in the code, based on the understanding gathered in Steps 1–5 and the codebase mapping you produced. Touch the packages, files, and existing constructs identified during that mapping.

If during implementation you discover something that is unclear or appears to contradict the spec, stop and ask for clarification rather than guessing.

After making code changes, run `make lint` and fix any reported issues. The linter is non-deterministic — run it repeatedly until it is clean.

## Step 7 — Run local tests

Run local tests using the `/erigon-test-all` skill. Analyse and fix any failures **in the implementation** (see also "Question the tests — do not silently fix them" below). Keep iterating until all tests are fixed.

### Which EL tests matter most

The most important tests when implementing a new EIP for the EL are the tests under the `execution/tests` package. More specifically:

- `TestExecutionSpecBlockchainDevnet` — covers the current devnet tests for the hardfork under development.
- All the other `TestExecutionSpecBlockchain*` variants — cover stable hard forks (the main `TestExecutionSpecBlockchain` plus per-fork specialised variants like `…CancunBlobs`, `…PragueCalldata`, `…OsakaCLZ`, `…FrontierScenarios`).

### Where the test fixtures come from

These protocol tests are defined by JSON test fixture files sourced from the `execution/tests/execution-spec-tests` directory. It is sourced as a git submodule which the `/erigon-test-all` skill knows how to keep in sync.

These test fixture files are generated by the Ethereum Python spec reference implementation, which can be found at:

```
https://github.com/ethereum/execution-specs
```

The code version which generates the test fixtures for a given hard fork lives on a branch following the `forks/<fork_name>` naming convention. For example, if the fork name is `osaka` then the branch name will be `forks/osaka`. The branch for the user-provided hard fork under development is `forks/$0`.

Sometimes new EIPs under development may not be in the `forks/$0` branch yet — they may be on a separate feature branch. To find it, look for a branch name that contains the provided EIP number `$1`. Once you find a branch (or several branches), ask the user to confirm which one to use.

The point of identifying the correct branch is to **read and analyse the right reference code** — i.e. the version of the Python spec implementation that corresponds to the fixtures we are testing against. Nothing in the local repo needs to be updated; this is purely about which code version the agent downloads / reads for cross-referencing while debugging or validating behaviour.

### Question the tests — do not silently fix them

It is **VERY IMPORTANT** to question the tests and validate they are doing the correct thing as per the EIP specifications. This applies to both EL spec tests (driven by `execution/tests/execution-spec-tests`) and CL spec tests (driven by `cl/spectest` — see "Running CL spec tests" below).

Sometimes some of the protocol spec tests may have bugs or mistakes in them due to inaccurate implementation in the upstream reference implementations (the Python `execution-specs` for the EL, the `consensus-spec-tests` for the CL).

Or some of our other local tests (outside of the ones driven by `execution/tests/execution-spec-tests`, `execution/tests/legacy-tests`, or the `cl/spectest` fixtures) may have gaps in them, test for the wrong thing, or test for logic from before the new EIP — in which case the test should be amended to be explicitly for the correct corresponding previous hardfork, and a new test should be written for the new hardfork which this EIP will go into, covering both the old logic (prior to this EIP) and the new logic (after it).

When you run into such situations **do not attempt to fix those tests**. Instead write a summary of your findings and ask for them to be reviewed and new guidance to be given.

### Running CL spec tests

If the EIP affects the CL (or has any CL-side touchpoints), also run the consensus-spec tests under `cl/spectest`. These mirror what the `Consensus spec` GitHub workflow (`.github/workflows/test-integration-caplin.yml`) runs.

The flow is:

```
cd cl/spectest
make tests        # downloads the consensus-spec-tests fixture tarball and unpacks it
make mainnet      # runs the full mainnet CL spec test suite
```

For faster iteration when only one fork is relevant, the `cl/spectest/Makefile` exposes per-fork targets such as `make capella`, `make deneb`, `make electra`, `make fulu`, etc. Use the one matching the fork the EIP belongs to. If no per-fork target exists yet for the new fork, add one or run `CGO_CFLAGS=-D__BLST_PORTABLE__ go test -run=/mainnet/<fork>/ -v --timeout 30m` from `cl/spectest` (the `CGO_CFLAGS` flag mirrors the Makefile and is required for the BLS portable build).

If `make tests` fails (e.g. fixture download issue), the CI workflow treats it as a soft skip — but locally you should investigate and fix it before relying on the CL test results.

## Step 8 — Wrap up

Once implementation is complete and tests are passing, produce a wrap-up summary of all the work done. The summary should cover:

- The EIP being implemented and the fork it targets
- All packages, files, and constructs that were touched
- Key design decisions and any deviations from the spec (with justification)
- Dependent / referenced EIPs that influenced the implementation
- Any outstanding questions, unresolved ambiguities, or test discrepancies that were flagged for review
- Test coverage status — which `TestExecutionSpecBlockchain*` tests now pass (and CL `cl/spectest` results, if applicable), and any that were skipped or remain open

Include the summary in your final reply, and also save it as a markdown file at:

```
agentspecs/eip-$1-implementation/summary.md
```

Create the `agentspecs/eip-$1-implementation/` directory if it does not already exist. The `agentspecs/` directory is gitignored, so these notes stay local.
