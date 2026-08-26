# EIP-8038 BAL-started commitment warmup

## Status

Implementation complete; pristine-versus-candidate benchmarks pending.

## Scope

The candidate worktree is based directly on local `main` at
`21790b44df304a8503cf06b5979a8e34ae45fe83`. It contains no MDBX readahead,
code-prefetch, recsplit, page-population, or late commitment-warmup experiment.

The change starts commitment-branch warmup from the block access list while
the existing BAL state warmup is running. It is independent of
`ParallelPatriciaHashed.Process` and uses its own read-only MDBX transactions.

Only changed account and storage keys enter the commitment warmup. Read-only
BAL entries are excluded. Keys are hash-nibbled and sorted so adjacent keys can
reuse their common prefix depth.

`PARALLEL_TRIE_BAL_WARMUPERS` controls the separate pool. Its default is
`GOMAXPROCS`, which is six in the benchmark container. Zero disables the pool.

The MDBX read-transaction floor now accounts separately for:

- parallel execution workers;
- mounted commitment workers;
- BAL commitment-warmup workers;
- block read-ahead workers;
- permanent readers and reserved headroom.

For the benchmark configuration, the floor census is:

```text
6 execution + 6 mounted + 6 commitment warmup + 6 BAL state warmup
  + 5 permanent + 2 read-ahead + 16 reserve = 47
```

## TDD and validation

The Red phase failed because the BAL key selector, commitment warmup, worker
default, and separate worker-pool accounting did not exist. The Green phase
passes:

- focused BAL commitment-warmup tests;
- affected package tests for `common/dbg`, `execution/exec`, `httpcfg`,
  `cmd/utils`, and `node`;
- the focused race-detector run;
- two isolated `make lint` runs;
- `make erigon integration`.

The first unisolated lint invocation read cached paths from the sibling
`chaindata-readahead` worktree. Running with a worktree-specific
`GOLANGCI_LINT_CACHE` produced two clean runs.

## Benchmark protocol

Both benchmark phases compare the same commits:

- pristine: local `main` at `21790b44df304a8503cf06b5979a8e34ae45fe83`;
- candidate: the latest committed revision of this worktree.

Both binaries are built with the same toolchain and image base. Both retain
the pristine MDBX chaindata read-ahead behavior. No benchmark-only source
change is allowed.

Phase one reruns the exact 299,818,800-gas `ACCOUNT_WRITE NOCODE` fixture with
fresh OverlayFS uppers and cold OS page cache. Phase two applies the same A/B
to every stateful-suite fixture at the 300M gas target.
