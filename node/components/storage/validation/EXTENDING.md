# Extending validation

Validation is an extension point in Erigon's storage component, not a
fixed registry. To add a new structural check, write a self-contained
type implementing the `Validator` interface and compose it into a
chain at the call site. There is no enum, no central registry, and no
bridge to register against.

## The contract

```go
type Validator interface {
    Name() string
    Validate(file *snapshot.FileEntry, content ContentSource) error
}
```

Two methods. `Name` returns a stable identifier used in error wrapping
and log output. `Validate` returns nil on accept, or a structured
error on reject — the wrapped error should be operator-readable.

`ContentSource` yields the file's bytes when the validator needs them;
stage-1 validators that check only metadata (name, range, kind, size)
take `ContentSource` as a parameter but ignore it.

## Adding a validator — four steps

### 1. Pick a package

Validators may live anywhere. The choice depends on what the
validator depends on:

- **Generic / structural** (no Erigon-specific imports) — keep next to
  your component, or under `node/components/storage/validation` if it
  belongs to the default chain.
- **Format-aware** (parses `.kv` / `.idx` / `.ef` etc.) — typically
  lives in `db/integrity` alongside the existing format readers, so
  the import graph stays clean.
- **Component-specific** — live in your component's own package.

There is no required home. Validators are first-class types in
whichever package owns the concept.

### 2. Define your type

Plain struct + two methods. Field-level configuration goes in the
struct; per-call state passes through `Validate`'s arguments.

### 3. Compose into a chain

The chain is `validation.Chain` — a plain `[]Validator` slice. Build
the chain at the construction site of whichever component runs
validators (typically the storage component's lifecycle driver, via
`lifecycle.BuildOnValidation`):

```go
chain := validation.Chain{
    validation.NameNotEmpty{},
    validation.RangeOrdering{},
    validation.KindConsistencyFromName{},
    myextension.MyCheck{Config: ...},  // your new validator
}

handler := lifecycle.BuildOnValidation(chain, contentFor, inv)
driver.OnValidation = handler
```

The chain runs in order, fails fast on the first rejection, and wraps
the failing validator's `Name` around the error.

### 4. Test in isolation

Write a unit test for your validator type. Validators are simple
functions of `(file, content)`; tests compose canned `*FileEntry` +
`BytesContent` and assert `Validate` returns the expected outcome.
The validation package's existing tests
(`validation_test.go`, `builtins_disk_test.go`) are reference shapes.

## Worked example

See `node/components/storage/validation/example_extension/` for a
complete compilable extension package. It defines one validator
(`FileNameSuffixWhitelist`) demonstrating the layout, the
implementation, and chain composition. The test in that package shows
how to validate the validator itself.

## Producer-gate context

Per-file validators run inside the storage-component's lifecycle
driver, gated on `LifecycleIndexed` and advancing successful files to
`LifecycleAdvertisable`
(see `docs/plans/20260501-storage-lifecycle-spec.md`). A failing
validator halts the file's promotion; the next sweep retries (so
transient failures self-heal without operator intervention).

Cross-file batch validators (commitment chain, history/kv alignment)
are stage-2 work — they live in `BatchValidator` and run at separate
batch boundaries, not per-file. The interface and the chain shape are
analogous; see `batch.go`.

## What NOT to do

- **No `Check` enum.** Adding a value to a `Check` enum and a
  matching switch case is the bridge anti-pattern. Validators are
  typed; chain composition is by listing types.

- **No central registry.** There is no global `RegisterValidator`
  function. Each consumer composes the chain that fits their use
  case.

- **No bridge package.** A wrapper that adapts external functions to
  the `Validator` interface is structurally redundant — the
  validator implementation itself satisfies the interface natively
  in one struct + two methods.

- **No deferred-support stubs.** If your validator needs dependencies
  that aren't yet wired, write the type when those dependencies are
  available. Empty stubs that return "not yet supported" errors
  accumulate dead code and obscure what's actually runnable.

## Reference

- `validation.go` — `Validator` interface, `ContentSource`, `Chain`.
- `batch.go` — `BatchValidator` for cross-file consistency.
- `builtins.go` — built-in stage-1 validators (NameNotEmpty,
  RangeOrdering, KindConsistencyFromName, ContentNotEmpty,
  SizeMatchesTorrent).
- `example_extension/` — the worked example.
- `docs/plans/20260501-storage-lifecycle-spec.md` — where validators
  run inside the storage-owned lifecycle.
- `app-integration-review-items.md` (memory) — item #3, the working
  decision behind this extension-point design.
