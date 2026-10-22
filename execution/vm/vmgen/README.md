# vmgen

Generates the EVM interpreter's untraced loop. Do not edit the outputs by hand.

## Run

```
make evm-interpreter-gen          # same as: go generate -run "go run ./vmgen" ./execution/vm/
cd execution/vm && go run ./vmgen -check   # exit 1 and list stale files, write nothing
```

`TestRunIsGenerated` fails when the outputs are stale.

## Inputs → outputs

```
interpreter.go   runTraced (the loop with tracing)  ─┐
instructions.go  execute funcs of the fast ops      ─┼─> vm_run_gen.go          run: runTraced with anyTrace=false and the fast-path switch
vmgen/main.go    fastOps(): which ops are inlined   ─┘   fast_path_gen_test.go  table of the inlined ops for the tests
```

## Adding an op to the fast path

1. Add it to `fastOps()` in `main.go`. An op qualifies only if it charges constant gas (plus copy gas per word for copies) and cannot fail once its stack and gas checks pass.
2. Run `make evm-interpreter-gen`.
3. Run `go test ./execution/vm/`. `TestFastPathMatchesJumpTables` checks the gas and stack numbers against every fork; `TestRunMatchesRunTraced` checks the inlined body against the jump-table op.
