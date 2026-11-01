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
instructions.go  execute funcs of the fast ops      ─┤
eips.go          execute funcs of EIP ops (PUSH0..) ─┼─> vm_run_gen.go          run: runTraced with anyTrace=false and the fast-path switches
vmgen/main.go    fastOps(), coldOps(): inlined ops  ─┘   fast_path_gen_test.go  table of the inlined ops for the tests
```

## Adding an op to the fast path

1. Add a hot op to `fastOps()` in `main.go`, and a cheap or fork-gated one to `coldOps()`: its switch runs after the `fastOps()` one, so the hot switch stays shallow. An op qualifies if it charges constant gas (plus copy gas per word for copies) and needs no check beyond its stack and gas checks. An error from its execute func, such as a bad JUMP or JUMPI destination, leaves the loop; jumps set `jump`. A memory op (MLOAD, MSTORE, MCOPY) is inlined only while memory does not grow: set `memorySize`, `dynamicGas` and `allocated`; when memory must grow, the op takes the jump-table path. A copy op also sets `copyLen`, or its case charges only the constant gas. An op that earlier forks lack sets `fork` (`forked(...)` in `coldOps()`), so its case checks that the fork's jump table has it; a memory op puts that check at the start of `allocated` instead, as MCOPY does with `jt[MCOPY].memorySize != nil`.
2. Run `make evm-interpreter-gen`.
3. Run `go test ./execution/vm/`. `TestFastPathMatchesJumpTables` checks the gas and stack numbers against every fork; `TestRunMatchesRunTraced` checks the inlined body against the jump-table op.
