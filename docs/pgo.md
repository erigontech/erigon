# Profile-guided optimization (PGO)

`cmd/erigon/default.pgo` is a CPU profile that `go build` applies automatically
(`-pgo=auto`, the Go default) when building the `erigon` binary. No Makefile
flag is needed. The profile only steers inlining and devirtualization, so a
stale profile degrades to "no effect" rather than making the binary slower.

Cost: a build that does not hit the Go build cache takes about twice as long
(89 s to 174 s on a 20-core box). Once a profile has been compiled against,
later builds with the same profile use the cache as usual.

Disable it for a build with:

```sh
make erigon GO_FLAGS=-pgo=off
```

## Regenerating the profile

Collect from a real workload. The committed profile is a merge of samples taken
from a mainnet node at chaintip and from a block replay (`integration stage_exec`).

1. Run a synced node with the pprof server enabled:

   ```sh
   erigon ... --pprof --pprof.addr=127.0.0.1 --pprof.port=6062
   ```

2. Take a few 5-minute CPU samples while the node is in the state you want to
   optimise for (at tip, or replaying blocks):

   ```sh
   for i in 1 2 3; do
     curl -s "http://127.0.0.1:6062/debug/pprof/profile?seconds=300" -o cpu_$i.pprof
   done
   ```

3. Merge the samples (add profiles from other workloads to the same command)
   and replace the committed file:

   ```sh
   go tool pprof -proto cpu_*.pprof > cmd/erigon/default.pgo
   ```

4. Rebuild and confirm the profile was used:

   ```sh
   make erigon && go version -m build/bin/erigon | grep pgo
   ```

Refresh the profile after large refactors of hot paths, or roughly once per
release; the compiler matches profile entries by function name, so renamed
functions simply stop benefiting until the profile is regenerated.
