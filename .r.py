import re, subprocess
def edit(p, pairs):
    s = open(p).read()
    for a, b in pairs:
        assert s.count(a) == 1, (p, a[:70])
        s = s.replace(a, b)
    open(p, "w").write(s)
edit("cmd/utils/flags.go", [(
"""func warnParallelCommitmentDeprecated(ctx *cli.Command, logger log.Logger) {
	if ctx.IsSet(ExperimentalParallelCommitmentFlag.Name) {
		logger.Warn("[DEPRECATED] --experimental.parallel-commitment has no effect; commitment always runs on the parallel trie")
	}
}""",
"""func warnParallelCommitmentDeprecated(ctx *cli.Command, logger log.Logger) {
	if ctx.IsSet(ExperimentalParallelCommitmentFlag.Name) {
		logger.Warn("[DEPRECATED] --experimental.parallel-commitment has no effect; commitment always runs on the parallel trie")
	}
	for _, name := range []string{"COMMITMENT_PARALLEL", dbg.ErigonEnvPrefix + "COMMITMENT_PARALLEL"} {
		if _, ok := os.LookupEnv(name); ok {
			logger.Warn("[DEPRECATED] " + name + " has no effect; commitment always runs on the parallel trie")
		}
	}
}""")])
# 1. inline PickTrieVariant
edit("db/state/execctx/domain_shared.go", [
("""// PickTrieVariant returns the commitment trie variant for a commitment.TrieConfig built inline.
func PickTrieVariant() commitment.TrieVariant {
	return commitment.VariantParallelHexPatricia
}

""", ""),
("""	o.trieCfg.Variant = PickTrieVariant()""", """	o.trieCfg.Variant = commitment.VariantParallelHexPatricia"""),
])
for p in ["db/state/squeeze.go", "execution/stagedsync/exec3_parallel_robustness_test.go", "db/test/domain_shared_bench_test.go", "db/state/aggregator_bench_test.go"]:
    s = open(p).read()
    assert "execctx.PickTrieVariant()" in s, p
    s = s.replace("execctx.PickTrieVariant()", "commitment.VariantParallelHexPatricia")
    open(p, "w").write(s)
# 3. drop the wrapper and its test
edit("node/node.go", [
("""func parallelCommitmentReaders() int {
	return commitment.ParallelCommitmentReadTxs()
}

""", ""),
("""			parallelCommitmentReaders(),""", """			commitment.ParallelCommitmentReadTxs(),"""),
])
edit("node/node_db_test.go", [(
"""
func TestParallelCommitmentReaders(t *testing.T) {
	require.Equal(t, commitment.ParallelCommitmentReadTxs(), parallelCommitmentReaders())
}
""", "")])
edit("ChangeLog.md", [(
"""### Changed

""",
"""### Changed

- db/state: commitment always runs on the parallel trie. `--experimental.parallel-commitment` and the `COMMITMENT_PARALLEL` environment variable no longer select the sequential trie: `=false`, documented as the way back to sequential commitment, is now ignored with a deprecation warning at startup (#26336) — by @AskAlexSharov
""")])
print("ok")
