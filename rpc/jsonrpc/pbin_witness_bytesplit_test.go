package jsonrpc

// Measures what code chunking costs a binary witness, two ways: a byte split by
// node kind, and a re-prune with every code-chunk key dropped from the proved
// set so the branches that existed only to bind those chunks go too.
//
// The re-prune is not a proposal — the keys are inside the leaf hashes and the
// root would move. It sizes the cost.
//
// Two limits on what the numbers mean. Proved keys are derived from the leaves
// present in the witness, so absence proofs are undercounted: a blinded node for
// a key with no leaf is off every derived proof path and the re-prune drops it,
// which reads as a code saving on a block holding no code chunks. And the
// re-prune baseline is not the input witness, so the saved column is only a code
// figure for blocks whose code% is non-zero.

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// pbinLeafKeyOf returns the tree key of a leaf preimage, or nil for a branch.
func pbinLeafKeyOf(node []byte) []byte {
	if len(node) == 0 || node[0] != 0x00 {
		return nil
	}
	return node[1 : len(node)-32]
}

// isCodeChunkKey reports whether a tree key names a code chunk: every chunk
// lives in the code zone, content-addressed by code hash.
func isCodeChunkKey(key []byte) bool {
	return len(key) > 0 && key[0] == 0x01
}

// pbinDelegationSubIndex is the EIP's DELEGATION_LEAF_KEY, unexported by package
// commitment. It stands where CODE_HASH does for a 7702-delegated account.
const pbinDelegationSubIndex = 2

func TestPBinWitnessCodeWeight(t *testing.T) {
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)
	c := buildPBinWitnessChain(t)
	enableCommitmentHistoryFlag(t, c.m.DB)
	api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})

	t.Logf("%-6s %-38s %6s %8s %8s %8s %7s %8s %8s %7s",
		"block", "shape", "nodes", "total", "leafkey", "branch", "code%", "noCodeN", "noCodeB", "saved")

	var gTotal, gNoCode int
	for _, block := range pbinWitnessCorpus {
		result := pbinWitnessOf(t, api, block.num)
		require.NotEmpty(t, result.State)

		nodes := make([][]byte, 0, len(result.State))
		for _, n := range result.State {
			nodes = append(nodes, n)
		}
		// The RPC sorts result.State bytewise before returning, so the root-first
		// contract is gone by here; take the root from the parent header instead.
		root := c.block(t, block.num-1).Root()

		var total, leafKey, branch, codeBytes int
		var keep [][]byte
		for _, n := range nodes {
			total += len(n)
			key := pbinLeafKeyOf(n)
			if key == nil {
				branch += len(n)
				continue
			}
			leafKey += len(key)
			if isCodeChunkKey(key) {
				codeBytes += len(n)
				continue
			}
			keep = append(keep, key)
		}

		lean, err := commitment.PBinWitnessNodesForKeys(nodes, root[:], keep)
		require.NoError(t, err, "block %d re-prune", block.num)
		noCodeBytes := 0
		for _, n := range lean {
			noCodeBytes += len(n)
		}

		gTotal += total
		gNoCode += noCodeBytes
		saved := 0.0
		if total > 0 {
			saved = 100 * float64(total-noCodeBytes) / float64(total)
		}
		t.Logf("%-6d %-38s %6d %8d %8d %8d %6.1f%% %8d %8d %6.1f%%",
			block.num, block.shape, len(nodes), total, leafKey, branch,
			100*float64(codeBytes)/float64(total), len(lean), noCodeBytes, saved)
	}
	t.Logf("corpus: %d B with code, %d B without (%.1f%% is code chunking)",
		gTotal, gNoCode, 100*float64(gTotal-gNoCode)/float64(gTotal))
}
