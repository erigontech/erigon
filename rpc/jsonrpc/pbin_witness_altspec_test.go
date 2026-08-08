package jsonrpc

// What a binary witness would weigh if code were not committed as chunk leaves:
// never chunk; every contract ships as a blob, as hex does.
//
// The variant is measured, not modelled: the chunk keys are dropped from the
// proved set and the real pruner re-runs, so the branches that existed only to
// bind those chunks go too. The blob term is the contract's own bytecode, the
// same bytes hex carries in Codes.
//
// The variant is not a proposal and its root differs from the spec's — this
// prices the choice, it does not implement it.

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

type pbinAltRow struct {
	name, hex string
	size      int
	chunks    int

	hexProof, hexCode, hexTotal int
	hexNodes                    int

	binNodes, binTotal   int
	leanNodes, leanProof int // re-pruned, code-chunk keys dropped
}

// blob is what the contract's bytecode weighs when a witness ships it whole.
func (r pbinAltRow) blob() int { return r.size }

// noChunk drops chunking outright.
func (r pbinAltRow) noChunk() int { return r.leanProof + r.blob() }

func TestPBinWitnessNoCodeZone(t *testing.T) {
	withCommitmentHistory(t)
	n := len(pbinGranCases)
	rows := make([]pbinAltRow, n)
	for i := range rows {
		rows[i].name = pbinGranCases[i].name
		rows[i].size = pbinGranCases[i].size
		rows[i].chunks = pbinGranCases[i].chunks
	}

	t.Run("hex", func(t *testing.T) {
		c, _ := pbinGranChain(t)
		enableCommitmentHistoryFlag(t, c.m.DB)
		api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})
		for i := range rows {
			w := pbinWitnessOf(t, api, uint64(n+i+1))
			r := &rows[i]
			r.hexProof = sumBytes(w.State) + sumBytes(w.Headers)
			r.hexCode = sumBytes(w.Codes)
			r.hexTotal = r.hexProof + r.hexCode
			r.hexNodes = len(w.State)
		}
	})

	t.Run("bin", func(t *testing.T) {
		withBinCommitmentDatadir(t)
		c, _ := pbinGranChain(t)
		enableCommitmentHistoryFlag(t, c.m.DB)
		api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})
		for i := range rows {
			num := uint64(n + i + 1)
			w := pbinWitnessOf(t, api, num)
			r := &rows[i]

			nodes := make([][]byte, 0, len(w.State))
			keep := make([][]byte, 0, len(w.State))
			for _, node := range w.State {
				nodes = append(nodes, node)
				r.binNodes++
				r.binTotal += len(node)
				key := pbinLeafKeyOf(node)
				if key != nil && !isCodeChunkKey(key) {
					keep = append(keep, key)
				}
			}
			r.binTotal += sumBytes(w.Headers)

			// The RPC sorts result.State, so take the root from the parent header.
			root := c.block(t, num-1).Root()
			lean, err := commitment.PBinWitnessNodesForKeys(nodes, root[:], keep)
			require.NoError(t, err, "%s re-prune", r.name)
			r.leanNodes = len(lean)
			r.leanProof = sumBytes(w.Headers)
			for _, node := range lean {
				r.leanProof += len(node)
			}
			// What the "blob" column costs is only meaningful if dropping the
			// chunk keys really drops nodes: the code zone has to be a separable
			// part of the witness, not entangled with the account's proof.
			require.Less(t, r.leanNodes, r.binNodes, "%s: re-pruning kept every node", r.name)
			require.NotZero(t, r.leanNodes, "%s: re-pruning kept nothing", r.name)
		}
	})

	t.Log("witness bytes for a call executing 8 bytes, by how code is committed\n" + pbinAltTable(rows))
}

// TestPBinWitnessPartialChunks prices chunking's own premise. A blob costs the
// bytecode once; a chunk leaf plus the branch binding it costs ~4.35x the 31
// bytes it carries, so chunking only wins when a witness can prove a fraction
// of the contract rather than all of it. This sweeps that fraction against the
// real pruner and reports where the two meet.
func TestPBinWitnessPartialChunks(t *testing.T) {
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)
	n := len(pbinGranCases)
	c, _ := pbinGranChain(t)
	enableCommitmentHistoryFlag(t, c.m.DB)
	api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})

	out := fmt.Sprintf("%-16s %6s %8s %6s | %9s %8s | %8s %9s\n",
		"case", "chunks", "pattern", "proved", "witness B", "vs blob", "blob B", "break-even")
	for i, gc := range pbinGranCases {
		if gc.chunks < 32 || gc.zeroPad {
			continue
		}
		w := pbinWitnessOf(t, api, uint64(n+i+1))
		nodes := make([][]byte, 0, len(w.State))
		var base, chunkKeys [][]byte
		for _, node := range w.State {
			nodes = append(nodes, node)
			key := pbinLeafKeyOf(node)
			if key == nil {
				continue
			}
			if isCodeChunkKey(key) {
				chunkKeys = append(chunkKeys, key)
				continue
			}
			base = append(base, key)
		}
		require.Equal(t, gc.chunks, len(chunkKeys), "%s chunk leaves", gc.name)
		slices.SortFunc(chunkKeys, bytes.Compare)

		root := c.block(t, uint64(n+i)).Root()
		size := func(keep [][]byte) int {
			lean, err := commitment.PBinWitnessNodesForKeys(nodes, root[:], keep)
			require.NoError(t, err)
			return sumBytes(w.Headers) + func() int {
				total := 0
				for _, node := range lean {
					total += len(node)
				}
				return total
			}()
		}
		blob := size(base) + gc.size

		for _, pattern := range []string{"adjacent", "scattered"} {
			var prev int
			for _, proved := range []int{1, 8, 32, 64, 128, 256, gc.chunks} {
				if proved > gc.chunks || proved == prev {
					continue
				}
				prev = proved
				keep := slices.Clone(base)
				for j := range proved {
					idx := j
					if pattern == "scattered" {
						idx = j * gc.chunks / proved
					}
					keep = append(keep, chunkKeys[idx])
				}
				got := size(keep)
				mark := ""
				if got > blob {
					mark = " (dearer than a blob)"
				}
				out += fmt.Sprintf("%-16s %6d %8s %6d | %9d %7.2fx | %8d%s\n",
					gc.name, gc.chunks, pattern, proved, got, float64(got)/float64(blob), blob, mark)
			}
		}
	}
	t.Log("witness bytes when only part of a contract's chunks are proved\n" + out)
}

func pbinAltTable(rows []pbinAltRow) string {
	s := fmt.Sprintf("%-16s %7s %6s | %8s | %9s %7s | %9s %7s\n",
		"case", "code B", "chunks", "hex tot",
		"spec", "/hex", "blob", "/hex")
	for _, r := range rows {
		ratio := func(v int) float64 { return float64(v) / float64(r.hexTotal) }
		s += fmt.Sprintf("%-16s %7d %6d | %8d | %9d %6.2fx | %9d %6.2fx\n",
			r.name, r.size, r.chunks, r.hexTotal,
			r.binTotal, ratio(r.binTotal),
			r.noChunk(), ratio(r.noChunk()))
	}
	s += "\nproof bytes alone, code blob excluded from both sides:\n"
	s += fmt.Sprintf("%-16s %9s %8s | %9s %8s %7s | %8s\n",
		"case", "hexProof", "hexNodes", "binProof", "binNodes", "/hex", "blob B")
	for _, r := range rows {
		s += fmt.Sprintf("%-16s %9d %8d | %9d %8d %6.2fx | %8d\n",
			r.name, r.hexProof, r.hexNodes,
			r.leanProof, r.leanNodes, float64(r.leanProof)/float64(r.hexProof), r.blob())
	}
	return s
}
