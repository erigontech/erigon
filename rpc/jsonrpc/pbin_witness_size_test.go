// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// hexWitnessBaselinePath pins the hex arm's measured sizes. Regenerate with
// ERIGON_UPDATE_HEX_WITNESS_BASELINE=true when hex witness output changes on purpose.
var hexWitnessBaselinePath = filepath.Join("testdata", "hex_witness_baseline.json")

// witnessSizes is one block's witness payload split into the parts that scale
// differently. The split matters because under the binary trie a block's code is
// committed as chunk leaves inside State, so Codes repeats bytes State already
// carries; see totalBytes.
type witnessSizes struct {
	Block       uint64 `json:"block"`
	Shape       string `json:"shape"`
	Nodes       int    `json:"nodes"`
	StateBytes  int    `json:"stateBytes"`
	Codes       int    `json:"codes"`
	CodeBytes   int    `json:"codeBytes"`
	Headers     int    `json:"headers"`
	HeaderBytes int    `json:"headerBytes"`
}

// totalBytes is what a stateless verifier has to be handed. Under bin the code
// blobs are redundant — the reader reassembles code from the chunk leaves already
// counted in StateBytes — so adding Codes there would count code twice and make
// the two arms incomparable.
func (s witnessSizes) totalBytes(binTrie bool) int {
	if binTrie {
		return s.StateBytes + s.HeaderBytes
	}
	return s.StateBytes + s.CodeBytes + s.HeaderBytes
}

func sumBytes(blobs []hexutil.Bytes) int {
	total := 0
	for _, blob := range blobs {
		total += len(blob)
	}
	return total
}

// pbinWitnessCorpus names what each block of buildPBinWitnessChain exercises, so
// the measured table reads as a size per witness shape rather than per block number.
var pbinWitnessCorpus = []struct {
	num   uint64
	shape string
}{
	{1, "plain transfer"},
	{2, "deploy within one code-zone group"},
	{3, "deploy crossing a group boundary"},
	{4, "storage write"},
	{5, "SSTORE to zero"},
	{6, "code read across a group boundary"},
	{7, "no transactions"},
}

// measureWitnessSizes builds the corpus chain under one commitment variant and
// measures every block's witness. Both arms run with the stateless gate on, so a
// witness that got measured is a witness that re-executed its block to the header's
// post-state root.
func measureWitnessSizes(t *testing.T, binTrie bool) []witnessSizes {
	t.Helper()

	t.Setenv("ERIGON_WITNESS_NO_VERIFY", "false")
	if binTrie {
		withBinCommitmentDatadir(t)
	}
	require.Equal(t, binTrie, binCommitmentTrie())
	require.False(t, witnessVerifySkipped(binTrie), "a measured witness must be a verified one")

	c := buildPBinWitnessChain(t)
	enableCommitmentHistoryFlag(t, c.m.DB)
	api := NewPrivateDebugAPI(newBaseApiForTest(c.m), c.m.DB, nil, &rpccfg.DebugApiConfig{})

	sizes := make([]witnessSizes, 0, len(pbinWitnessCorpus))
	for _, block := range pbinWitnessCorpus {
		result := pbinWitnessOf(t, api, block.num)
		require.NotEmpty(t, result.State, "block %d touches state", block.num)
		sizes = append(sizes, witnessSizes{
			Block:       block.num,
			Shape:       block.shape,
			Nodes:       len(result.State),
			StateBytes:  sumBytes(result.State),
			Codes:       len(result.Codes),
			CodeBytes:   sumBytes(result.Codes),
			Headers:     len(result.Headers),
			HeaderBytes: sumBytes(result.Headers),
		})
	}
	return sizes
}

// requireHexBaseline holds the hex arm to its committed numbers. The bin work must
// leave hex witness output alone, and a golden file makes that checkable here rather
// than by building the same corpus on another branch.
func requireHexBaseline(t *testing.T, sizes []witnessSizes) {
	t.Helper()

	encoded, err := json.MarshalIndent(sizes, "", "  ")
	require.NoError(t, err)
	encoded = append(encoded, '\n')

	if dbg.EnvBool("ERIGON_UPDATE_HEX_WITNESS_BASELINE", false) {
		require.NoError(t, os.WriteFile(hexWitnessBaselinePath, encoded, 0o644))
		t.Fatalf("rewrote %s; re-run without ERIGON_UPDATE_HEX_WITNESS_BASELINE", hexWitnessBaselinePath)
	}

	baseline, err := os.ReadFile(hexWitnessBaselinePath)
	require.NoError(t, err)
	require.JSONEq(t, string(baseline), string(encoded),
		"hex witness sizes moved: the bin witness path must leave the hex one byte-identical")
}

// witnessSizeTable renders the measured arms as markdown, for the plan's table.
func witnessSizeTable(hexArm, binArm []witnessSizes) string {
	var b strings.Builder
	b.WriteString("| block | shape | hex nodes | hex state B | bin nodes | bin state B | bin/hex state |\n")
	b.WriteString("|---|---|---:|---:|---:|---:|---:|\n")

	var hexTotal, binTotal int
	for i, h := range hexArm {
		n := binArm[i]
		hexTotal += h.totalBytes(false)
		binTotal += n.totalBytes(true)
		fmt.Fprintf(&b, "| %d | %s | %d | %d | %d | %d | %.2fx |\n",
			h.Block, h.Shape, h.Nodes, h.StateBytes, n.Nodes, n.StateBytes,
			float64(n.StateBytes)/float64(h.StateBytes))
	}

	b.WriteString("\n| block | hex codes | hex code B | bin codes | bin code B | hex headers B | bin headers B |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|\n")
	for i, h := range hexArm {
		n := binArm[i]
		fmt.Fprintf(&b, "| %d | %d | %d | %d | %d | %d | %d |\n",
			h.Block, h.Codes, h.CodeBytes, n.Codes, n.CodeBytes, h.HeaderBytes, n.HeaderBytes)
	}

	fmt.Fprintf(&b, "\ncorpus total handed to a verifier: hex %d B, bin %d B (%.2fx)\n",
		hexTotal, binTotal, float64(binTotal)/float64(hexTotal))
	return b.String()
}

// TestWitnessSizeBinVsHex builds one block sequence twice — same genesis, same
// transactions, different commitment trie — and measures both witnesses, so binary
// witness sizes come from real blocks instead of estimates.
func TestWitnessSizeBinVsHex(t *testing.T) {
	// No t.Parallel, and the arms run in sequence: the commitment variant and its
	// hash suite are process-global, and each arm restores what it set.
	withCommitmentHistory(t)

	var hexArm, binArm []witnessSizes
	t.Run("hex", func(t *testing.T) { hexArm = measureWitnessSizes(t, false) })
	t.Run("bin", func(t *testing.T) { binArm = measureWitnessSizes(t, true) })
	require.Len(t, hexArm, len(pbinWitnessCorpus))
	require.Len(t, binArm, len(pbinWitnessCorpus))

	requireHexBaseline(t, hexArm)
	t.Log("witness sizes, bin vs hex:\n" + witnessSizeTable(hexArm, binArm))
}
