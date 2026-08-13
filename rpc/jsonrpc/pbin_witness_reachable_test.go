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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// The commitment variant and its hash are datadir properties resolved process-wide,
// so a test using this may never run in parallel.
func withBinCommitmentDatadir(t *testing.T) {
	t.Helper()

	origBin, origHash, origSuite := statecfg.ExperimentalBinCommitment, statecfg.BinCommitmentHash, commitment.PBinHashSuiteName()
	origParallel := statecfg.ExperimentalParallelCommitment
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = origBin
		statecfg.BinCommitmentHash = origHash
		require.NoError(t, commitment.SetPBinHashSuite(origSuite))
		statecfg.ExperimentalParallelCommitment = origParallel
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	// erigondb.toml resolution refuses the combination: the bin trie is
	// sequential-only, regardless of a process-wide parallel default.
	statecfg.ExperimentalParallelCommitment = false
}

func withCommitmentHistory(t *testing.T) {
	t.Helper()

	previousSchema := statecfg.Schema
	t.Cleanup(func() { statecfg.Schema = previousSchema })
	statecfg.EnableHistoricalCommitment()
}

func enableCommitmentHistoryFlag(t *testing.T, db kv.TemporalRwDB) {
	t.Helper()

	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))
}

// TestPBinExecutionWitnessReachable confirms a bin datadir reaches the witness pipeline
// instead of ErrBinCommitmentUnsupported. Under bin the stateless gate is not skippable, so
// a returned witness is one that re-executed the block to its post-state root.
func TestPBinExecutionWitnessReachable(t *testing.T) {
	// No t.Parallel: mutates process-global commitment flags.
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)

	m, _, _, _ := chainWithDeployedContract(t)
	enableCommitmentHistoryFlag(t, m.DB)
	require.True(t, binCommitmentTrie(), "the chain above is committed with the binary trie")

	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})

	// Block 2 calls the contract deployed by block 1, so its witness covers an account
	// read, a storage write and a code read.
	bn := rpc.BlockNumber(2)
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &bn}, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, result.State, "a block that touches state proves it with nodes")
	require.NotEmpty(t, result.Keys)
}

// The witness capture serves the sequential hex trie and the bin trie; the parallel
// trie it cannot serve must still be demoted rather than reaching the capture.
func TestWitnessPathDemotesParallelTrie(t *testing.T) {
	// No t.Parallel: mutates process-global commitment flags.
	withCommitmentHistory(t)

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	enableCommitmentHistoryFlag(t, m.DB)

	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})
	bn := rpc.BlockNumber(3)
	block := rpc.BlockNumberOrHash{BlockNumber: &bn}

	sequential, err := api.ExecutionWitness(t.Context(), block, nil)
	require.NoError(t, err)

	orig := statecfg.ExperimentalParallelCommitment
	t.Cleanup(func() { statecfg.ExperimentalParallelCommitment = orig })
	statecfg.ExperimentalParallelCommitment = true

	demoted, err := api.ExecutionWitness(t.Context(), block, nil)
	require.NoError(t, err, "the parallel trie must be demoted, not handed to the witness capture")
	require.Equal(t, sequential.State, demoted.State)
}

// eth_getWitness recomputes with the hex trie and has no bin implementation, so it
// must keep refusing a bin datadir rather than reading bit-path records as hex ones.
func TestPBinGetWitnessRefusesBin(t *testing.T) {
	// No t.Parallel: mutates process-global commitment flags.
	withCommitmentHistory(t)

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	enableCommitmentHistoryFlag(t, m.DB)

	cfg := &rpccfg.EthApiConfig{
		GasCap:                      5000000,
		FeeCap:                      ethconfig.Defaults.RPCTxFeeCap,
		ReturnDataLimit:             100_000,
		MaxGetProofRewindBlockCount: 1,
		SubscribeLogsChannelSize:    128,
		RpcTxSyncDefaultTimeout:     20 * time.Second,
		RpcTxSyncMaxTimeout:         1 * time.Minute,
	}
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, cfg, log.New())

	// The chain above is built on the hex trie; only the witness call runs under bin.
	origBin := statecfg.ExperimentalBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalBinCommitment = origBin })
	statecfg.ExperimentalBinCommitment = true

	bn := rpc.BlockNumber(3)
	_, err := api.GetWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &bn})
	require.ErrorIs(t, err, execctx.ErrBinCommitmentUnsupported)
}

// debug_executionWitness is the only caller that stopped declaring itself hex-only.
// The refusal of the rest is a source property — each has to keep passing the option
// whose bin behaviour execctx.TestPBinHexOnlyCommitmentRefusesBin pins — so it is
// checked where it lives rather than by re-deriving every caller's preconditions.
func TestPBinHexOnlyCallersStillRefuse(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..")
	for _, rel := range []string{
		"rpc/jsonrpc/eth_call.go",       // eth_getProof, eth_getWitness
		"rpc/jsonrpc/eth_simulation.go", // eth_simulateV1
		"rpc/jsonrpc/receipts/receipts_generator.go",
		"rpc/rpchelper/commitment.go",
		"db/integrity/commitment_integrity.go",
	} {
		src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		require.NoError(t, err)
		for i, line := range strings.Split(string(src), "\n") {
			if !strings.Contains(line, "execctx.NewSharedDomains(") {
				continue
			}
			require.Contains(t, line, "execctx.WithHexCommitmentOnly()",
				"%s:%d recomputes with the hex trie and must keep refusing bin", rel, i+1)
		}
	}

	src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash("rpc/jsonrpc/debug_execution_witness.go")))
	require.NoError(t, err)
	require.NotContains(t, string(src), "execctx.WithHexCommitmentOnly()",
		"the witness path serves bin through its own collector")
}
