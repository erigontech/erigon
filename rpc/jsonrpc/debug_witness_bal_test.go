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
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// amsterdamBALHarness is the shared setup used by Task 1's tests. It returns
// the tester, the inserted ChainPack, and a constructed DebugAPIImpl with
// commitment history enabled. Each test installs its own
// recordingStateConstructedHookForTest counter.
type amsterdamBALHarness struct {
	m         *execmoduletester.ExecModuleTester
	chainPack *blockgen.ChainPack
	api       *DebugAPIImpl
}

func setupAmsterdamBALHarness(t *testing.T, cfg *chain.Config, numBlocks int) *amsterdamBALHarness {
	t.Helper()

	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() {
		statecfg.Schema = previousSchema
	})

	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	genesis := &types.Genesis{
		Config: cfg,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
	)

	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseFee := m.Genesis.BaseFee().Uint64()
	gasPrice := baseFee * 2
	toAddr := common.HexToAddress("0x00000000000000000000000000000000000000aa")

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, numBlocks, func(i int, gen *blockgen.BlockGen) {
		nonce := gen.TxNonce(senderAddr)
		tx, txErr := types.SignTx(
			types.NewTransaction(nonce, toAddr, uint256.NewInt(1), 21000, uint256.NewInt(gasPrice), nil),
			*signer, privKey)
		require.NoError(t, txErr)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))

	require.NoError(t, m.DB.Update(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))

	return &amsterdamBALHarness{
		m:         m,
		chainPack: chainPack,
		api:       NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false),
	}
}

// installRecordingHookCounter installs a fresh counter on the
// recordingStateConstructedHookForTest seam and returns a closure that reads
// the current value. Test cleanup restores the previous hook.
func installRecordingHookCounter(t *testing.T) func() int {
	t.Helper()
	previous := recordingStateConstructedHookForTest
	count := 0
	recordingStateConstructedHookForTest = func() {
		count++
	}
	t.Cleanup(func() {
		recordingStateConstructedHookForTest = previous
	})
	return func() int { return count }
}

// TestExecutionWitnessAmsterdamBAL is Task 1's primary failing test (RED until
// Task 3 lands the IsAmsterdam branch in ExecutionWitness). It asserts that on
// an Amsterdam block:
//
//  1. The re-exec path is NOT taken (recording-state hook counter remains 0).
//  2. The resulting witness verifies via verifyWitnessStateless (the merge
//     gate per the plan's Testing Strategy).
func TestExecutionWitnessAmsterdamBAL(t *testing.T) {
	h := setupAmsterdamBALHarness(t, chain.AllProtocolChanges, 1)
	require.True(t, h.m.ChainConfig.IsAmsterdam(h.chainPack.Headers[0].Time),
		"AllProtocolChanges must activate Amsterdam for the inserted block")
	require.NotEmpty(t, h.chainPack.BlockAccessLists[0],
		"BAL must be produced for an Amsterdam block")

	hookCount := installRecordingHookCounter(t)

	bn := rpc.BlockNumber(h.chainPack.Blocks[0].NumberU64())
	result, err := h.api.ExecutionWitness(context.Background(), rpc.BlockNumberOrHash{BlockNumber: &bn})

	require.NoError(t, err, "ExecutionWitness must succeed on the Amsterdam block via the BAL branch (verifyWitnessStateless is the merge gate)")
	require.NotNil(t, result)
	require.NotNil(t, result.State, "State must not be nil")
	require.Equal(t, 0, hookCount(), "Amsterdam block must NOT take the re-exec path — IsAmsterdam gate failed to flip; RecordingState was constructed %d time(s)", hookCount())
}

// TestExecutionWitnessAmsterdamActivationBlock locks the IsAmsterdam boundary
// at header.Time == AmsterdamTime. AllProtocolChanges sets AmsterdamTime=0,
// which makes the genesis block sit exactly on the boundary; we assert
// IsAmsterdam is inclusive at that point at the chain-config level, then
// exercise the first Amsterdam-era block through the handler.
//
// A non-zero AmsterdamTime can't be tested via the in-test chain builder:
// when parent.Time < AmsterdamTime, the parallel-exec BAL infra isn't
// allocated (chain_makers.go gates on IsAmsterdam(parent.Time)), and
// InsertChain then fails because the serial executor refuses Amsterdam
// blocks. The unit-level boundary check below covers that case structurally.
func TestExecutionWitnessAmsterdamActivationBlock(t *testing.T) {
	cfg := chain.AllProtocolChanges
	require.NotNil(t, cfg.AmsterdamTime, "AllProtocolChanges must schedule Amsterdam")
	require.True(t, cfg.IsAmsterdam(*cfg.AmsterdamTime),
		"IsAmsterdam must be inclusive at header.Time == AmsterdamTime")

	h := setupAmsterdamBALHarness(t, cfg, 1)
	require.Equal(t, *cfg.AmsterdamTime, h.m.Genesis.Time(),
		"genesis must sit exactly on the AmsterdamTime boundary for this fixture")

	hookCount := installRecordingHookCounter(t)

	bn := rpc.BlockNumber(h.chainPack.Blocks[0].NumberU64())
	_, err := h.api.ExecutionWitness(context.Background(), rpc.BlockNumberOrHash{BlockNumber: &bn})

	require.NoError(t, err, "first Amsterdam-era block must produce a verifiable witness via the BAL branch")
	require.Equal(t, 0, hookCount(),
		"activation-era block must take the BAL branch — IsAmsterdam gate failed to flip; RecordingState was constructed %d time(s)", hookCount())
}
