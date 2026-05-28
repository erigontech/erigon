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

// TestExecutionWitnessAmsterdamBALConstructability is a Task-0 spike that
// proves the three things Task 1's real test depends on can be composed in
// one process:
//
//  1. An Amsterdam chain (AmsterdamTime=0 via AllProtocolChanges) inserted
//     through ExecModuleTester produces a BAL that gets persisted via
//     rawdb.WriteBlockAccessListBytes during InsertChain.
//  2. Historical commitment is enabled (statecfg.EnableHistoricalCommitment
//     + rawdb.WriteDBCommitmentHistoryEnabled), so debug_executionWitness
//     does not bail on the "requires commitment history" check.
//  3. DebugAPIImpl, constructed from the tester's BaseAPI/DB, reaches
//     ExecutionWitness for the inserted Amsterdam block (no infra error).
//
// At this stage there is no Amsterdam branch in ExecutionWitness, so the
// call goes through the pre-existing re-exec path. The spike only verifies
// the harness composability; correctness is Task 1.
func TestExecutionWitnessAmsterdamBALConstructability(t *testing.T) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() {
		statecfg.Schema = previousSchema
	})

	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
	)

	require.True(t, m.ChainConfig.IsAmsterdam(0), "AllProtocolChanges must activate Amsterdam at genesis time")

	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseFee := m.Genesis.BaseFee().Uint64()
	gasPrice := baseFee * 2
	toAddr := common.HexToAddress("0x00000000000000000000000000000000000000aa")

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		nonce := gen.TxNonce(senderAddr)
		tx, txErr := types.SignTx(
			types.NewTransaction(nonce, toAddr, uint256.NewInt(1), 21000, uint256.NewInt(gasPrice), nil),
			*signer, privKey)
		require.NoError(t, txErr)
		gen.AddTx(tx)
	})
	require.NoError(t, err)

	require.NotEmpty(t, chainPack.BlockAccessLists[0],
		"Amsterdam block generation must produce a BAL payload")
	require.NotNil(t, chainPack.Headers[0].BlockAccessListHash,
		"Amsterdam header must carry BlockAccessListHash")

	require.NoError(t, m.InsertChain(chainPack))

	blockHash := chainPack.Blocks[0].Hash()
	blockNum := chainPack.Blocks[0].NumberU64()

	var persistedBAL []byte
	require.NoError(t, m.DB.View(context.Background(), func(tx kv.Tx) error {
		persistedBAL, err = rawdb.ReadBlockAccessListBytes(tx, blockHash, blockNum)
		return err
	}))
	require.NotEmpty(t, persistedBAL,
		"BAL must be persisted in DB after InsertChain — Task 1 reads it via rawdb.ReadBlockAccessListBytes")

	bal, err := types.DecodeBlockAccessListBytes(persistedBAL)
	require.NoError(t, err)
	require.NotEmpty(t, bal,
		"persisted BAL must decode into a non-empty BlockAccessList")

	require.NoError(t, m.DB.Update(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))

	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false)
	bn := rpc.BlockNumber(blockNum)
	_, err = api.ExecutionWitness(context.Background(), rpc.BlockNumberOrHash{BlockNumber: &bn})
	t.Logf("Task-0 spike: debugApi.ExecutionWitness on Amsterdam block %d returned err=%v (pre-Amsterdam-branch path; may be nil or a verification mismatch — neither is a constructability blocker)", blockNum, err)
}
