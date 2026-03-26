// Copyright 2024 The Erigon Authors
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

package handler

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
)

// TestCaplinBlockProductionWithWithdrawalRequest tests Caplin's produceBeaconBody
// against a real Erigon execution layer. A withdrawal request transaction is
// submitted to the EIP-7002 system contract, and then Caplin's actual block
// production code builds the beacon body — calling ForkChoiceUpdate,
// GetAssembledBlock, and decoding the execution requests. This is the code path
// that was broken in issue #14319 and fixed in PR #14326.
func TestCaplinBlockProductionWithWithdrawalRequest(t *testing.T) {
	ctx := context.Background()

	// --- Set up real execution layer ---

	m := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))

	// Insert 1 initial block so we have a chain head.
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		tx, err := types.SignTx(
			types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key,
		)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)

	// Submit a withdrawal request transaction (EIP-7002).
	var pubkey [48]byte
	for i := range pubkey {
		pubkey[i] = 0x01
	}
	var calldata []byte
	calldata = append(calldata, pubkey[:]...)
	calldata = append(calldata, make([]byte, 8)...) // amount=0 → full exit

	baseFee := chainPack.TopBlock.BaseFee().Uint64()
	withdrawalAddr := params.WithdrawalRequestAddress.Value()
	withdrawalTx, err := types.SignTx(
		&types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    1,
				GasLimit: 1_000_000,
				To:       &withdrawalAddr,
				Value:    *uint256.NewInt(500_000_000_000_000_000), // 0.5 ETH
				Data:     calldata,
			},
			GasPrice: *uint256.NewInt(baseFee),
		},
		*types.LatestSignerForChainID(m.ChainConfig.ChainID),
		m.Key,
	)
	require.NoError(t, err)

	var txBuf bytes.Buffer
	err = withdrawalTx.EncodeRLP(&txBuf)
	require.NoError(t, err)
	addResp, err := m.TxPoolGrpcServer.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{txBuf.Bytes()}})
	require.NoError(t, err)
	require.Equal(t, "success", addResp.Errors[0])

	// --- Wire real EL into Caplin's ApiHandler ---

	chainRW := chainreader.NewChainReaderEth1(
		m.ChainConfig,
		direct.NewExecutionClientDirect(m.ExecModule),
		time.Hour,
	)
	engine, err := execution_client.NewExecutionClientDirect(chainRW, nil)
	require.NoError(t, err)

	// Set up handler with Electra test data (provides validator set, RANDAO, etc.)
	// and our real execution engine.
	_, blocks, _, _, postState, h, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	h.engine = engine

	// Patch the beacon state's execution payload header to point at the real
	// EL chain head — this is how produceBeaconBody knows what hash to send
	// in ForkChoiceUpdate.
	elHead := chainPack.TopBlock.Header()
	elHeader := cltypes.NewEth1Header(clparams.ElectraVersion)
	elHeader.BlockHash = elHead.Hash()
	elHeader.BlockNumber = elHead.Number.Uint64()
	elHeader.Time = elHead.Time
	elHeader.BaseFeePerGas = common.BigToHash(elHead.BaseFee.ToBig())
	postState.SetLatestExecutionPayloadHeader(elHeader)

	// Make GetEth1Hash return the EL head hash for any checkpoint root —
	// produceBeaconBody falls back to head when the hash is zero, but we
	// set it explicitly for clarity.
	elHeadHash := elHead.Hash()
	fcu.Eth1Hashes[postState.FinalizedCheckpoint().Root] = elHeadHash
	fcu.Eth1Hashes[postState.CurrentJustifiedCheckpoint().Root] = elHeadHash

	// --- Call Caplin's actual block production ---

	baseBlock := blocks[len(blocks)-1].Block
	targetSlot := baseBlock.Slot + 1
	baseBlockRoot, err := baseBlock.HashSSZ()
	require.NoError(t, err)

	beaconBody, execValue, err := h.produceBeaconBody(
		ctx, 3, baseBlock.Slot, baseBlockRoot, postState, targetSlot,
		common.Bytes96{0xc0}, // infinity BLS signature (skip RANDAO verification)
		common.Hash{},
	)
	require.NoError(t, err)
	require.NotNil(t, beaconBody)
	require.NotZero(t, execValue)

	// --- Verify execution requests were decoded by Caplin ---

	require.NotNil(t, beaconBody.ExecutionRequests,
		"ExecutionRequests must not be nil — this was the bug in issue #14319")
	require.Greater(t, beaconBody.ExecutionRequests.Withdrawals.Len(), 0,
		"expected at least 1 withdrawal request from the EL system contract")

	gotWithdrawal := beaconBody.ExecutionRequests.Withdrawals.Get(0)
	require.Equal(t, common.Bytes48(pubkey), gotWithdrawal.ValidatorPubKey,
		"withdrawal request pubkey should match what was submitted")
	require.Equal(t, uint64(0), gotWithdrawal.Amount,
		"withdrawal request amount should be 0 (full exit)")
}
