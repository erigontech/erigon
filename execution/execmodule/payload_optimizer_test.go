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

package execmodule_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/payloadoptimizer"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider"
)

type oneBatchTxnProvider struct {
	mu   sync.Mutex
	txns types.Transactions
}

func (p *oneBatchTxnProvider) ProvideTxns(ctx context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	txns := p.txns
	p.txns = nil
	return txns, nil
}

func TestPayloadOptimizerMatchesTheCanonicalColdBuilder(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	chainPack, err := m.GenerateChain(1, func(_ int, gen *blockgen.BlockGen) {
		tx, txErr := types.SignTx(
			types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil),
			*types.LatestSignerForChainID(m.ChainConfig.ChainID),
			m.Key,
		)
		require.NoError(t, txErr)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	parent := chainPack.TopBlock
	var pubkey [48]byte
	for i := range pubkey {
		pubkey[i] = 0x02
	}
	calldata := append(pubkey[:], make([]byte, 8)...)
	withdrawalAddress := params.WithdrawalRequestAddress.Value()
	orderflowTx, err := types.SignTx(
		&types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    1,
				GasLimit: 1_000_000,
				To:       &withdrawalAddress,
				Value:    *uint256.NewInt(500_000_000_000_000_000),
				Data:     calldata,
			},
			GasPrice: *uint256.NewInt(parent.BaseFee().Uint64() + 1),
		},
		*types.LatestSignerForChainID(m.ChainConfig.ChainID),
		m.Key,
	)
	require.NoError(t, err)
	orderflowTx.SetSender(accounts.InternAddress(m.Address))
	targetGasLimit := parent.GasLimit() + 1_000_000
	beaconRoot := randomHash()
	buildParams := &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             parent.Time() + 1,
		PrevRandao:            parent.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{3},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &beaconRoot,
		SlotNumber:            syntheticSlotNumber(parent),
		TargetGasLimit:        &targetGasLimit,
	}
	oracleParams := buildParams.Copy()
	oracleParams.CustomTxnProvider = &oneBatchTxnProvider{txns: types.Transactions{orderflowTx}}
	oracleID, err := m.ExecModule.AssembleBlock(ctx, oracleParams)
	require.NoError(t, err)
	require.False(t, oracleID.Busy)
	oracle := collectPayloadOptimizerResult(t, ctx, m.ExecModule, oracleID.PayloadID)
	m.ExecModule.DiscardAssembledBlock(oracleID.PayloadID)

	buildCtx, err := payloadoptimizer.NewBuildContext(buildParams, [4]byte{0x07}, nil, parent.GasLimit())
	require.NoError(t, err)
	session, err := payloadoptimizer.New(m.ExecModule).Open(ctx, buildCtx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{orderflowTx})
	require.NoError(t, err)
	candidate, err := session.Apply(ctx, update)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	actual := candidate.Block()

	require.Equal(t, oracle.Block.Block.Header(), actual.Block.Header())
	require.Equal(t, oracle.Block.Block.Hash(), actual.Block.Hash())
	require.Equal(t, transactionHashes(oracle.Block.Block.Transactions()), transactionHashes(actual.Block.Transactions()))
	require.Equal(t, oracle.Block.Receipts, actual.Receipts)
	require.Equal(t, oracle.Block.Requests, actual.Requests)
	require.NotEmpty(t, actual.Requests)
	require.Equal(t, oracle.Block.BlockAccessList, actual.BlockAccessList)
	require.Equal(t, oracle.Block.Block.BlockAccessListHash(), actual.Block.BlockAccessListHash())
	require.Equal(t, oracle.Block.Block.Header().BlobGasUsed, actual.Block.Header().BlobGasUsed)
	require.Equal(t, oracle.Block.Block.Header().ExcessBlobGas, actual.Block.Header().ExcessBlobGas)
	require.Equal(t, oracle.BlockValue, candidate.Value())
}

func transactionHashes(transactions types.Transactions) []common.Hash {
	hashes := make([]common.Hash, len(transactions))
	for i, transaction := range transactions {
		hashes[i] = transaction.Hash()
	}
	return hashes
}

func collectPayloadOptimizerResult(t *testing.T, ctx context.Context, module *execmodule.ExecModule, payloadID uint64) execmodule.AssembledBlockResult {
	t.Helper()
	for {
		result, err := module.GetAssembledBlock(ctx, payloadID)
		require.NoError(t, err)
		if !result.Busy {
			return result
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}
