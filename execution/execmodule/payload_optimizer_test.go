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
	"bytes"
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	execpkg "github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/payloadoptimizer"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/txnprovider"
)

type oneBatchTxnProvider struct {
	mu   sync.Mutex
	txns types.Transactions
}

type countedRetainedTxnProvider struct {
	transaction types.Transaction
	calls       atomic.Uint64
}

type stopBoundedRetainedTxnProvider struct {
	transaction types.Transaction
	calls       atomic.Uint64
	sawDeadline chan struct{}
	deadline    sync.Once
}

func (p *countedRetainedTxnProvider) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	batch, err := p.ProvideRetainedTxns(ctx, opts...)
	return batch.Transactions, err
}

func (p *countedRetainedTxnProvider) ProvideRetainedTxns(ctx context.Context, opts ...txnprovider.ProvideOption) (builder.RetainedTxnBatch, error) {
	if err := ctx.Err(); err != nil {
		return builder.RetainedTxnBatch{}, err
	}
	p.calls.Add(1)
	options := txnprovider.ApplyProvideOptions(opts...)
	hash := [32]byte(p.transaction.Hash())
	if options.TxnIdsFilter != nil && options.TxnIdsFilter.Contains(hash) {
		return builder.RetainedTxnBatch{PassComplete: true}, nil
	}
	if options.TxnIdsFilter != nil {
		options.TxnIdsFilter.Add(hash)
	}
	return builder.RetainedTxnBatch{
		Transactions:       types.Transactions{p.transaction},
		NewlyYieldedTxnIDs: [][32]byte{hash},
		PassComplete:       true,
	}, nil
}

func (p *stopBoundedRetainedTxnProvider) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	batch, err := p.ProvideRetainedTxns(ctx, opts...)
	return batch.Transactions, err
}

func (p *stopBoundedRetainedTxnProvider) ProvideRetainedTxns(ctx context.Context, opts ...txnprovider.ProvideOption) (builder.RetainedTxnBatch, error) {
	call := p.calls.Add(1)
	if _, ok := ctx.Deadline(); call > 1 && ok {
		p.deadline.Do(func() { close(p.sawDeadline) })
		<-ctx.Done()
		return builder.RetainedTxnBatch{}, ctx.Err()
	}
	options := txnprovider.ApplyProvideOptions(opts...)
	hash := [32]byte(p.transaction.Hash())
	if options.TxnIdsFilter != nil {
		options.TxnIdsFilter.Add(hash)
	}
	return builder.RetainedTxnBatch{
		Transactions:       types.Transactions{p.transaction},
		NewlyYieldedTxnIDs: [][32]byte{hash},
	}, nil
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
	oracleID, err := assemblePayloadOptimizerBlock(ctx, m.ExecModule, oracleParams)
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

	requireHeadersEqual(t, oracle.Block.Block.Header(), actual.Block.Header())
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

func TestPayloadOptimizerResolvesNondefaultBuilderConfiguration(t *testing.T) {
	ctx := t.Context()
	targetGasLimit, maxBlobs := uint64(31_000_000), uint64(1)
	buildDefaults := buildercfg.BuilderConfig{
		GasLimit:         &targetGasLimit,
		ExtraData:        []byte{0xaa, 0xbb},
		MaxBlobsPerBlock: &maxBlobs,
	}
	m := execmoduletester.New(t,
		execmoduletester.WithChainConfig(chain.AllProtocolChanges),
		execmoduletester.WithBuilderConfig(buildDefaults),
	)
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	parent := chainPack.TopBlock
	orderflowTx, err := types.SignTx(
		types.NewTransaction(0, common.Address{1}, uint256.NewInt(1), params.TxGas, uint256.NewInt(parent.BaseFee().Uint64()+1), nil),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID),
		m.Key,
	)
	require.NoError(t, err)
	orderflowTx.SetSender(accounts.InternAddress(m.Address))
	beaconRoot := randomHash()
	buildParams := &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             parent.Time() + 1,
		PrevRandao:            parent.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{3},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &beaconRoot,
		SlotNumber:            syntheticSlotNumber(parent),
	}
	oracleParams := buildParams.Copy()
	oracleParams.CustomTxnProvider = &oneBatchTxnProvider{txns: types.Transactions{orderflowTx}}
	oracleID, err := assemblePayloadOptimizerBlock(ctx, m.ExecModule, oracleParams)
	require.NoError(t, err)
	oracle := collectPayloadOptimizerResult(t, ctx, m.ExecModule, oracleID.PayloadID)
	m.ExecModule.DiscardAssembledBlock(oracleID.PayloadID)

	defaults := payloadoptimizer.BuildDefaultsFromConfig(buildDefaults)
	buildCtx, err := payloadoptimizer.NewBuildContext(buildParams, [4]byte{0x07}, nil, parent.GasLimit(), defaults)
	require.NoError(t, err)
	require.Equal(t, targetGasLimit, *buildCtx.Parameters().TargetGasLimit)
	require.Equal(t, []byte{0xaa, 0xbb}, buildCtx.Parameters().ExtraData)
	require.Equal(t, maxBlobs, *buildCtx.Parameters().MaxBlobsPerBlock)
	session, err := payloadoptimizer.New(m.ExecModule).Open(ctx, buildCtx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{orderflowTx})
	require.NoError(t, err)
	candidate, err := session.Apply(ctx, update)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	actual := candidate.Block()

	require.Equal(t, oracle.Block.Block.Hash(), actual.Block.Hash())
	requireHeadersEqual(t, oracle.Block.Block.Header(), actual.Block.Header())
	require.Equal(t, transactionHashes(oracle.Block.Block.Transactions()), transactionHashes(actual.Block.Transactions()))
	require.Equal(t, oracle.Block.Receipts, actual.Receipts)
	require.Equal(t, oracle.BlockValue, candidate.Value())
}

func TestPayloadOptimizerAcceptsGloasTargetAboveHeaderBounds(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config:   chain.AllProtocolChanges,
		GasLimit: 30_000_000,
	}))
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	parent := chainPack.TopBlock
	require.Equal(t, uint64(30_000_000), parent.GasLimit())
	targetGasLimit := uint64(math.MaxUint64)
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
	buildCtx, err := payloadoptimizer.NewBuildContext(buildParams, [4]byte{0x07}, nil, parent.GasLimit())
	require.NoError(t, err)
	require.Equal(t, targetGasLimit, *buildCtx.Parameters().TargetGasLimit)
	session, err := payloadoptimizer.New(m.ExecModule).Open(ctx, buildCtx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	update, err := payloadoptimizer.NewOrderflowUpdate(nil)
	require.NoError(t, err)
	candidate, err := session.Apply(ctx, update)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	require.Equal(t, misc.CalcGasLimit(parent.GasLimit(), targetGasLimit), candidate.Block().Block.GasLimit())
}

func TestPayloadOptimizerMatchesCanonicalProviderAcrossBatchBoundary(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t,
		execmoduletester.WithChainConfig(chain.AllProtocolChanges),
		execmoduletester.WithTxPool(),
	)
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	parent := chainPack.TopBlock
	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	ascending := make(types.Transactions, 51)
	rlpTxs := make([][]byte, len(ascending))
	for nonce := range ascending {
		transaction, signErr := types.SignTx(
			types.NewTransaction(uint64(nonce), common.Address{1}, uint256.NewInt(1), params.TxGas, uint256.NewInt(parent.BaseFee().Uint64()+1), nil),
			signer,
			m.Key,
		)
		require.NoError(t, signErr)
		transaction.SetSender(accounts.InternAddress(m.Address))
		ascending[nonce] = transaction
		var encoded bytes.Buffer
		require.NoError(t, transaction.EncodeRLP(&encoded))
		rlpTxs[nonce] = encoded.Bytes()
	}
	added, err := m.TxPoolGrpcServer.Add(ctx, &txpoolproto.AddRequest{RlpTxs: rlpTxs})
	require.NoError(t, err)
	for _, result := range added.Errors {
		require.Equal(t, "success", result)
	}
	beaconRoot := randomHash()
	buildParams := &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             parent.Time() + 1,
		PrevRandao:            parent.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{3},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &beaconRoot,
		SlotNumber:            syntheticSlotNumber(parent),
	}
	oracleID, err := assemblePayloadOptimizerBlock(ctx, m.ExecModule, buildParams)
	require.NoError(t, err)
	oracle := collectPayloadOptimizerResult(t, ctx, m.ExecModule, oracleID.PayloadID)
	m.ExecModule.DiscardAssembledBlock(oracleID.PayloadID)
	require.Len(t, oracle.Block.Block.Transactions(), len(ascending))

	descending := make(types.Transactions, len(ascending))
	for i := range ascending {
		descending[i] = ascending[len(ascending)-1-i]
	}
	buildCtx, err := payloadoptimizer.NewBuildContext(buildParams, [4]byte{0x07}, nil, parent.GasLimit())
	require.NoError(t, err)
	session, err := payloadoptimizer.New(m.ExecModule).Open(ctx, buildCtx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	update, err := payloadoptimizer.NewOrderflowUpdate(descending)
	require.NoError(t, err)
	candidate, err := session.Apply(ctx, update)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	actual := candidate.Block()

	require.Len(t, actual.Block.Transactions(), len(ascending))
	require.Equal(t, transactionHashes(oracle.Block.Block.Transactions()), transactionHashes(actual.Block.Transactions()))
	require.Equal(t, oracle.Block.Block.Hash(), actual.Block.Hash())
	require.Equal(t, oracle.Block.Receipts, actual.Receipts)
}

func TestPayloadOptimizerReconsidersBudgetAfterStableBlobRejection(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	parent := chainPack.TopBlock
	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseWrapper := types.MakeV1WrappedBlobTxn(m.ChainConfig.ChainID)
	baseWrapper.Tx.BlobVersionedHashes = baseWrapper.Tx.BlobVersionedHashes[:1]
	baseWrapper.Blobs = baseWrapper.Blobs[:1]
	baseWrapper.Commitments = baseWrapper.Commitments[:1]
	baseWrapper.Proofs = baseWrapper.Proofs[:params.CellsPerExtBlob]
	makeTransaction := func(feeCap uint64) types.Transaction {
		wrapper := types.CopyTxs(types.Transactions{baseWrapper})[0].(*types.BlobTxWrapper)
		wrapper.Tx.Nonce = 0
		wrapper.Tx.TipCap = *uint256.NewInt(1)
		wrapper.Tx.FeeCap = *uint256.NewInt(feeCap)
		wrapper.Tx.MaxFeePerBlobGas = *uint256.NewInt(1_000_000_000)
		transaction, signErr := types.SignTx(wrapper, signer, m.Key)
		require.NoError(t, signErr)
		transaction.SetSender(accounts.InternAddress(m.Address))
		return transaction
	}
	lowFee := makeTransaction(0)
	valid := makeTransaction(parent.BaseFee().Uint64() + 1)
	beaconRoot := randomHash()
	maxBlobs := uint64(1)
	buildParams := &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             parent.Time() + 1,
		PrevRandao:            parent.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{3},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &beaconRoot,
		SlotNumber:            syntheticSlotNumber(parent),
		MaxBlobsPerBlock:      &maxBlobs,
	}
	oracleParams := buildParams.Copy()
	oracleParams.CustomTxnProvider = &oneBatchTxnProvider{txns: types.Transactions{lowFee, valid}}
	oracleID, err := assemblePayloadOptimizerBlock(ctx, m.ExecModule, oracleParams)
	require.NoError(t, err)
	oracle := collectPayloadOptimizerResult(t, ctx, m.ExecModule, oracleID.PayloadID)
	m.ExecModule.DiscardAssembledBlock(oracleID.PayloadID)
	require.Equal(t, []common.Hash{valid.Hash()}, transactionHashes(oracle.Block.Block.Transactions()))
	oracleWrapper := oracle.Block.Block.Transactions()[0].(*types.BlobTxWrapper)
	require.Equal(t, byte(1), oracleWrapper.WrapperVersion)
	require.NotEmpty(t, oracleWrapper.Blobs)
	require.NotEmpty(t, oracleWrapper.Commitments)
	require.NotEmpty(t, oracleWrapper.Proofs)

	buildCtx, err := payloadoptimizer.NewBuildContext(buildParams, [4]byte{0x07}, nil, parent.GasLimit())
	require.NoError(t, err)
	session, err := payloadoptimizer.New(m.ExecModule).Open(ctx, buildCtx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{lowFee, valid})
	require.NoError(t, err)
	candidate, err := session.Apply(ctx, update)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	actual := candidate.Block()

	require.Equal(t, oracle.Block.Block.Hash(), actual.Block.Hash())
	require.Equal(t, transactionHashes(oracle.Block.Block.Transactions()), transactionHashes(actual.Block.Transactions()))
	actualWrapper := actual.Block.Transactions()[0].(*types.BlobTxWrapper)
	require.Equal(t, oracleWrapper.WrapperVersion, actualWrapper.WrapperVersion)
	require.Equal(t, oracleWrapper.Blobs, actualWrapper.Blobs)
	require.Equal(t, oracleWrapper.Commitments, actualWrapper.Commitments)
	require.Equal(t, oracleWrapper.Proofs, actualWrapper.Proofs)
	oracleBundle, err := engine_types.BlobsBundleFromTransactions(oracle.Block.Block.Transactions())
	require.NoError(t, err)
	actualBundle, err := engine_types.BlobsBundleFromTransactions(actual.Block.Transactions())
	require.NoError(t, err)
	require.NotEmpty(t, actualBundle.Blobs)
	require.Equal(t, oracleBundle, actualBundle)
}

func TestPayloadOptimizerReconsidersBudgetAfterStableRlpRejection(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	parent := chainPack.TopBlock
	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	beaconRoot := randomHash()
	buildParams := &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             parent.Time() + 1,
		PrevRandao:            parent.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{3},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &beaconRoot,
		SlotNumber:            syntheticSlotNumber(parent),
	}
	emptyParams := buildParams.Copy()
	emptyParams.CustomTxnProvider = &oneBatchTxnProvider{}
	emptyID, err := assemblePayloadOptimizerBlock(ctx, m.ExecModule, emptyParams)
	require.NoError(t, err)
	empty := collectPayloadOptimizerResult(t, ctx, m.ExecModule, emptyID.PayloadID)
	m.ExecModule.DiscardAssembledBlock(emptyID.PayloadID)
	assembled := &execpkg.AssembledBlock{
		Header:      empty.Block.Block.Header(),
		Uncles:      empty.Block.Block.Uncles(),
		Withdrawals: empty.Block.Block.Withdrawals(),
	}
	available := assembled.AvailableRlpSpace(m.ChainConfig)
	valid, err := types.SignTx(
		&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: 0, GasLimit: params.TxGas, To: &common.Address{1}},
			ChainID:  *m.ChainConfig.ChainID,
			TipCap:   *uint256.NewInt(1),
			FeeCap:   *uint256.NewInt(parent.BaseFee().Uint64() + 1),
		},
		signer,
		m.Key,
	)
	require.NoError(t, err)
	valid.SetSender(accounts.InternAddress(m.Address))
	validCost := valid.EncodingSize() + rlp.ListPrefixLen(valid.EncodingSize())
	require.Greater(t, validCost, 1)
	targetCost := available - 1
	dataSize := targetCost - 128
	var lowFee types.Transaction
	for range 4 {
		lowFee, err = types.SignTx(
			&types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: 0, GasLimit: parent.GasLimit(), To: &common.Address{1}, Data: make([]byte, dataSize)},
				ChainID:  *m.ChainConfig.ChainID,
				TipCap:   *uint256.NewInt(1),
				FeeCap:   *uint256.NewInt(0),
			},
			signer,
			m.Key,
		)
		require.NoError(t, err)
		cost := lowFee.EncodingSize() + rlp.ListPrefixLen(lowFee.EncodingSize())
		if cost == targetCost {
			break
		}
		dataSize += targetCost - cost
	}
	lowFee.SetSender(accounts.InternAddress(m.Address))
	require.Equal(t, targetCost, lowFee.EncodingSize()+rlp.ListPrefixLen(lowFee.EncodingSize()))
	require.Equal(t, 1, available-targetCost)

	oracleParams := buildParams.Copy()
	oracleParams.CustomTxnProvider = &oneBatchTxnProvider{txns: types.Transactions{lowFee, valid}}
	oracleID, err := assemblePayloadOptimizerBlock(ctx, m.ExecModule, oracleParams)
	require.NoError(t, err)
	oracle := collectPayloadOptimizerResult(t, ctx, m.ExecModule, oracleID.PayloadID)
	m.ExecModule.DiscardAssembledBlock(oracleID.PayloadID)
	require.Equal(t, []common.Hash{valid.Hash()}, transactionHashes(oracle.Block.Block.Transactions()))

	buildCtx, err := payloadoptimizer.NewBuildContext(buildParams, [4]byte{0x07}, nil, parent.GasLimit())
	require.NoError(t, err)
	session, err := payloadoptimizer.New(m.ExecModule).Open(ctx, buildCtx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{lowFee, valid})
	require.NoError(t, err)
	candidate, err := session.Apply(ctx, update)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	actual := candidate.Block()

	require.Equal(t, oracle.Block.Block.Hash(), actual.Block.Hash())
	require.Equal(t, transactionHashes(oracle.Block.Block.Transactions()), transactionHashes(actual.Block.Transactions()))
}

func TestPayloadOptimizerStopsAfterRetainedPassWithoutProgress(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t,
		execmoduletester.WithChainConfig(chain.AllProtocolChanges),
		execmoduletester.WithTxPool(),
	)
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	parent := chainPack.TopBlock
	transaction, err := types.SignTx(
		types.NewTransaction(1, common.Address{1}, uint256.NewInt(1), params.TxGas, uint256.NewInt(parent.BaseFee().Uint64()+1), nil),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID),
		m.Key,
	)
	require.NoError(t, err)
	transaction.SetSender(accounts.InternAddress(m.Address))
	var encoded bytes.Buffer
	require.NoError(t, transaction.EncodeRLP(&encoded))
	added, err := m.TxPoolGrpcServer.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{encoded.Bytes()}})
	require.NoError(t, err)
	require.Equal(t, []string{"success"}, added.Errors)
	beaconRoot := randomHash()
	buildParams := &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             parent.Time() + 1,
		PrevRandao:            parent.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{3},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &beaconRoot,
		SlotNumber:            syntheticSlotNumber(parent),
	}
	oracleID, err := assemblePayloadOptimizerBlock(ctx, m.ExecModule, buildParams)
	require.NoError(t, err)
	oracle := collectPayloadOptimizerResult(t, ctx, m.ExecModule, oracleID.PayloadID)
	m.ExecModule.DiscardAssembledBlock(oracleID.PayloadID)
	require.Empty(t, oracle.Block.Block.Transactions())

	buildCtx, err := payloadoptimizer.NewBuildContext(buildParams, [4]byte{0x07}, nil, parent.GasLimit())
	require.NoError(t, err)
	session, err := payloadoptimizer.New(m.ExecModule).Open(ctx, buildCtx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{transaction})
	require.NoError(t, err)
	applyCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	candidate, err := session.Apply(applyCtx, update)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	actual := candidate.Block()

	require.Empty(t, actual.Block.Transactions())
	require.Equal(t, oracle.Block.Block.Hash(), actual.Block.Hash())
	require.Equal(t, oracle.Block.Receipts, actual.Receipts)
}

func TestPayloadOptimizerBoundsStoppedRetainedProviderAcrossIncompleteFilteredBatches(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	parent := chainPack.TopBlock
	transaction, err := types.SignTx(
		types.NewTransaction(1, common.Address{1}, uint256.NewInt(1), params.TxGas, uint256.NewInt(parent.BaseFee().Uint64()+1), nil),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID),
		m.Key,
	)
	require.NoError(t, err)
	transaction.SetSender(accounts.InternAddress(m.Address))
	provider := &stopBoundedRetainedTxnProvider{transaction: transaction, sawDeadline: make(chan struct{})}
	beaconRoot := randomHash()
	buildParams := &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             parent.Time() + 1,
		PrevRandao:            parent.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{3},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &beaconRoot,
		SlotNumber:            syntheticSlotNumber(parent),
		CustomTxnProvider:     provider,
	}
	assembled, err := assemblePayloadOptimizerBlock(ctx, m.ExecModule, buildParams)
	require.NoError(t, err)
	t.Cleanup(func() { m.ExecModule.DiscardAssembledBlock(assembled.PayloadID) })

	getCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	result, err := m.ExecModule.GetAssembledBlock(getCtx, assembled.PayloadID)
	require.NoError(t, err)
	require.NotNil(t, result.Block)
	require.Empty(t, result.Block.Block.Transactions())
	require.Greater(t, provider.calls.Load(), uint64(1))
	select {
	case <-provider.sawDeadline:
	default:
		t.Fatal("retained provider never received the stopped-build deadline")
	}
}

func TestPayloadOptimizerStopsAfterAssemblerRejectsCompletedRetainedPass(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	parent := chainPack.TopBlock
	transaction, err := types.SignTx(
		types.NewTransaction(0, common.Address{1}, uint256.NewInt(1), 1, uint256.NewInt(parent.BaseFee().Uint64()+1), nil),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID),
		m.Key,
	)
	require.NoError(t, err)
	transaction.SetSender(accounts.InternAddress(m.Address))
	beaconRoot := randomHash()
	buildParams := &builder.Parameters{
		ParentHash:            parent.Hash(),
		Timestamp:             parent.Time() + 1,
		PrevRandao:            parent.Header().MixDigest,
		SuggestedFeeRecipient: common.Address{3},
		Withdrawals:           make([]*types.Withdrawal, 0),
		ParentBeaconBlockRoot: &beaconRoot,
		SlotNumber:            syntheticSlotNumber(parent),
	}
	oracleParams := buildParams.Copy()
	oracleParams.CustomTxnProvider = &oneBatchTxnProvider{txns: types.Transactions{transaction}}
	oracleID, err := assemblePayloadOptimizerBlock(ctx, m.ExecModule, oracleParams)
	require.NoError(t, err)
	oracle := collectPayloadOptimizerResult(t, ctx, m.ExecModule, oracleID.PayloadID)
	m.ExecModule.DiscardAssembledBlock(oracleID.PayloadID)
	require.Empty(t, oracle.Block.Block.Transactions())
	retainedProvider := &countedRetainedTxnProvider{transaction: transaction}
	retainedParams := buildParams.Copy()
	retainedParams.CustomTxnProvider = retainedProvider
	retainedID, err := assemblePayloadOptimizerBlock(ctx, m.ExecModule, retainedParams)
	require.NoError(t, err)
	retained := collectPayloadOptimizerResult(t, ctx, m.ExecModule, retainedID.PayloadID)
	m.ExecModule.DiscardAssembledBlock(retainedID.PayloadID)
	require.Empty(t, retained.Block.Block.Transactions())
	require.Equal(t, uint64(2), retainedProvider.calls.Load())

	buildCtx, err := payloadoptimizer.NewBuildContext(buildParams, [4]byte{0x07}, nil, parent.GasLimit())
	require.NoError(t, err)
	session, err := payloadoptimizer.New(m.ExecModule).Open(ctx, buildCtx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	update, err := payloadoptimizer.NewOrderflowUpdate(types.Transactions{transaction})
	require.NoError(t, err)
	applyCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	candidate, err := session.Apply(applyCtx, update)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	actual := candidate.Block()

	require.Empty(t, actual.Block.Transactions())
	require.Equal(t, oracle.Block.Block.Hash(), actual.Block.Hash())
	require.Equal(t, oracle.Block.Receipts, actual.Receipts)
}

func transactionHashes(transactions types.Transactions) []common.Hash {
	hashes := make([]common.Hash, len(transactions))
	for i, transaction := range transactions {
		hashes[i] = transaction.Hash()
	}
	return hashes
}

func requireHeadersEqual(t *testing.T, expected, actual *types.Header) {
	t.Helper()
	if len(expected.Extra) == 0 && len(actual.Extra) == 0 {
		expected.Extra = []byte{}
		actual.Extra = []byte{}
	}
	require.Equal(t, expected, actual)
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

func assemblePayloadOptimizerBlock(ctx context.Context, module *execmodule.ExecModule, params *builder.Parameters) (execmodule.AssembleBlockResult, error) {
	retryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		result, err := module.AssembleBlock(retryCtx, params)
		if err != nil || !result.Busy {
			return result, err
		}
		select {
		case <-retryCtx.Done():
			return execmodule.AssembleBlockResult{}, retryCtx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}
