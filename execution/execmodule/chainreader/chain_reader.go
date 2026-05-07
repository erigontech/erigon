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

package chainreader

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

type ChainReaderWriterEth1 struct {
	cfg             *chain.Config
	executionModule execmodule.ExecutionModule

	fcuTimeout time.Duration
}

func NewChainReaderEth1(cfg *chain.Config, executionModule execmodule.ExecutionModule, fcuTimeout time.Duration) ChainReaderWriterEth1 {
	return ChainReaderWriterEth1{
		cfg:             cfg,
		executionModule: executionModule,
		fcuTimeout:      fcuTimeout,
	}
}

func (c ChainReaderWriterEth1) Config() *chain.Config {
	return c.cfg
}

func (c ChainReaderWriterEth1) CurrentHeader(ctx context.Context) *types.Header {
	h, err := c.executionModule.CurrentHeader(ctx)
	if err != nil {
		log.Warn("[engine] CurrentHeader", "err", err)
		return nil
	}
	return h
}

func (c ChainReaderWriterEth1) GetHeader(ctx context.Context, hash common.Hash, number uint64) *types.Header {
	h, err := c.executionModule.GetHeader(ctx, &hash, &number)
	if err != nil {
		log.Warn("[engine] GetHeader", "err", err)
		return nil
	}
	return h
}

func (c ChainReaderWriterEth1) GetBlockByHash(ctx context.Context, hash common.Hash) *types.Block {
	header := c.GetHeaderByHash(ctx, hash)
	if header == nil {
		return nil
	}

	number := header.Number.Uint64()
	body, err := c.executionModule.GetBody(ctx, &hash, &number)
	if err != nil {
		log.Warn("[engine] GetBlockByHash", "err", err)
		return nil
	}
	if body == nil {
		return nil
	}
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		log.Warn("[engine] GetBlockByHash", "err", err)
		return nil
	}
	return types.NewBlock(header, txs, nil, nil, body.Withdrawals)
}

func (c ChainReaderWriterEth1) GetBlockByNumber(ctx context.Context, number uint64) *types.Block {
	header := c.GetHeaderByNumber(ctx, number)
	if header == nil {
		return nil
	}

	body, err := c.executionModule.GetBody(ctx, nil, &number)
	if err != nil {
		log.Warn("[engine] GetBlockByNumber", "err", err)
		return nil
	}
	if body == nil {
		return nil
	}
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		log.Warn("[engine] GetBlockByNumber", "err", err)
		return nil
	}
	return types.NewBlock(header, txs, nil, nil, body.Withdrawals)
}

func (c ChainReaderWriterEth1) GetHeaderByHash(ctx context.Context, hash common.Hash) *types.Header {
	h, err := c.executionModule.GetHeader(ctx, &hash, nil)
	if err != nil {
		log.Warn("[engine] GetHeaderByHash", "err", err)
		return nil
	}
	return h
}

func (c ChainReaderWriterEth1) GetHeaderByNumber(ctx context.Context, number uint64) *types.Header {
	h, err := c.executionModule.GetHeader(ctx, nil, &number)
	if err != nil {
		log.Warn("[engine] GetHeaderByNumber", "err", err)
		return nil
	}
	return h
}

func (c ChainReaderWriterEth1) GetTd(ctx context.Context, hash common.Hash, number uint64) *big.Int {
	td, err := c.executionModule.GetTD(ctx, &hash, &number)
	if err != nil {
		log.Warn("[engine] GetTd", "err", err)
		return nil
	}
	return td
}

func (c ChainReaderWriterEth1) GetBodiesByHashes(ctx context.Context, hashes []common.Hash) ([]*types.RawBody, error) {
	return c.executionModule.GetBodiesByHashes(ctx, hashes)
}

func (c ChainReaderWriterEth1) GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error) {
	return c.executionModule.GetBodiesByRange(ctx, start, count)
}

func (c ChainReaderWriterEth1) GetPayloadBodiesByHash(ctx context.Context, hashes []common.Hash) ([]*engine_types.ExecutionPayloadBodyV2, error) {
	bodies, err := c.executionModule.GetPayloadBodiesByHash(ctx, hashes)
	if err != nil {
		return nil, err
	}
	return convertPayloadBodies(bodies), nil
}

func (c ChainReaderWriterEth1) GetPayloadBodiesByRange(ctx context.Context, start, count uint64) ([]*engine_types.ExecutionPayloadBodyV2, error) {
	bodies, err := c.executionModule.GetPayloadBodiesByRange(ctx, start, count)
	if err != nil {
		return nil, err
	}
	return convertPayloadBodies(bodies), nil
}

func convertPayloadBodies(bodies []*execmodule.PayloadBody) []*engine_types.ExecutionPayloadBodyV2 {
	result := make([]*engine_types.ExecutionPayloadBodyV2, len(bodies))
	for i, body := range bodies {
		if body == nil {
			continue
		}
		txs := make([]hexutil.Bytes, len(body.Transactions))
		for j, tx := range body.Transactions {
			txs[j] = tx
		}
		result[i] = &engine_types.ExecutionPayloadBodyV2{
			Transactions:    txs,
			Withdrawals:     body.Withdrawals,
			BlockAccessList: body.BlockAccessList,
		}
	}
	return result
}

func (c ChainReaderWriterEth1) Ready(ctx context.Context) (bool, error) {
	return c.executionModule.Ready(ctx)
}

func (c ChainReaderWriterEth1) HeaderNumber(ctx context.Context, hash common.Hash) (*uint64, error) {
	return c.executionModule.GetHeaderHashNumber(ctx, hash)
}

func (c ChainReaderWriterEth1) IsCanonicalHash(ctx context.Context, hash common.Hash) (bool, error) {
	return c.executionModule.IsCanonicalHash(ctx, hash)
}

func (c ChainReaderWriterEth1) FrozenBlocks(ctx context.Context) (uint64, bool) {
	frozen, hasGap, err := c.executionModule.FrozenBlocks(ctx)
	if err != nil {
		panic(err)
	}
	return frozen, hasGap
}

func (c ChainReaderWriterEth1) InsertBlocksAndWait(ctx context.Context, blocks []*types.Block) error {
	return c.InsertBlocksAndWaitWithAccessLists(ctx, blocks, nil)
}

// InsertBlocksAndWaitWithAccessLists inserts blocks and waits for confirmation.
// accessLists maps block hash to its RLP-encoded block access list bytes (nil if not present).
func (c ChainReaderWriterEth1) InsertBlocksAndWaitWithAccessLists(ctx context.Context, blocks []*types.Block, accessLists map[common.Hash][]byte) error {
	rawBlocks := blocksToRaw(blocks, accessLists)
	for {
		status, err := c.executionModule.InsertBlocks(ctx, rawBlocks)
		if err != nil {
			return err
		}
		if status != execmodule.ExecutionStatusBusy {
			if status != execmodule.ExecutionStatusSuccess {
				return fmt.Errorf("InsertBlocksAndWait: executionModule.InsertBlocks ExecutionStatus = %s", status)
			}
			return nil
		}
		const retryDelay = 100 * time.Millisecond
		select {
		case <-time.After(retryDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c ChainReaderWriterEth1) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	return c.InsertBlocksWithAccessLists(ctx, blocks, nil)
}

// InsertBlocksWithAccessLists inserts blocks without waiting for confirmation.
// accessLists maps block hash to its RLP-encoded block access list bytes (nil if not present).
func (c ChainReaderWriterEth1) InsertBlocksWithAccessLists(ctx context.Context, blocks []*types.Block, accessLists map[common.Hash][]byte) error {
	rawBlocks := blocksToRaw(blocks, accessLists)
	status, err := c.executionModule.InsertBlocks(ctx, rawBlocks)
	if err != nil {
		return err
	}
	if status == execmodule.ExecutionStatusBusy {
		return context.DeadlineExceeded
	}
	if status != execmodule.ExecutionStatusSuccess {
		return fmt.Errorf("InsertBlocks: invalid code received from execution module: %s", status)
	}
	return nil
}

func (c ChainReaderWriterEth1) InsertBlockAndWait(ctx context.Context, block *types.Block) error {
	return c.InsertBlocksAndWait(ctx, []*types.Block{block})
}

func blocksToRaw(blocks []*types.Block, accessLists map[common.Hash][]byte) []*types.RawBlock {
	raw := make([]*types.RawBlock, len(blocks))
	for i, b := range blocks {
		rawBody := b.RawBody()
		rb := &types.RawBlock{Header: b.Header(), Body: rawBody}
		if accessLists != nil {
			if bal, ok := accessLists[b.Hash()]; ok {
				rb.BlockAccessList = bal
			}
		}
		raw[i] = rb
	}
	return raw
}

func (c ChainReaderWriterEth1) ValidateChain(ctx context.Context, hash common.Hash, number uint64) (execmodule.ExecutionStatus, *string, common.Hash, error) {
	result, err := c.executionModule.ValidateChain(ctx, hash, number)
	if err != nil {
		return 0, nil, common.Hash{}, err
	}
	var validationError *string
	if len(result.ValidationError) > 0 {
		validationError = &result.ValidationError
	}
	return result.ValidationStatus, validationError, result.LatestValidHash, nil
}

func (c ChainReaderWriterEth1) UpdateForkChoice(ctx context.Context, headHash, safeHash, finalizeHash common.Hash, timeoutOverride ...uint64) (execmodule.ExecutionStatus, *string, common.Hash, error) {
	// Apply timeout via context so the native UpdateForkChoice returns Busy on deadline exceeded.
	timeout := c.fcuTimeout
	if len(timeoutOverride) > 0 {
		timeout = time.Duration(timeoutOverride[0]) * time.Millisecond
	}
	callCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	result, err := c.executionModule.UpdateForkChoice(callCtx, headHash, safeHash, finalizeHash)
	if err != nil {
		return 0, nil, common.Hash{}, err
	}
	var validationError *string
	if len(result.ValidationError) > 0 {
		validationError = &result.ValidationError
	}
	return result.Status, validationError, result.LatestValidHash, nil
}

func (c ChainReaderWriterEth1) GetForkChoice(ctx context.Context) (headHash, finalizedHash, safeHash common.Hash, err error) {
	state, err := c.executionModule.GetForkChoice(ctx)
	if err != nil {
		log.Warn("[engine] GetForkChoice", "err", err)
		return
	}
	return state.HeadHash, state.FinalizedHash, state.SafeHash, nil
}

func (c ChainReaderWriterEth1) HasBlock(ctx context.Context, hash common.Hash) (bool, error) {
	return c.executionModule.HasBlock(ctx, &hash, nil)
}

func (c ChainReaderWriterEth1) AssembleBlock(baseHash common.Hash, attributes *engine_types.PayloadAttributes) (id uint64, err error) {
	params := &builder.Parameters{
		ParentHash:            baseHash,
		Timestamp:             uint64(attributes.Timestamp),
		PrevRandao:            attributes.PrevRandao,
		SuggestedFeeRecipient: attributes.SuggestedFeeRecipient,
		Withdrawals:           attributes.Withdrawals,
		SlotNumber:            (*uint64)(attributes.SlotNumber),
		ParentBeaconBlockRoot: attributes.ParentBeaconBlockRoot,
	}
	result, err := c.executionModule.AssembleBlock(context.Background(), params)
	if err != nil {
		return 0, err
	}
	if result.Busy {
		return 0, errors.New("execution data is still syncing")
	}
	return result.PayloadID, nil
}

func (c ChainReaderWriterEth1) GetAssembledBlock(id uint64) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
	result, err := c.executionModule.GetAssembledBlock(context.Background(), id)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if result.Busy {
		return nil, nil, nil, nil, errors.New("execution data is still syncing")
	}
	if result.Block == nil {
		return nil, nil, nil, nil, nil
	}

	br := result.Block
	block := br.Block
	header := block.Header()

	// Encode transactions
	encodedTxs, err := types.MarshalTransactionsBinary(block.Transactions())
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Build CL Eth1Block
	extraData := solid.NewExtraData()
	extraData.SetBytes(header.Extra)
	blockHash := block.Hash()

	// BaseFeePerGas in cltypes.Eth1Block is stored as little-endian bytes in a common.Hash.
	var baseFeeLE common.Hash
	if header.BaseFee != nil {
		be := header.BaseFee.Bytes32() // big-endian [32]byte
		copy(baseFeeLE[:], be[:])
		utils.ReverseBytes(&baseFeeLE) // convert to little-endian
	}

	eth1Block := &cltypes.Eth1Block{
		ParentHash:    header.ParentHash,
		FeeRecipient:  header.Coinbase,
		StateRoot:     header.Root,
		ReceiptsRoot:  header.ReceiptHash,
		LogsBloom:     header.Bloom,
		BlockNumber:   header.Number.Uint64(),
		GasLimit:      header.GasLimit,
		GasUsed:       header.GasUsed,
		Time:          header.Time,
		Extra:         extraData,
		PrevRandao:    header.MixDigest,
		Transactions:  solid.NewTransactionsSSZFromTransactions(encodedTxs),
		BlockHash:     blockHash,
		BaseFeePerGas: baseFeeLE,
	}
	if header.ExcessBlobGas != nil {
		eth1Block.ExcessBlobGas = *header.ExcessBlobGas
	}
	if header.BlobGasUsed != nil {
		eth1Block.BlobGasUsed = *header.BlobGasUsed
	}

	// Withdrawals
	withdrawals := solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
	for _, w := range block.Withdrawals() {
		withdrawals.Append(&cltypes.Withdrawal{
			Amount:    w.Amount,
			Address:   w.Address,
			Index:     w.Index,
			Validator: w.Validator,
		})
	}
	eth1Block.Withdrawals = withdrawals

	// Blob bundle
	blobsBundle, err := engine_types.BlobsBundleFromTransactions(block.Transactions())
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Requests bundle
	var requestsBundle *typesproto.RequestsBundle
	if br.Requests != nil {
		requestsBundle = &typesproto.RequestsBundle{}
		for _, r := range br.Requests {
			requestsBundle.Requests = append(requestsBundle.Requests, r.Encode())
		}
	}

	blockValue := result.BlockValue.ToBig()

	return eth1Block, blobsBundle, requestsBundle, blockValue, nil
}
