package eth1_chain_reader

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	execution "github.com/ledgerwatch/erigon-lib/gointerfaces/executionproto"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/typesproto"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_utils"
)

type ChainReaderWriterEth1 struct {
	cfg             *chain.Config
	executionModule execution.ExecutionClient

	fcuTimeoutMillis uint64
}

func NewChainReaderEth1(cfg *chain.Config, executionModule execution.ExecutionClient, fcuTimeoutMillis uint64) ChainReaderWriterEth1 {
	return ChainReaderWriterEth1{
		cfg:              cfg,
		executionModule:  executionModule,
		fcuTimeoutMillis: fcuTimeoutMillis,
	}
}

func (c ChainReaderWriterEth1) Config() *chain.Config {
	return c.cfg
}

func (c ChainReaderWriterEth1) CurrentHeader(ctx context.Context) *types.Header {
	resp, err := c.executionModule.CurrentHeader(ctx, &emptypb.Empty{})
	if err != nil {
		log.Warn("[engine] CurrentHeader", "err", err)
		return nil
	}
	if resp == nil || resp.Header == nil {
		return nil
	}
	ret, err := eth1_utils.HeaderRpcToHeader(resp.Header)
	if err != nil {
		log.Warn("[engine] CurrentHeader", "err", err)
		return nil
	}
	return ret
}

func (c ChainReaderWriterEth1) GetHeader(ctx context.Context, hash libcommon.Hash, number uint64) *types.Header {
	resp, err := c.executionModule.GetHeader(ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   gointerfaces.ConvertHashToH256(hash),
	})
	if err != nil {
		log.Warn("[engine] GetHeader", "err", err)
		return nil
	}
	if resp == nil || resp.Header == nil {
		return nil
	}
	ret, err := eth1_utils.HeaderRpcToHeader(resp.Header)
	if err != nil {
		log.Warn("[engine] GetHeader", "err", err)
		return nil
	}
	return ret
}

func (c ChainReaderWriterEth1) GetBlockByHash(ctx context.Context, hash libcommon.Hash) *types.Block {
	header := c.GetHeaderByHash(ctx, hash)
	if header == nil {
		return nil
	}

	number := header.Number.Uint64()
	resp, err := c.executionModule.GetBody(ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   gointerfaces.ConvertHashToH256(hash),
	})
	if err != nil {
		log.Warn("[engine] GetBlockByHash", "err", err)
		return nil
	}
	if resp == nil || resp.Body == nil {
		return nil
	}
	body, err := eth1_utils.ConvertRawBlockBodyFromRpc(resp.Body)
	if err != nil {
		log.Warn("[engine] GetBlockByHash", "err", err)
		return nil
	}
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		log.Warn("[engine] GetBlockByHash", "err", err)
		return nil
	}
	return types.NewBlock(header, txs, nil, nil, body.Withdrawals, body.Requests)
}

func (c ChainReaderWriterEth1) GetBlockByNumber(ctx context.Context, number uint64) *types.Block {
	header := c.GetHeaderByNumber(ctx, number)
	if header == nil {
		return nil
	}

	resp, err := c.executionModule.GetBody(ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
	})
	if err != nil {
		log.Warn("[engine] GetBlockByNumber", "err", err)
		return nil
	}
	if resp == nil || resp.Body == nil {
		return nil
	}
	body, err := eth1_utils.ConvertRawBlockBodyFromRpc(resp.Body)
	if err != nil {
		log.Warn("[engine] GetBlockByNumber", "err", err)
		return nil
	}
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		log.Warn("[engine] GetBlockByNumber", "err", err)
		return nil
	}
	return types.NewBlock(header, txs, nil, nil, body.Withdrawals, body.Requests)
}

func (c ChainReaderWriterEth1) GetHeaderByHash(ctx context.Context, hash libcommon.Hash) *types.Header {
	resp, err := c.executionModule.GetHeader(ctx, &execution.GetSegmentRequest{
		BlockNumber: nil,
		BlockHash:   gointerfaces.ConvertHashToH256(hash),
	})
	if err != nil {
		log.Warn("[engine] GetHeaderByHash", "err", err)
		return nil
	}
	if resp == nil || resp.Header == nil {
		return nil
	}
	ret, err := eth1_utils.HeaderRpcToHeader(resp.Header)
	if err != nil {
		log.Warn("[engine] GetHeaderByHash", "err", err)
		return nil
	}
	return ret
}

func (c ChainReaderWriterEth1) GetHeaderByNumber(ctx context.Context, number uint64) *types.Header {
	resp, err := c.executionModule.GetHeader(ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   nil,
	})
	if err != nil {
		log.Warn("[engine] GetHeaderByNumber", "err", err)
		return nil
	}
	if resp == nil || resp.Header == nil {
		return nil
	}
	ret, err := eth1_utils.HeaderRpcToHeader(resp.Header)
	if err != nil {
		log.Warn("[engine] GetHeaderByNumber", "err", err)
		return nil
	}
	return ret
}

func (c ChainReaderWriterEth1) GetTd(ctx context.Context, hash libcommon.Hash, number uint64) *big.Int {
	resp, err := c.executionModule.GetTD(ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   gointerfaces.ConvertHashToH256(hash),
	})
	if err != nil {
		log.Warn("[engine] GetTd", "err", err)
		return nil
	}
	if resp == nil || resp.Td == nil {
		return nil
	}
	return eth1_utils.ConvertBigIntFromRpc(resp.Td)
}

func (c ChainReaderWriterEth1) GetBodiesByHashes(ctx context.Context, hashes []libcommon.Hash) ([]*types.RawBody, error) {
	grpcHashes := make([]*types2.H256, len(hashes))
	for i := range grpcHashes {
		grpcHashes[i] = gointerfaces.ConvertHashToH256(hashes[i])
	}
	resp, err := c.executionModule.GetBodiesByHashes(ctx, &execution.GetBodiesByHashesRequest{
		Hashes: grpcHashes,
	})
	if err != nil {
		return nil, err
	}
	ret := make([]*types.RawBody, len(resp.Bodies))
	for i := range ret {
		ret[i], err = eth1_utils.ConvertRawBlockBodyFromRpc(resp.Bodies[i])
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (c ChainReaderWriterEth1) GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error) {
	resp, err := c.executionModule.GetBodiesByRange(ctx, &execution.GetBodiesByRangeRequest{
		Start: start,
		Count: count,
	})
	if err != nil {
		return nil, err
	}
	ret := make([]*types.RawBody, len(resp.Bodies))
	for i := range ret {
		ret[i], err = eth1_utils.ConvertRawBlockBodyFromRpc(resp.Bodies[i])
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (c ChainReaderWriterEth1) Ready(ctx context.Context) (bool, error) {
	resp, err := c.executionModule.Ready(ctx, &emptypb.Empty{})
	if err != nil {
		return false, err
	}
	return resp.Ready, nil
}

func (c ChainReaderWriterEth1) HeaderNumber(ctx context.Context, hash libcommon.Hash) (*uint64, error) {
	resp, err := c.executionModule.GetHeaderHashNumber(ctx, gointerfaces.ConvertHashToH256(hash))
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	return resp.BlockNumber, nil
}

func (c ChainReaderWriterEth1) IsCanonicalHash(ctx context.Context, hash libcommon.Hash) (bool, error) {
	resp, err := c.executionModule.IsCanonicalHash(ctx, gointerfaces.ConvertHashToH256(hash))
	if err != nil {
		return false, err
	}
	if resp == nil {
		return false, nil
	}
	return resp.Canonical, nil
}

func (c ChainReaderWriterEth1) FrozenBlocks(ctx context.Context) uint64 {
	ret, err := c.executionModule.FrozenBlocks(ctx, &emptypb.Empty{})
	if err != nil {
		panic(err)
	}
	return ret.FrozenBlocks
}

func (c ChainReaderWriterEth1) InsertBlocksAndWait(ctx context.Context, blocks []*types.Block) error {
	request := &execution.InsertBlocksRequest{
		Blocks: eth1_utils.ConvertBlocksToRPC(blocks),
	}
	response, err := c.executionModule.InsertBlocks(ctx, request)
	if err != nil {
		return err
	}

	for response.Result == execution.ExecutionStatus_Busy {
		const retryDelay = 100 * time.Millisecond
		select {
		case <-time.After(retryDelay):
		case <-ctx.Done():
			return ctx.Err()
		}

		response, err = c.executionModule.InsertBlocks(ctx, request)
		if err != nil {
			return err
		}
	}
	if response.Result != execution.ExecutionStatus_Success {
		return fmt.Errorf("InsertBlocksAndWait: executionModule.InsertBlocks ExecutionStatus = %s", response.Result.String())
	}
	return nil
}

func (c ChainReaderWriterEth1) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	request := &execution.InsertBlocksRequest{
		Blocks: eth1_utils.ConvertBlocksToRPC(blocks),
	}
	response, err := c.executionModule.InsertBlocks(ctx, request)
	if err != nil {
		return err
	}

	if response.Result == execution.ExecutionStatus_Busy {
		return context.DeadlineExceeded
	}
	if response.Result != execution.ExecutionStatus_Success {
		return fmt.Errorf("insertHeadersAndWait: invalid code recieved from execution module: %s", response.Result.String())
	}
	return nil
}

func (c ChainReaderWriterEth1) InsertBlockAndWait(ctx context.Context, block *types.Block) error {
	blocks := []*types.Block{block}
	return c.InsertBlocksAndWait(ctx, blocks)
}

func (c ChainReaderWriterEth1) ValidateChain(ctx context.Context, hash libcommon.Hash, number uint64) (execution.ExecutionStatus, *string, libcommon.Hash, error) {
	resp, err := c.executionModule.ValidateChain(ctx, &execution.ValidationRequest{
		Hash:   gointerfaces.ConvertHashToH256(hash),
		Number: number,
	})
	if err != nil {
		return 0, nil, libcommon.Hash{}, err
	}
	var validationError *string
	if len(resp.ValidationError) > 0 {
		validationError = &resp.ValidationError
	}
	return resp.ValidationStatus, validationError, gointerfaces.ConvertH256ToHash(resp.LatestValidHash), err
}

func (c ChainReaderWriterEth1) UpdateForkChoice(ctx context.Context, headHash, safeHash, finalizeHash libcommon.Hash) (execution.ExecutionStatus, *string, libcommon.Hash, error) {
	resp, err := c.executionModule.UpdateForkChoice(ctx, &execution.ForkChoice{
		HeadBlockHash:      gointerfaces.ConvertHashToH256(headHash),
		SafeBlockHash:      gointerfaces.ConvertHashToH256(safeHash),
		FinalizedBlockHash: gointerfaces.ConvertHashToH256(finalizeHash),
		Timeout:            c.fcuTimeoutMillis,
	})
	if err != nil {
		return 0, nil, libcommon.Hash{}, err
	}
	var validationError *string
	if len(resp.ValidationError) > 0 {
		validationError = &resp.ValidationError
	}
	return resp.Status, validationError, gointerfaces.ConvertH256ToHash(resp.LatestValidHash), err
}

func (c ChainReaderWriterEth1) GetForkChoice(ctx context.Context) (headHash, finalizedHash, safeHash libcommon.Hash, err error) {
	var resp *execution.ForkChoice
	resp, err = c.executionModule.GetForkChoice(ctx, &emptypb.Empty{})
	if err != nil {
		log.Warn("[engine] GetForkChoice", "err", err)
		return
	}
	return gointerfaces.ConvertH256ToHash(resp.HeadBlockHash), gointerfaces.ConvertH256ToHash(resp.FinalizedBlockHash),
		gointerfaces.ConvertH256ToHash(resp.SafeBlockHash), nil
}

func (c ChainReaderWriterEth1) HasBlock(ctx context.Context, hash libcommon.Hash) (bool, error) {
	resp, err := c.executionModule.HasBlock(ctx, &execution.GetSegmentRequest{
		BlockHash: gointerfaces.ConvertHashToH256(hash),
	})
	if err != nil {
		return false, err
	}
	return resp.HasBlock, nil
}

func (c ChainReaderWriterEth1) AssembleBlock(baseHash libcommon.Hash, attributes *engine_types.PayloadAttributes) (id uint64, err error) {
	request := &execution.AssembleBlockRequest{
		Timestamp:             uint64(attributes.Timestamp),
		PrevRandao:            gointerfaces.ConvertHashToH256(attributes.PrevRandao),
		SuggestedFeeRecipient: gointerfaces.ConvertAddressToH160(attributes.SuggestedFeeRecipient),
		Withdrawals:           eth1_utils.ConvertWithdrawalsToRpc(attributes.Withdrawals),
		ParentHash:            gointerfaces.ConvertHashToH256(baseHash),
	}
	if attributes.ParentBeaconBlockRoot != nil {
		request.ParentBeaconBlockRoot = gointerfaces.ConvertHashToH256(*attributes.ParentBeaconBlockRoot)
	}
	resp, err := c.executionModule.AssembleBlock(context.Background(), request)
	if err != nil {
		return 0, err
	}
	if resp.Busy {
		return 0, fmt.Errorf("execution data is still syncing")
	}
	return resp.Id, nil
}

func (c ChainReaderWriterEth1) GetAssembledBlock(id uint64) (*cltypes.Eth1Block, *engine_types.BlobsBundleV1, *big.Int, error) {
	resp, err := c.executionModule.GetAssembledBlock(context.Background(), &execution.GetAssembledBlockRequest{
		Id: id,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if resp.Busy {
		return nil, nil, nil, fmt.Errorf("execution data is still syncing")
	}
	if resp.Data == nil {
		return nil, nil, nil, nil
	}

	bundle := engine_types.ConvertBlobsFromRpc(resp.Data.BlobsBundle)
	blockValue := gointerfaces.ConvertH256ToUint256Int(resp.Data.BlockValue).ToBig()
	payloadRpc := resp.Data.ExecutionPayload

	extraData := solid.NewExtraData()
	extraData.SetBytes(payloadRpc.ExtraData)
	blockHash := gointerfaces.ConvertH256ToHash(payloadRpc.BlockHash)
	block := &cltypes.Eth1Block{
		ParentHash:    gointerfaces.ConvertH256ToHash(payloadRpc.ParentHash),
		FeeRecipient:  gointerfaces.ConvertH160toAddress(payloadRpc.Coinbase),
		StateRoot:     gointerfaces.ConvertH256ToHash(payloadRpc.StateRoot),
		ReceiptsRoot:  gointerfaces.ConvertH256ToHash(payloadRpc.ReceiptRoot),
		LogsBloom:     gointerfaces.ConvertH2048ToBloom(payloadRpc.LogsBloom),
		BlockNumber:   payloadRpc.BlockNumber,
		GasLimit:      payloadRpc.GasLimit,
		GasUsed:       payloadRpc.GasUsed,
		Time:          payloadRpc.Timestamp,
		Extra:         extraData,
		PrevRandao:    gointerfaces.ConvertH256ToHash(payloadRpc.PrevRandao),
		Transactions:  solid.NewTransactionsSSZFromTransactions(payloadRpc.Transactions),
		BlockHash:     blockHash,
		BaseFeePerGas: gointerfaces.ConvertH256ToHash(payloadRpc.BaseFeePerGas),
	}
	copy(block.BaseFeePerGas[:], utils.ReverseOfByteSlice(block.BaseFeePerGas[:])) // reverse the byte slice
	if payloadRpc.ExcessBlobGas != nil {
		block.ExcessBlobGas = *payloadRpc.ExcessBlobGas
	}
	if payloadRpc.BlobGasUsed != nil {
		block.BlobGasUsed = *payloadRpc.BlobGasUsed
	}

	// change the limit later
	withdrawals := solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
	for _, w := range payloadRpc.Withdrawals {
		withdrawals.Append(&cltypes.Withdrawal{
			Amount:    w.Amount,
			Address:   gointerfaces.ConvertH160toAddress(w.Address),
			Index:     w.Index,
			Validator: w.ValidatorIndex,
		})
	}
	block.Withdrawals = withdrawals
	return block, bundle, blockValue, nil
}
