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
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/core/types"
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
	return types.NewBlock(header, txs, nil, nil, body.Withdrawals)
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
	return types.NewBlock(header, txs, nil, nil, body.Withdrawals)
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

const retryTimeout = 10 * time.Millisecond

func (c ChainReaderWriterEth1) InsertBlocksAndWait(ctx context.Context, blocks []*types.Block) error {
	request := &execution.InsertBlocksRequest{
		Blocks: eth1_utils.ConvertBlocksToRPC(blocks),
	}
	response, err := c.executionModule.InsertBlocks(ctx, request)
	if err != nil {
		return err
	}
	retryInterval := time.NewTicker(retryTimeout)
	defer retryInterval.Stop()

	for response.Result == execution.ExecutionStatus_Busy {
		select {
		case <-retryInterval.C:
			response, err = c.executionModule.InsertBlocks(ctx, request)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if response.Result != execution.ExecutionStatus_Success {
		return fmt.Errorf("insertHeadersAndWait: invalid code recieved from execution module: %s", response.Result.String())
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
	request := &execution.InsertBlocksRequest{
		Blocks: eth1_utils.ConvertBlocksToRPC(blocks),
	}

	response, err := c.executionModule.InsertBlocks(ctx, request)
	if err != nil {
		return err
	}
	retryInterval := time.NewTicker(retryTimeout)
	defer retryInterval.Stop()
	for response.Result == execution.ExecutionStatus_Busy {
		select {
		case <-retryInterval.C:
			response, err = c.executionModule.InsertBlocks(ctx, request)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return context.Canceled
		}
	}
	if response.Result != execution.ExecutionStatus_Success {
		return fmt.Errorf("insertHeadersAndWait: invalid code recieved from execution module: %s", response.Result.String())
	}
	return c.InsertBlocksAndWait(ctx, []*types.Block{block})
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
