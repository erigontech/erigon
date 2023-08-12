package eth1_chain_reader

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_utils"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ChainReaderWriterEth1 struct {
	ctx             context.Context
	cfg             *chain.Config
	executionModule execution.ExecutionClient

	fcuTimoutMillis uint64
}

func NewChainReaderEth1(ctx context.Context, cfg *chain.Config, executionModule execution.ExecutionClient, fcuTimoutMillis uint64) ChainReaderWriterEth1 {
	return ChainReaderWriterEth1{
		ctx:             ctx,
		cfg:             cfg,
		executionModule: executionModule,
		fcuTimoutMillis: fcuTimoutMillis,
	}
}

func (c ChainReaderWriterEth1) Config() *chain.Config {
	return c.cfg
}

func (c ChainReaderWriterEth1) CurrentHeader() *types.Header {
	resp, err := c.executionModule.CurrentHeader(c.ctx, &emptypb.Empty{})
	if err != nil {
		log.Error("GetHeader failed", "err", err)
		return nil
	}
	if resp == nil || resp.Header == nil {
		return nil
	}
	ret, err := eth1_utils.HeaderRpcToHeader(resp.Header)
	if err != nil {
		log.Error("GetHeader decoding", "err", err)
		return nil
	}
	return ret
}

func (c ChainReaderWriterEth1) GetHeader(hash libcommon.Hash, number uint64) *types.Header {
	resp, err := c.executionModule.GetHeader(c.ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   gointerfaces.ConvertHashToH256(hash),
	})
	if err != nil {
		log.Error("GetHeader failed", "err", err)
		return nil
	}
	if resp == nil || resp.Header == nil {
		return nil
	}
	ret, err := eth1_utils.HeaderRpcToHeader(resp.Header)
	if err != nil {
		log.Error("GetHeader decoding", "err", err)
		return nil
	}
	return ret
}

func (c ChainReaderWriterEth1) GetBlockByHash(hash libcommon.Hash) *types.Block {
	header := c.GetHeaderByHash(hash)
	if header == nil {
		return nil
	}

	number := header.Number.Uint64()
	resp, err := c.executionModule.GetBody(c.ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   gointerfaces.ConvertHashToH256(hash),
	})
	if err != nil {
		log.Error("GetBlockByHash failed", "err", err)
		return nil
	}
	if resp == nil || resp.Body == nil {
		return nil
	}
	body := eth1_utils.ConvertRawBlockBodyFromRpc(resp.Body)
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		log.Error("GetBlockByHash failed", "err", err)
		return nil
	}
	return types.NewBlock(header, txs, nil, nil, body.Withdrawals)
}

func (c ChainReaderWriterEth1) GetBlockByNumber(number uint64) *types.Block {
	header := c.GetHeaderByNumber(number)
	if header == nil {
		return nil
	}

	resp, err := c.executionModule.GetBody(c.ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
	})
	if err != nil {
		log.Error("GetBlockByNumber failed", "err", err)
		return nil
	}
	if resp == nil || resp.Body == nil {
		return nil
	}
	body := eth1_utils.ConvertRawBlockBodyFromRpc(resp.Body)
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		log.Error("GetBlockByNumber failed", "err", err)
		return nil
	}
	return types.NewBlock(header, txs, nil, nil, body.Withdrawals)
}

func (c ChainReaderWriterEth1) GetHeaderByHash(hash libcommon.Hash) *types.Header {
	resp, err := c.executionModule.GetHeader(c.ctx, &execution.GetSegmentRequest{
		BlockNumber: nil,
		BlockHash:   gointerfaces.ConvertHashToH256(hash),
	})
	if err != nil {
		log.Error("GetHeaderByHash failed", "err", err)
		return nil
	}
	if resp == nil || resp.Header == nil {
		return nil
	}
	ret, err := eth1_utils.HeaderRpcToHeader(resp.Header)
	if err != nil {
		log.Error("GetHeaderByHash decoding", "err", err)
		return nil
	}
	return ret
}

func (c ChainReaderWriterEth1) GetHeaderByNumber(number uint64) *types.Header {
	resp, err := c.executionModule.GetHeader(c.ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   nil,
	})
	if err != nil {
		log.Error("GetHeaderByHash failed", "err", err)
		return nil
	}
	if resp == nil || resp.Header == nil {
		return nil
	}
	ret, err := eth1_utils.HeaderRpcToHeader(resp.Header)
	if err != nil {
		log.Error("GetHeaderByHash decoding", "err", err)
		return nil
	}
	return ret
}

func (c ChainReaderWriterEth1) GetTd(hash libcommon.Hash, number uint64) *big.Int {
	resp, err := c.executionModule.GetTD(c.ctx, &execution.GetSegmentRequest{
		BlockNumber: &number,
		BlockHash:   gointerfaces.ConvertHashToH256(hash),
	})
	if err != nil {
		log.Error("GetHeaderByHash failed", "err", err)
		return nil
	}
	if resp == nil || resp.Td == nil {
		return nil
	}
	return eth1_utils.ConvertBigIntFromRpc(resp.Td)
}

func (c ChainReaderWriterEth1) Ready() (bool, error) {
	resp, err := c.executionModule.Ready(c.ctx, &emptypb.Empty{})
	if err != nil {
		return false, err
	}
	return resp.Ready, nil
}

func (c ChainReaderWriterEth1) HeaderNumber(hash libcommon.Hash) (*uint64, error) {
	resp, err := c.executionModule.GetHeaderHashNumber(c.ctx, gointerfaces.ConvertHashToH256(hash))
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	return resp.BlockNumber, nil
}

func (c ChainReaderWriterEth1) IsCanonicalHash(hash libcommon.Hash) (bool, error) {
	resp, err := c.executionModule.IsCanonicalHash(c.ctx, gointerfaces.ConvertHashToH256(hash))
	if err != nil {
		return false, err
	}
	if resp == nil {
		return false, nil
	}
	return resp.Canonical, nil
}

func (ChainReaderWriterEth1) FrozenBlocks() uint64 {
	panic("ChainReaderEth1.FrozenBlocks not implemented")
}

const retryTimeout = 10 * time.Millisecond

func (c ChainReaderWriterEth1) InsertBlocksAndWait(blocks []*types.Block) error {
	request := &execution.InsertBlocksRequest{
		Blocks: eth1_utils.ConvertBlocksToRPC(blocks),
	}
	response, err := c.executionModule.InsertBlocks(c.ctx, request)
	if err != nil {
		return err
	}
	retryInterval := time.NewTicker(retryTimeout)
	defer retryInterval.Stop()
	for response.Result == execution.ExecutionStatus_Busy {
		select {
		case <-retryInterval.C:
			response, err = c.executionModule.InsertBlocks(c.ctx, request)
			if err != nil {
				return err
			}
		case <-c.ctx.Done():
			return context.Canceled
		}
	}
	if response.Result != execution.ExecutionStatus_Success {
		return fmt.Errorf("insertHeadersAndWait: invalid code recieved from execution module: %s", response.Result.String())
	}
	return nil
}

func (c ChainReaderWriterEth1) InsertBlockAndWait(block *types.Block) error {
	return c.InsertBlocksAndWait([]*types.Block{block})
}

func (c ChainReaderWriterEth1) ValidateChain(hash libcommon.Hash, number uint64) (execution.ExecutionStatus, libcommon.Hash, error) {
	resp, err := c.executionModule.ValidateChain(c.ctx, &execution.ValidationRequest{
		Hash:   gointerfaces.ConvertHashToH256(hash),
		Number: number,
	})
	if err != nil {
		return 0, libcommon.Hash{}, err
	}
	return resp.ValidationStatus, gointerfaces.ConvertH256ToHash(resp.LatestValidHash), err
}

func (c ChainReaderWriterEth1) UpdateForkChoice(headHash, safeHash, finalizeHash libcommon.Hash) (execution.ExecutionStatus, libcommon.Hash, error) {
	resp, err := c.executionModule.UpdateForkChoice(c.ctx, &execution.ForkChoice{
		HeadBlockHash:      gointerfaces.ConvertHashToH256(headHash),
		SafeBlockHash:      gointerfaces.ConvertHashToH256(safeHash),
		FinalizedBlockHash: gointerfaces.ConvertHashToH256(finalizeHash),
		Timeout:            c.fcuTimoutMillis,
	})
	if err != nil {
		return 0, libcommon.Hash{}, err
	}
	return resp.Status, gointerfaces.ConvertH256ToHash(resp.LatestValidHash), err
}

func (c ChainReaderWriterEth1) GetForkchoice() (headHash, finalizedHash, safeHash libcommon.Hash, err error) {
	var resp *execution.ForkChoice
	resp, err = c.executionModule.GetForkChoice(c.ctx, &emptypb.Empty{})
	if err != nil {
		log.Error("GetHeader failed", "err", err)
		return
	}
	return gointerfaces.ConvertH256ToHash(resp.HeadBlockHash), gointerfaces.ConvertH256ToHash(resp.FinalizedBlockHash),
		gointerfaces.ConvertH256ToHash(resp.SafeBlockHash), nil
}
