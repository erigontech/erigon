package execution_client

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_chain_reader.go"
)

type ExecutionClientDirect struct {
	chainRW eth1_chain_reader.ChainReaderWriterEth1
}

func NewExecutionClientDirect(chainRW eth1_chain_reader.ChainReaderWriterEth1) (*ExecutionClientDirect, error) {
	return &ExecutionClientDirect{
		chainRW: chainRW,
	}, nil
}

func (cc *ExecutionClientDirect) NewPayload(ctx context.Context, payload *cltypes.Eth1Block, beaconParentRoot *libcommon.Hash, versionedHashes []libcommon.Hash) (invalid bool, err error) {
	if payload == nil {
		return
	}

	header, err := payload.RlpHeader(beaconParentRoot)
	if err != nil {
		return true, err
	}

	body := payload.Body()
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		return true, err
	}

	if err := cc.chainRW.InsertBlockAndWait(ctx, types.NewBlockFromStorage(payload.BlockHash, header, txs, nil, body.Withdrawals)); err != nil {
		return false, err
	}

	status, _, _, err := cc.chainRW.ValidateChain(ctx, payload.BlockHash, payload.BlockNumber)
	if err != nil {
		return false, err
	}
	invalid = status == execution.ExecutionStatus_BadBlock

	return
}

func (cc *ExecutionClientDirect) ForkChoiceUpdate(ctx context.Context, finalized libcommon.Hash, head libcommon.Hash) error {
	status, _, _, err := cc.chainRW.UpdateForkChoice(ctx, head, head, finalized)
	if err != nil {
		return fmt.Errorf("execution Client RPC failed to retrieve ForkChoiceUpdate response, err: %w", err)
	}
	if status == execution.ExecutionStatus_InvalidForkchoice {
		return fmt.Errorf("forkchoice was invalid")
	}
	if status == execution.ExecutionStatus_BadBlock {
		return fmt.Errorf("bad block as forkchoice")
	}
	return nil
}

func (cc *ExecutionClientDirect) SupportInsertion() bool {
	return true
}

func (cc *ExecutionClientDirect) InsertBlocks(ctx context.Context, blocks []*types.Block, wait bool) error {
	if !wait {
		return cc.chainRW.InsertBlocksAndWait(ctx, blocks)
	}
	return cc.chainRW.InsertBlocks(ctx, blocks)
}

func (cc *ExecutionClientDirect) InsertBlock(ctx context.Context, blk *types.Block) error {
	return cc.chainRW.InsertBlockAndWait(ctx, blk)
}

func (cc *ExecutionClientDirect) IsCanonicalHash(ctx context.Context, hash libcommon.Hash) (bool, error) {
	return cc.chainRW.IsCanonicalHash(ctx, hash)
}

func (cc *ExecutionClientDirect) Ready(ctx context.Context) (bool, error) {
	return cc.chainRW.Ready(ctx)
}

// GetBodiesByRange gets block bodies in given block range
func (cc *ExecutionClientDirect) GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error) {
	return cc.chainRW.GetBodiesByRange(ctx, start, count)
}

// GetBodiesByHashes gets block bodies with given hashes
func (cc *ExecutionClientDirect) GetBodiesByHashes(ctx context.Context, hashes []libcommon.Hash) ([]*types.RawBody, error) {
	return cc.chainRW.GetBodiesByHashes(ctx, hashes)
}

func (cc *ExecutionClientDirect) FrozenBlocks(ctx context.Context) uint64 {
	return cc.chainRW.FrozenBlocks(ctx)
}
