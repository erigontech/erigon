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
	ctx     context.Context
}

func NewExecutionClientDirect(ctx context.Context, chainRW eth1_chain_reader.ChainReaderWriterEth1) (*ExecutionClientDirect, error) {
	return &ExecutionClientDirect{
		chainRW: chainRW,
		ctx:     ctx,
	}, nil
}

func (cc *ExecutionClientDirect) NewPayload(payload *cltypes.Eth1Block, beaconParentRoot *libcommon.Hash) (invalid bool, err error) {
	if payload == nil {
		return
	}

	header, err := payload.RlpHeader()
	if err != nil {
		return true, err
	}

	body := payload.Body()
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		return true, err
	}

	if err := cc.chainRW.InsertBlockAndWait(types.NewBlockFromStorage(payload.BlockHash, header, txs, nil, body.Withdrawals)); err != nil {
		return false, err
	}

	status, _, err := cc.chainRW.ValidateChain(payload.BlockHash, payload.BlockNumber)
	if err != nil {
		return false, err
	}
	invalid = status == execution.ExecutionStatus_BadBlock

	return
}

func (cc *ExecutionClientDirect) ForkChoiceUpdate(finalized libcommon.Hash, head libcommon.Hash) error {
	status, _, err := cc.chainRW.UpdateForkChoice(head, head, finalized)
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

func (cc *ExecutionClientDirect) InsertBlocks(blks []*types.Block) error {
	return cc.chainRW.InsertBlocksAndWait(blks)
}

func (cc *ExecutionClientDirect) InsertBlock(blk *types.Block) error {
	return cc.chainRW.InsertBlockAndWait(blk)
}

func (cc *ExecutionClientDirect) IsCanonicalHash(hash libcommon.Hash) (bool, error) {
	return cc.chainRW.IsCanonicalHash(hash)
}

func (cc *ExecutionClientDirect) Ready() (bool, error) {
	return cc.chainRW.Ready()
}

// GetBodiesByRange gets block bodies in given block range
func (cc *ExecutionClientDirect) GetBodiesByRange(start, count uint64) ([]*types.RawBody, error) {
	return cc.chainRW.GetBodiesByRange(start, count), nil

}

// GetBodiesByHashes gets block bodies with given hashes
func (cc *ExecutionClientDirect) GetBodiesByHashes(hashes []libcommon.Hash) ([]*types.RawBody, error) {
	return cc.chainRW.GetBodiesByHases(hashes), nil
}
