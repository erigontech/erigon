package execution_client

import (
	"context"
	"fmt"
	"time"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	types2 "github.com/ledgerwatch/erigon/core/types"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
)

var errContextExceeded = "rpc error: code = DeadlineExceeded desc = context deadline exceeded"

// ExecutionEngine is used only for syncing up very close to chain tip and to stay in sync.
// It pretty much mimics engine API.
type ExecutionEngine interface {
	NewPayload(payload *cltypes.Eth1Block) error
	ForkChoiceUpdate(finalized libcommon.Hash, head libcommon.Hash) error
}

// ExecutionEnginePhase1 is just the normal engine api.
type ExecutionEnginePhase1 struct {
	// Either execution server or client
	executionServer remote.ETHBACKENDServer
	executionClient remote.ETHBACKENDClient

	ctx context.Context
}

// NewExecutionEnginePhase1FromServer use ethbackend server
func NewExecutionEnginePhase1FromServer(ctx context.Context, executionServer remote.ETHBACKENDServer) *ExecutionEnginePhase1 {
	return &ExecutionEnginePhase1{
		executionServer: executionServer,
		ctx:             ctx,
	}
}

// NewExecutionEnginePhase1FromServer use ethbackend client
func NewExecutionEnginePhase1FromClient(ctx context.Context, executionClient remote.ETHBACKENDClient) *ExecutionEnginePhase1 {
	return &ExecutionEnginePhase1{
		executionClient: executionClient,
		ctx:             ctx,
	}
}

func (e *ExecutionEnginePhase1) NewPayload(payload *cltypes.Eth1Block) error {
	grpcMessage, err := convertPayloadToGrpc(payload)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(e.ctx, 3*time.Second)
	defer cancel()

	var status *remote.EnginePayloadStatus
	if e.executionServer != nil {
		status, err = e.executionServer.EngineNewPayload(ctx, grpcMessage)
	} else if e.executionClient != nil {
		status, err = e.executionClient.EngineNewPayload(ctx, grpcMessage)
	}
	// Ignore timeouts
	if err != nil {
		if err.Error() == errContextExceeded {
			return nil
		}
		return err
	}
	if status.Status == remote.EngineStatus_INVALID {
		return fmt.Errorf("invalid block")
	}
	if status.Status == remote.EngineStatus_INVALID_BLOCK_HASH {
		return fmt.Errorf("invalid block hash")
	}
	return err
}

func (e *ExecutionEnginePhase1) ForkChoiceUpdate(finalized libcommon.Hash, head libcommon.Hash) error {
	grpcMessage := &remote.EngineForkChoiceUpdatedRequest{
		ForkchoiceState: &remote.EngineForkChoiceState{
			HeadBlockHash:      gointerfaces.ConvertHashToH256(head),
			SafeBlockHash:      gointerfaces.ConvertHashToH256(head),
			FinalizedBlockHash: gointerfaces.ConvertHashToH256(finalized),
		},
	}
	var err error
	ctx, cancel := context.WithTimeout(e.ctx, 3*time.Second)
	defer cancel()
	if e.executionClient != nil {
		_, err = e.executionClient.EngineForkChoiceUpdated(ctx, grpcMessage)
	} else if e.executionServer != nil {
		_, err = e.executionServer.EngineForkChoiceUpdated(ctx, grpcMessage)
	}
	// Ignore timeouts
	if err != nil && err.Error() == errContextExceeded {
		return nil
	}

	return err
}

// ExecutionEnginePhase2 is "real merge" (TODO).
type ExecutionEnginePhase2 struct {
}

func convertPayloadToGrpc(e *cltypes.Eth1Block) (*types.ExecutionPayload, error) {
	var baseFee *uint256.Int
	header, err := e.RlpHeader()
	if err != nil {
		return nil, err
	}

	if header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			panic("NewPayload BaseFeePerGas overflow")
		}
	}
	withdrawals := make([]*types2.Withdrawal, e.Withdrawals.Len())
	e.Withdrawals.Range(func(_ int, w *types2.Withdrawal, _ int) bool {
		withdrawals = append(withdrawals, w)
		return true
	})

	res := &types.ExecutionPayload{
		Version:       1,
		ParentHash:    gointerfaces.ConvertHashToH256(header.ParentHash),
		Coinbase:      gointerfaces.ConvertAddressToH160(header.Coinbase),
		StateRoot:     gointerfaces.ConvertHashToH256(header.Root),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(header.ReceiptHash),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(header.Bloom[:]),
		PrevRandao:    gointerfaces.ConvertHashToH256(header.MixDigest),
		BlockNumber:   header.Number.Uint64(),
		GasLimit:      header.GasLimit,
		GasUsed:       header.GasUsed,
		Timestamp:     header.Time,
		ExtraData:     header.Extra,
		BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(baseFee),
		BlockHash:     gointerfaces.ConvertHashToH256(e.BlockHash),
		Transactions:  e.Transactions.UnderlyngReference(),
		Withdrawals:   privateapi.ConvertWithdrawalsToRpc(withdrawals),
	}
	if e.Withdrawals != nil {
		res.Version = 2
		res.Withdrawals = privateapi.ConvertWithdrawalsToRpc(withdrawals)
	}

	return res, nil
}
