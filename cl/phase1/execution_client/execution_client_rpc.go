package execution_client

import (
	"context"
	"fmt"
	"net/http"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client/rpc_helper"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
)

const DefaultRPCHTTPTimeout = time.Second * 30

type ExecutionClientRpc struct {
	client    *rpc.Client
	context   context.Context
	addr      string
	jwtSecret []byte
}

func NewExecutionClientRPC(ctx context.Context, jwtSecret []byte, addr string) (*ExecutionClientRpc, error) {
	roundTripper := rpc_helper.NewJWTRoundTripper(jwtSecret)
	client := &http.Client{Timeout: DefaultRPCHTTPTimeout, Transport: roundTripper}

	rpcClient, err := rpc.DialHTTPWithClient("http://"+addr, client, nil)
	if err != nil {
		return nil, err
	}

	return &ExecutionClientRpc{
		client:    rpcClient,
		context:   ctx,
		addr:      addr,
		jwtSecret: jwtSecret,
	}, nil
}

func (cc *ExecutionClientRpc) NewPayload(payload *cltypes.Eth1Block) error {
	if payload == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(cc.context, 8*time.Second)
	defer cancel()

	engineMethod := rpc_helper.EngineNewPayloadV1
	execPayload := types.ExecutionPayload{
		Version:       uint32(payload.Version()),
		ParentHash:    gointerfaces.ConvertHashToH256(payload.ParentHash),
		StateRoot:     gointerfaces.ConvertHashToH256(payload.StateRoot),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(payload.ReceiptsRoot),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(payload.LogsBloom[:]),
		PrevRandao:    gointerfaces.ConvertHashToH256(payload.PrevRandao),
		BlockNumber:   payload.BlockNumber,
		GasLimit:      payload.GasLimit,
		GasUsed:       payload.GasUsed,
		Timestamp:     payload.Time,
		ExtraData:     payload.Extra.Bytes(),
		BaseFeePerGas: gointerfaces.ConvertHashToH256(payload.BaseFeePerGas),
		BlockHash:     gointerfaces.ConvertHashToH256(payload.BlockHash),
		Transactions:  payload.Body().Transactions,
	}

	if payload.Version() >= 2 {
		engineMethod = rpc_helper.EngineNewPayloadV2
		withdrawals := make([]*types.Withdrawal, payload.Withdrawals.Len())
		for i := 0; i < payload.Withdrawals.Len(); i++ {
			withdrawal := payload.Withdrawals.Get(i)
			withdrawals[i] = &types.Withdrawal{
				Index:          withdrawal.Index,
				ValidatorIndex: withdrawal.Validator,
				Address:        gointerfaces.ConvertAddressToH160(withdrawal.Address),
				Amount:         withdrawal.Amount,
			}
		}

		execPayload.Withdrawals = withdrawals
	}

	if payload.Version() >= 3 {
		engineMethod = rpc_helper.EngineNewPayloadV3
		execPayload.ExcessDataGas = &payload.ExcessDataGas
		execPayload.DataGasUsed = &payload.DataGasUsed
	}

	payloadStatus := &remote.EnginePayloadStatus{}
	log.Debug("[ExecutionClientRpc] Calling EL", "method", engineMethod)
	err := cc.client.CallContext(ctx, payloadStatus, engineMethod, payload)
	if err != nil {
		if err.Error() == errContextExceeded {
			return nil
		}
		return err
	}

	if err != nil {
		return fmt.Errorf("Execution Client RPC failed to retrieve the NewPayload status response, err: %w", err)
	}
	if payloadStatus.Status == remote.EngineStatus_INVALID {
		return fmt.Errorf("invalid block")
	}
	if payloadStatus.Status == remote.EngineStatus_INVALID_BLOCK_HASH {
		return fmt.Errorf("invalid block hash")
	}
	return err
}

func (cc *ExecutionClientRpc) ForkChoiceUpdate(finalized libcommon.Hash, head libcommon.Hash) error {
	ctx, cancel := context.WithTimeout(cc.context, 8*time.Second)
	defer cancel()

	forkChoiceRequest := &remote.EngineForkChoiceUpdatedRequest{
		ForkchoiceState: &remote.EngineForkChoiceState{
			HeadBlockHash:      gointerfaces.ConvertHashToH256(head),
			SafeBlockHash:      gointerfaces.ConvertHashToH256(head),
			FinalizedBlockHash: gointerfaces.ConvertHashToH256(finalized),
		},
	}
	forkChoiceResp := &remote.EngineForkChoiceUpdatedResponse{}
	log.Debug("[ExecutionClientRpc] Calling EL", "method", rpc_helper.ForkChoiceUpdatedV1)

	err := cc.client.CallContext(ctx, forkChoiceResp, rpc_helper.ForkChoiceUpdatedV1, forkChoiceRequest)
	if err != nil {
		return fmt.Errorf("Execution Client RPC failed to retrieve ForkChoiceUpdate response, err: %w", err)
	}
	// Ignore timeouts
	if err != nil && err.Error() == errContextExceeded {
		return nil
	}

	return err
}
