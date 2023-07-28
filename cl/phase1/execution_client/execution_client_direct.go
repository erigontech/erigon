package execution_client

import (
	"context"
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/log/v3"
)

type ExecutionClientDirect struct {
	api engineapi.EngineAPI
	ctx context.Context
}

func NewExecutionClientDirect(ctx context.Context, api engineapi.EngineAPI) (*ExecutionClientDirect, error) {
	return &ExecutionClientDirect{
		api: api,
		ctx: ctx,
	}, nil
}

func (cc *ExecutionClientDirect) NewPayload(payload *cltypes.Eth1Block, beaconParentRoot *libcommon.Hash) (invalid bool, err error) {
	if payload == nil {
		return
	}

	reversedBaseFeePerGas := libcommon.Copy(payload.BaseFeePerGas[:])
	for i, j := 0, len(reversedBaseFeePerGas)-1; i < j; i, j = i+1, j-1 {
		reversedBaseFeePerGas[i], reversedBaseFeePerGas[j] = reversedBaseFeePerGas[j], reversedBaseFeePerGas[i]
	}
	baseFee := new(big.Int).SetBytes(reversedBaseFeePerGas)

	request := engine_types.ExecutionPayload{
		ParentHash:   payload.ParentHash,
		FeeRecipient: payload.FeeRecipient,
		StateRoot:    payload.StateRoot,
		ReceiptsRoot: payload.ReceiptsRoot,
		LogsBloom:    payload.LogsBloom[:],
		PrevRandao:   payload.PrevRandao,
		BlockNumber:  hexutil.Uint64(payload.BlockNumber),
		GasLimit:     hexutil.Uint64(payload.GasLimit),
		GasUsed:      hexutil.Uint64(payload.GasUsed),
		Timestamp:    hexutil.Uint64(payload.Time),
		ExtraData:    payload.Extra.Bytes(),
		BlockHash:    payload.BlockHash,
	}

	request.BaseFeePerGas = new(hexutil.Big)
	*request.BaseFeePerGas = hexutil.Big(*baseFee)
	payloadBody := payload.Body()
	// Setup transactionbody
	request.Withdrawals = payloadBody.Withdrawals

	for _, bytesTransaction := range payloadBody.Transactions {
		request.Transactions = append(request.Transactions, bytesTransaction)
	}
	// Process Deneb
	if payload.Version() >= clparams.DenebVersion {
		request.BlobGasUsed = new(hexutil.Uint64)
		request.ExcessBlobGas = new(hexutil.Uint64)
		*request.BlobGasUsed = hexutil.Uint64(payload.BlobGasUsed)
		*request.ExcessBlobGas = hexutil.Uint64(payload.ExcessBlobGas)
	}

	payloadStatus := &engine_types.PayloadStatus{} // As it is done in the rpcdaemon

	// determine the engine method
	switch payload.Version() {
	case clparams.BellatrixVersion:
		payloadStatus, err = cc.api.NewPayloadV1(cc.ctx, &request)
	case clparams.CapellaVersion:
		payloadStatus, err = cc.api.NewPayloadV2(cc.ctx, &request)
	case clparams.DenebVersion:
		//TODO: Add 4844 and 4788 fields correctly
		payloadStatus, err = cc.api.NewPayloadV3(cc.ctx, &request, nil, beaconParentRoot)
	default:
		err = fmt.Errorf("invalid payload version")
	}
	if err != nil {
		err = fmt.Errorf("execution Client RPC failed to retrieve the NewPayload status response, err: %w", err)
		return
	}

	invalid = payloadStatus.Status == engine_types.InvalidStatus || payloadStatus.Status == engine_types.InvalidBlockHashStatus
	err = checkPayloadStatus(payloadStatus)
	if payloadStatus.Status == engine_types.AcceptedStatus {
		log.Info("[ExecutionClientRpc] New block accepted")
	}
	return
}

func (cc *ExecutionClientDirect) ForkChoiceUpdate(finalized libcommon.Hash, head libcommon.Hash) error {
	forkChoiceRequest := engine_types.ForkChoiceState{
		HeadHash:           head,
		SafeBlockHash:      head,
		FinalizedBlockHash: finalized,
	}

	forkChoiceResp, err := cc.api.ForkchoiceUpdatedV2(cc.ctx, &forkChoiceRequest, nil)
	if err != nil {
		return fmt.Errorf("execution Client RPC failed to retrieve ForkChoiceUpdate response, err: %w", err)
	}
	return checkPayloadStatus(forkChoiceResp.PayloadStatus)
}
