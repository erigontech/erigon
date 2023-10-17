package execution_client

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client/rpc_helper"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/log/v3"
)

const DefaultRPCHTTPTimeout = time.Second * 30

type ExecutionClientRpc struct {
	client    *rpc.Client
	ctx       context.Context
	addr      string
	jwtSecret []byte
}

func NewExecutionClientRPC(ctx context.Context, jwtSecret []byte, addr string, port int) (*ExecutionClientRpc, error) {
	roundTripper := rpc_helper.NewJWTRoundTripper(jwtSecret)
	client := &http.Client{Timeout: DefaultRPCHTTPTimeout, Transport: roundTripper}

	isHTTPpecified := strings.HasPrefix(addr, "http")
	isHTTPSpecified := strings.HasPrefix(addr, "https")
	protocol := ""
	if isHTTPSpecified {
		protocol = "https://"
	} else if !isHTTPpecified {
		protocol = "http://"
	}
	rpcClient, err := rpc.DialHTTPWithClient(fmt.Sprintf("%s%s:%d", protocol, addr, port), client, nil)
	if err != nil {
		return nil, err
	}

	return &ExecutionClientRpc{
		client:    rpcClient,
		ctx:       ctx,
		addr:      addr,
		jwtSecret: jwtSecret,
	}, nil
}

func (cc *ExecutionClientRpc) NewPayload(payload *cltypes.Eth1Block, beaconParentRoot *libcommon.Hash) (invalid bool, err error) {
	if payload == nil {
		return
	}

	reversedBaseFeePerGas := libcommon.Copy(payload.BaseFeePerGas[:])
	for i, j := 0, len(reversedBaseFeePerGas)-1; i < j; i, j = i+1, j-1 {
		reversedBaseFeePerGas[i], reversedBaseFeePerGas[j] = reversedBaseFeePerGas[j], reversedBaseFeePerGas[i]
	}
	baseFee := new(big.Int).SetBytes(reversedBaseFeePerGas)
	var engineMethod string
	// determine the engine method
	switch payload.Version() {
	case clparams.BellatrixVersion:
		engineMethod = rpc_helper.EngineNewPayloadV1
	case clparams.CapellaVersion:
		engineMethod = rpc_helper.EngineNewPayloadV2
	case clparams.DenebVersion:
		engineMethod = rpc_helper.EngineNewPayloadV3
	default:
		err = fmt.Errorf("invalid payload version")
		return
	}

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
	log.Debug("[ExecutionClientRpc] Calling EL", "method", engineMethod)
	err = cc.client.CallContext(cc.ctx, &payloadStatus, engineMethod, request)
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

func (cc *ExecutionClientRpc) ForkChoiceUpdate(finalized libcommon.Hash, head libcommon.Hash) error {
	forkChoiceRequest := engine_types.ForkChoiceState{
		HeadHash:           head,
		SafeBlockHash:      head,
		FinalizedBlockHash: finalized,
	}
	forkChoiceResp := &engine_types.ForkChoiceUpdatedResponse{}
	log.Debug("[ExecutionClientRpc] Calling EL", "method", rpc_helper.ForkChoiceUpdatedV1)

	err := cc.client.CallContext(cc.ctx, forkChoiceResp, rpc_helper.ForkChoiceUpdatedV1, forkChoiceRequest)
	if err != nil {
		return fmt.Errorf("execution Client RPC failed to retrieve ForkChoiceUpdate response, err: %w", err)
	}
	// Ignore timeouts
	if err != nil && err.Error() == errContextExceeded {
		return nil
	}
	if err != nil {
		return err
	}

	return checkPayloadStatus(forkChoiceResp.PayloadStatus)
}

func checkPayloadStatus(payloadStatus *engine_types.PayloadStatus) error {
	if payloadStatus == nil {
		return fmt.Errorf("empty payloadStatus")
	}

	validationError := payloadStatus.ValidationError
	if validationError != nil {
		return validationError.Error()
	}

	if payloadStatus.Status == engine_types.InvalidStatus {
		return fmt.Errorf("status: %s", payloadStatus.Status)
	}
	return nil
}

func (cc *ExecutionClientRpc) SupportInsertion() bool {
	return false
}

func (cc *ExecutionClientRpc) InsertBlocks([]*types.Block) error {
	panic("unimplemented")
}

func (cc *ExecutionClientRpc) InsertBlock(*types.Block) error {
	panic("unimplemented")
}

func (cc *ExecutionClientRpc) IsCanonicalHash(libcommon.Hash) (bool, error) {
	panic("unimplemented")
}

func (cc *ExecutionClientRpc) Ready() (bool, error) {
	return true, nil // Engine API is always ready
}

// Range methods

// GetBodiesByRange gets block bodies in given block range
func (cc *ExecutionClientRpc) GetBodiesByRange(start, count uint64) ([]*types.RawBody, error) {
	result := []*engine_types.ExecutionPayloadBodyV1{}

	if err := cc.client.CallContext(cc.ctx, &result, rpc_helper.GetPayloadBodiesByRangeV1, hexutil.Uint64(start), hexutil.Uint64(count)); err != nil {
		return nil, err
	}
	ret := make([]*types.RawBody, len(result))
	for i := range result {
		ret[i] = &types.RawBody{
			Withdrawals: result[i].Withdrawals,
		}
		for _, txn := range result[i].Transactions {
			ret[i].Transactions = append(ret[i].Transactions, txn)
		}
	}
	return ret, nil
}

// GetBodiesByHashes gets block bodies with given hashes
func (cc *ExecutionClientRpc) GetBodiesByHashes(hashes []libcommon.Hash) ([]*types.RawBody, error) {
	result := []*engine_types.ExecutionPayloadBodyV1{}

	if err := cc.client.CallContext(cc.ctx, &result, rpc_helper.GetPayloadBodiesByHashV1, hashes); err != nil {
		return nil, err
	}
	ret := make([]*types.RawBody, len(result))
	for i := range result {
		ret[i] = &types.RawBody{
			Withdrawals: result[i].Withdrawals,
		}
		for _, txn := range result[i].Transactions {
			ret[i].Transactions = append(ret[i].Transactions, txn)
		}
	}
	return ret, nil
}

func (cc *ExecutionClientRpc) FrozenBlocks() uint64 {
	panic("unimplemented")
}
