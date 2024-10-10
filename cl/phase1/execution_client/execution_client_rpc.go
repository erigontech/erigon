package execution_client

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/ledgerwatch/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client/rpc_helper"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
)

const DefaultRPCHTTPTimeout = time.Second * 30

type ExecutionClientRpc struct {
	client    *rpc.Client
	addr      string
	jwtSecret []byte
}

func NewExecutionClientRPC(jwtSecret []byte, addr string, port int) (*ExecutionClientRpc, error) {
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
		addr:      addr,
		jwtSecret: jwtSecret,
	}, nil
}

func (cc *ExecutionClientRpc) NewPayload(ctx context.Context, payload *cltypes.Eth1Block, beaconParentRoot *libcommon.Hash, versionedHashes []libcommon.Hash) (invalid bool, err error) {
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
	args := []interface{}{request}
	if versionedHashes != nil {
		args = append(args, versionedHashes, *beaconParentRoot)
	}
	err = cc.client.CallContext(ctx, &payloadStatus, engineMethod, args...)
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

func (cc *ExecutionClientRpc) ForkChoiceUpdate(ctx context.Context, finalized libcommon.Hash, head libcommon.Hash, attributes *engine_types.PayloadAttributes) ([]byte, error) {
	forkChoiceRequest := engine_types.ForkChoiceState{
		HeadHash:           head,
		SafeBlockHash:      head,
		FinalizedBlockHash: finalized,
	}
	forkChoiceResp := &engine_types.ForkChoiceUpdatedResponse{}
	log.Debug("[ExecutionClientRpc] Calling EL", "method", rpc_helper.ForkChoiceUpdatedV1)
	args := []interface{}{forkChoiceRequest}
	if attributes != nil {
		args = append(args, attributes)
	}

	err := cc.client.CallContext(ctx, forkChoiceResp, rpc_helper.ForkChoiceUpdatedV1, args...)
	if err != nil {
		return nil, fmt.Errorf("execution Client RPC failed to retrieve ForkChoiceUpdate response, err: %w", err)
	}
	// Ignore timeouts
	if err != nil && err.Error() == errContextExceeded {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if forkChoiceResp.PayloadId == nil {
		return []byte{}, checkPayloadStatus(forkChoiceResp.PayloadStatus)
	}

	return *forkChoiceResp.PayloadId, checkPayloadStatus(forkChoiceResp.PayloadStatus)
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

func (cc *ExecutionClientRpc) InsertBlocks(ctx context.Context, blocks []*types.Block, wait bool) error {
	panic("unimplemented")
}

func (cc *ExecutionClientRpc) InsertBlock(ctx context.Context, block *types.Block) error {
	panic("unimplemented")
}

func (cc *ExecutionClientRpc) CurrentHeader(ctx context.Context) (*types.Header, error) {
	panic("unimplemented")
}

func (cc *ExecutionClientRpc) IsCanonicalHash(ctx context.Context, hash libcommon.Hash) (bool, error) {
	panic("unimplemented")
}

func (cc *ExecutionClientRpc) Ready(ctx context.Context) (bool, error) {
	return true, nil // Engine API is always ready
}

// Range methods

// GetBodiesByRange gets block bodies in given block range
func (cc *ExecutionClientRpc) GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error) {
	result := []*engine_types.ExecutionPayloadBodyV1{}

	if err := cc.client.CallContext(ctx, &result, rpc_helper.GetPayloadBodiesByRangeV1, hexutil.Uint64(start), hexutil.Uint64(count)); err != nil {
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
func (cc *ExecutionClientRpc) GetBodiesByHashes(ctx context.Context, hashes []libcommon.Hash) ([]*types.RawBody, error) {
	result := []*engine_types.ExecutionPayloadBodyV1{}

	if err := cc.client.CallContext(ctx, &result, rpc_helper.GetPayloadBodiesByHashV1, hashes); err != nil {
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

func (cc *ExecutionClientRpc) FrozenBlocks(ctx context.Context) uint64 {
	panic("unimplemented")
}

// HasBlock checks if block with given hash is present
func (cc *ExecutionClientRpc) HasBlock(ctx context.Context, hash libcommon.Hash) (bool, error) {
	panic("unimplemented")
}

// Block production

func (cc *ExecutionClientRpc) GetAssembledBlock(ctx context.Context, id []byte) (*cltypes.Eth1Block, *engine_types.BlobsBundleV1, *big.Int, error) {
	panic("unimplemented")
}
