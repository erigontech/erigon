package execution_client

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/engine"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client/rpc_helper"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
	"github.com/ledgerwatch/log/v3"
)

const DefaultRPCHTTPTimeout = time.Second * 30

const (
	ValidStatus            = "VALID"
	InvalidStatus          = "INVALID"
	SyncingStatus          = "SYNCING"
	AcceptedStatus         = "ACCEPTED"
	InvalidBlockHashStatus = "INVALID_BLOCK_HASH"
)

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

func (cc *ExecutionClientRpc) NewPayload(payload *cltypes.Eth1Block) (invalid bool, err error) {
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

	request := jsonrpc.ExecutionPayload{
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
		request.DataGasUsed = new(hexutil.Uint64)
		request.ExcessDataGas = new(hexutil.Uint64)
		*request.DataGasUsed = hexutil.Uint64(payload.DataGasUsed)
		*request.ExcessDataGas = hexutil.Uint64(payload.ExcessDataGas)
	}

	payloadStatus := make(map[string]interface{}) // As it is done in the rpcdaemon
	log.Debug("[ExecutionClientRpc] Calling EL", "method", engineMethod)
	err = cc.client.CallContext(cc.ctx, &payloadStatus, engineMethod, request)
	if err != nil {
		return
	}

	if err != nil {
		err = fmt.Errorf("execution Client RPC failed to retrieve the NewPayload status response, err: %w", err)
		return
	}
	var status string
	var ok bool
	if status, ok = payloadStatus["status"].(string); !ok {
		err = fmt.Errorf("invalid response received from NewPayload")
		return

	}
	invalid = status == InvalidStatus || status == InvalidBlockHashStatus

	return
}

func (cc *ExecutionClientRpc) ForkChoiceUpdate(finalized libcommon.Hash, head libcommon.Hash) error {
	forkChoiceRequest := jsonrpc.ForkChoiceState{
		HeadHash:           head,
		SafeBlockHash:      head,
		FinalizedBlockHash: finalized,
	}
	forkChoiceResp := &engine.EngineForkChoiceUpdatedResponse{}
	log.Debug("[ExecutionClientRpc] Calling EL", "method", rpc_helper.ForkChoiceUpdatedV1)

	err := cc.client.CallContext(cc.ctx, forkChoiceResp, rpc_helper.ForkChoiceUpdatedV1, forkChoiceRequest)
	if err != nil {
		return fmt.Errorf("execution Client RPC failed to retrieve ForkChoiceUpdate response, err: %w", err)
	}
	// Ignore timeouts
	if err != nil && err.Error() == errContextExceeded {
		return nil
	}

	return err
}
