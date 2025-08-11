// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package execution_client

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	common "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/jwt"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client/rpc_helper"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

const DefaultRPCHTTPTimeout = time.Second * 30

type ExecutionClientRpc struct {
	client    *rpc.Client
	addr      string
	jwtSecret []byte
}

func NewExecutionClientRPC(jwtSecret []byte, addr string, port int) (*ExecutionClientRpc, error) {
	roundTripper := jwt.NewHttpRoundTripper(http.DefaultTransport, jwtSecret)
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

func (cc *ExecutionClientRpc) NewPayload(
	ctx context.Context,
	payload *cltypes.Eth1Block,
	beaconParentRoot *common.Hash,
	versionedHashes []common.Hash,
	executionRequestsList []hexutil.Bytes,
) (PayloadStatus, error) {
	if payload == nil {
		return PayloadStatusValidated, nil
	}

	reversedBaseFeePerGas := common.Copy(payload.BaseFeePerGas[:])
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
	case clparams.ElectraVersion:
		engineMethod = rpc_helper.EngineNewPayloadV4
	// TODO: Add Fulu case
	default:
		return PayloadStatusNone, errors.New("invalid payload version")
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
	if executionRequestsList != nil {
		args = append(args, executionRequestsList)
	}
	if err := cc.client.CallContext(ctx, &payloadStatus, engineMethod, args...); err != nil {
		err = fmt.Errorf("execution Client RPC failed to retrieve the NewPayload status response, err: %w", err)
		return PayloadStatusNone, err
	}

	if payloadStatus.Status == engine_types.AcceptedStatus {
		log.Info("[ExecutionClientRpc] New block accepted")
	}
	return newPayloadStatusByEngineStatus(payloadStatus.Status), checkPayloadStatus(payloadStatus)
}

func (cc *ExecutionClientRpc) ForkChoiceUpdate(ctx context.Context, finalized, safe, head common.Hash, attributes *engine_types.PayloadAttributes) ([]byte, error) {
	forkChoiceRequest := engine_types.ForkChoiceState{
		HeadHash:           head,
		SafeBlockHash:      safe,
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
		if err.Error() == errContextExceeded {
			// ignore timeouts
			return nil, nil
		}
		return nil, fmt.Errorf("execution Client RPC failed to retrieve ForkChoiceUpdate response, err: %w", err)
	}

	if forkChoiceResp.PayloadId == nil {
		return []byte{}, checkPayloadStatus(forkChoiceResp.PayloadStatus)
	}

	return *forkChoiceResp.PayloadId, checkPayloadStatus(forkChoiceResp.PayloadStatus)
}

func checkPayloadStatus(payloadStatus *engine_types.PayloadStatus) error {
	if payloadStatus == nil {
		return errors.New("empty payloadStatus")
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

func (cc *ExecutionClientRpc) IsCanonicalHash(ctx context.Context, hash common.Hash) (bool, error) {
	panic("unimplemented")
}

func (cc *ExecutionClientRpc) Ready(ctx context.Context) (bool, error) {
	return true, nil // Engine API is always ready
}

// Range methods

// GetBodiesByRange gets block bodies in given block range
func (cc *ExecutionClientRpc) GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error) {
	result := []*engine_types.ExecutionPayloadBody{}

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
func (cc *ExecutionClientRpc) GetBodiesByHashes(ctx context.Context, hashes []common.Hash) ([]*types.RawBody, error) {
	result := []*engine_types.ExecutionPayloadBody{}

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
func (cc *ExecutionClientRpc) HasBlock(ctx context.Context, hash common.Hash) (bool, error) {
	panic("unimplemented")
}

// Block production

func (cc *ExecutionClientRpc) GetAssembledBlock(ctx context.Context, id []byte) (*cltypes.Eth1Block, *engine_types.BlobsBundleV1, *typesproto.RequestsBundle, *big.Int, error) {
	panic("unimplemented")
}

func (cc *ExecutionClientRpc) HasGapInSnapshots(ctx context.Context) bool {
	panic("unimplemented")
}
