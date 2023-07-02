package execution_client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client/rpc_helper"
	"github.com/ledgerwatch/erigon/rpc"
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

	rpcClient, err := rpc.DialHTTPWithClient(addr, client, nil)
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

func (cc *ExecutionClientRpc) NewRequest(method, engineMethod string, bodyReader io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, "http://"+cc.addr+"/"+engineMethod, bodyReader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func (cc *ExecutionClientRpc) NewPayload(payload *cltypes.Eth1Block) error {
	ctx, cancel := context.WithTimeout(cc.context, 8*time.Second)
	defer cancel()

	engineMethod := rpc_helper.EngineNewPayloadV1
	if payload.Withdrawals != nil {
		engineMethod = rpc_helper.EngineNewPayloadV2
	}

	payloadStatus := &remote.EnginePayloadStatus{}

	err := cc.client.CallContext(ctx, payloadStatus, engineMethod, payload)
	if err != nil {
		if err.Error() == errContextExceeded {
			return nil
		}
		return err
	}

	if err != nil {
		return fmt.Errorf("Execution Client RPC failed to decode the NewPayload status response, err: %w", err)
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
