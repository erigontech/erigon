package lightclient

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/common"
)

type LightClientServer struct {
	lightrpc.UnimplementedLightclientServer

	executionClient remote.ETHBACKENDClient
	executionServer remote.ETHBACKENDServer
}

func NewLightclientServer(executionClient remote.ETHBACKENDClient) lightrpc.LightclientServer {
	return &LightClientServer{
		executionClient: executionClient,
	}
}

func NewLightclientServerInternal(executionServer remote.ETHBACKENDServer) lightrpc.LightclientServer {
	return &LightClientServer{
		executionServer: executionServer,
	}
}

func (l *LightClientServer) NotifyBeaconBlock(ctx context.Context, beaconBlock *lightrpc.SignedBeaconBlockBellatrix) (*lightrpc.NotificationStatus, error) {
	payloadHash := gointerfaces.ConvertHashToH256(
		common.BytesToHash(beaconBlock.Block.Body.ExecutionPayload.BlockHash))
	// Send forkchoice
	var err error
	if l.executionClient != nil {
		_, err = l.executionClient.EngineForkChoiceUpdatedV1(ctx, &remote.EngineForkChoiceUpdatedRequest{
			ForkchoiceState: &remote.EngineForkChoiceState{
				HeadBlockHash:      payloadHash,
				SafeBlockHash:      payloadHash,
				FinalizedBlockHash: payloadHash,
			},
		})
	} else {
		_, err = l.executionServer.EngineForkChoiceUpdatedV1(ctx, &remote.EngineForkChoiceUpdatedRequest{
			ForkchoiceState: &remote.EngineForkChoiceState{
				HeadBlockHash:      payloadHash,
				SafeBlockHash:      payloadHash,
				FinalizedBlockHash: payloadHash,
			},
		})
	}

	return &lightrpc.NotificationStatus{
		Status: 0,
	}, err
}
