package lightclient

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/common"
)

type LightClientServer struct {
	executionClient remote.ETHBACKENDClient
}

func NewLightclientServer(executionClient remote.ETHBACKENDClient) *LightClientServer {
	return &LightClientServer{
		executionClient: executionClient,
	}
}

func (l *LightClientServer) NotifyBeaconBlock(ctx context.Context, beaconBlock *lightrpc.SignedBeaconBlockBellatrix) (*lightrpc.NotificationStatus, error) {
	payloadHash := gointerfaces.ConvertHashToH256(
		common.BytesToHash(beaconBlock.Block.Body.ExecutionPayload.BlockHash))
	// Send forkchoice
	_, err := l.executionClient.EngineForkChoiceUpdatedV1(ctx, &remote.EngineForkChoiceUpdatedRequest{
		ForkchoiceState: &remote.EngineForkChoiceState{
			HeadBlockHash:      payloadHash,
			SafeBlockHash:      payloadHash,
			FinalizedBlockHash: payloadHash,
		},
	})
	return &lightrpc.NotificationStatus{
		Status: 0,
	}, err
}
