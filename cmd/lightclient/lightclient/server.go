package lightclient

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
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

func convertLightrpcExecutionPayloadToEthbacked(e *lightrpc.ExecutionPayload) *types.ExecutionPayload {
	return &types.ExecutionPayload{
		ParentHash:    gointerfaces.ConvertHashToH256(common.BytesToHash(e.ParentHash)),
		Coinbase:      gointerfaces.ConvertAddressToH160(common.BytesToAddress(e.FeeRecipient)),
		StateRoot:     gointerfaces.ConvertHashToH256(common.BytesToHash(e.StateRoot)),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(common.BytesToHash(e.ReceiptsRoot)),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(e.LogsBloom),
		PrevRandao:    gointerfaces.ConvertHashToH256(common.BytesToHash(e.PrevRandao)),
		BlockNumber:   e.BlockNumber,
		GasLimit:      e.GasLimit,
		GasUsed:       e.GasUsed,
		Timestamp:     e.Timestamp,
		ExtraData:     e.ExtraData,
		BaseFeePerGas: gointerfaces.ConvertHashToH256(common.BytesToHash(e.BaseFeePerGas)),
		BlockHash:     gointerfaces.ConvertHashToH256(common.BytesToHash(e.BlockHash)),
		// Transactions:  e.Transactions,
	}
}

func (l *LightClientServer) NotifyBeaconBlock(ctx context.Context, beaconBlock *lightrpc.SignedBeaconBlockBellatrix) (*lightrpc.NotificationStatus, error) {
	payloadHash := gointerfaces.ConvertHashToH256(
		common.BytesToHash(beaconBlock.Block.Body.ExecutionPayload.BlockHash))

	// payload := convertLightrpcExecutionPayloadToEthbacked(beaconBlock.Block.Body.ExecutionPayload)
	// Send forkchoice
	var err error
	if l.executionClient != nil {
		/*_, err = l.executionClient.EngineNewPayloadV1(ctx, payload)
		if err != nil {
			return nil, err
		}
		// Wait a bit
		time.Sleep(100 * time.Millisecond)*/
		_, err = l.executionClient.EngineForkChoiceUpdatedV1(ctx, &remote.EngineForkChoiceUpdatedRequest{
			ForkchoiceState: &remote.EngineForkChoiceState{
				HeadBlockHash:      payloadHash,
				SafeBlockHash:      payloadHash,
				FinalizedBlockHash: payloadHash,
			},
		})
	} else {
		/*_, err = l.executionServer.EngineNewPayloadV1(ctx, payload)
		if err != nil {
			return nil, err
		}
		// Wait a bit
		time.Sleep(100 * time.Millisecond)*/
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
