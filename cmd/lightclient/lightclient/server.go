/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package lightclient

import (
	"context"
	"math/big"
	"time"

	"github.com/holiman/uint256"
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
	var baseFee *uint256.Int

	if e.BaseFeePerGas != nil {
		// Trim and reverse it.
		baseFeeBytes := common.CopyBytes(e.BaseFeePerGas)
		for baseFeeBytes[len(baseFeeBytes)-1] == 0 && len(baseFeeBytes) > 0 {
			baseFeeBytes = baseFeeBytes[:len(baseFeeBytes)-1]
		}
		for i, j := 0, len(baseFeeBytes)-1; i < j; i, j = i+1, j-1 {
			baseFeeBytes[i], baseFeeBytes[j] = baseFeeBytes[j], baseFeeBytes[i]
		}
		var overflow bool
		baseFee, overflow = uint256.FromBig(new(big.Int).SetBytes(baseFeeBytes))
		if overflow {
			panic("NewPayload BaseFeePerGas overflow")
		}
	}
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
		BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(baseFee),
		BlockHash:     gointerfaces.ConvertHashToH256(common.BytesToHash(e.BlockHash)),
		Transactions:  e.Transactions,
	}
}

func (l *LightClientServer) NotifyBeaconBlock(ctx context.Context, beaconBlock *lightrpc.SignedBeaconBlockBellatrix) (*lightrpc.NotificationStatus, error) {
	payloadHash := gointerfaces.ConvertHashToH256(
		common.BytesToHash(beaconBlock.Block.Body.ExecutionPayload.BlockHash))

	payload := convertLightrpcExecutionPayloadToEthbacked(beaconBlock.Block.Body.ExecutionPayload)
	var err error
	if l.executionClient != nil {
		_, err = l.executionClient.EngineNewPayloadV1(ctx, payload)
		if err != nil {
			return nil, err
		}
		// Wait a bit
		time.Sleep(500 * time.Millisecond)
		_, err = l.executionClient.EngineForkChoiceUpdatedV1(ctx, &remote.EngineForkChoiceUpdatedRequest{
			ForkchoiceState: &remote.EngineForkChoiceState{
				HeadBlockHash:      payloadHash,
				SafeBlockHash:      payloadHash,
				FinalizedBlockHash: payloadHash,
			},
		})
	} else {
		_, err = l.executionServer.EngineNewPayloadV1(ctx, payload)
		if err != nil {
			return nil, err
		}
		// Wait a bit
		time.Sleep(500 * time.Millisecond)
		_, err = l.executionServer.EngineForkChoiceUpdatedV1(ctx, &remote.EngineForkChoiceUpdatedRequest{
			ForkchoiceState: &remote.EngineForkChoiceState{
				HeadBlockHash:      payloadHash,
				SafeBlockHash:      payloadHash,
				FinalizedBlockHash: payloadHash,
			},
		})
	}
	if err != nil {
		return nil, err
	}

	return &lightrpc.NotificationStatus{
		Status: 0,
	}, err
}
