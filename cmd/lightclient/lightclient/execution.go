package lightclient

import (
	"context"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/common"
)

func convertLightrpcExecutionPayloadToEthbacked(e *cltypes.ExecutionPayload) *types.ExecutionPayload {
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
		ParentHash:    gointerfaces.ConvertHashToH256(e.ParentHash),
		Coinbase:      gointerfaces.ConvertAddressToH160(e.FeeRecipient),
		StateRoot:     gointerfaces.ConvertHashToH256(e.StateRoot),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(e.ReceiptsRoot),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(e.LogsBloom),
		PrevRandao:    gointerfaces.ConvertHashToH256(e.PrevRandao),
		BlockNumber:   e.BlockNumber,
		GasLimit:      e.GasLimit,
		GasUsed:       e.GasUsed,
		Timestamp:     e.Timestamp,
		ExtraData:     e.ExtraData,
		BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(baseFee),
		BlockHash:     gointerfaces.ConvertHashToH256(e.BlockHash),
		Transactions:  e.Transactions,
	}
}

func (l *LightClient) processBeaconBlock(ctx context.Context, beaconBlock *cltypes.SignedBeaconBlockBellatrix) error {
	if l.execution == nil {
		return nil
	}
	payloadHash := gointerfaces.ConvertHashToH256(beaconBlock.Block.Body.ExecutionPayload.BlockHash)

	payload := convertLightrpcExecutionPayloadToEthbacked(beaconBlock.Block.Body.ExecutionPayload)
	var err error
	_, err = l.execution.EngineNewPayloadV1(ctx, payload)
	if err != nil {
		return err
	}
	// Wait a bit
	time.Sleep(500 * time.Millisecond)
	_, err = l.execution.EngineForkChoiceUpdatedV1(ctx, &remote.EngineForkChoiceUpdatedRequest{
		ForkchoiceState: &remote.EngineForkChoiceState{
			HeadBlockHash:      payloadHash,
			SafeBlockHash:      payloadHash,
			FinalizedBlockHash: payloadHash,
		},
	})
	return err
}
