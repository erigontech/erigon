package lightclient

import (
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

func convertLightrpcExecutionPayloadToEthbacked(e *cltypes.Eth1Block) *types.ExecutionPayload {
	var baseFee *uint256.Int
	var (
		header = e.Header
		body   = e.Body
	)

	if e.Header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			panic("NewPayload BaseFeePerGas overflow")
		}
	}

	return &types.ExecutionPayload{
		ParentHash:    gointerfaces.ConvertHashToH256(header.ParentHash),
		Coinbase:      gointerfaces.ConvertAddressToH160(header.Coinbase),
		StateRoot:     gointerfaces.ConvertHashToH256(header.Root),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(header.ReceiptHash),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(header.Bloom[:]),
		PrevRandao:    gointerfaces.ConvertHashToH256(header.MixDigest),
		BlockNumber:   header.Number.Uint64(),
		GasLimit:      header.GasLimit,
		GasUsed:       header.GasUsed,
		Timestamp:     header.Time,
		ExtraData:     header.Extra,
		BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(baseFee),
		BlockHash:     gointerfaces.ConvertHashToH256(header.BlockHashCL),
		Transactions:  body.Transactions,
	}
}

func (l *LightClient) processBeaconBlock(beaconBlock *cltypes.BeaconBlockBellatrix) error {
	if l.execution == nil {
		return nil
	}
	// If we recently imported the beacon block, skip.
	bcRoot, err := beaconBlock.HashTreeRoot()
	if err != nil {
		return err
	}
	if l.recentHashesCache.Contains(bcRoot) {
		return nil
	}
	// Save as recent
	l.recentHashesCache.Add(bcRoot, struct{}{})

	payloadHash := gointerfaces.ConvertHashToH256(beaconBlock.Body.ExecutionPayload.Header.BlockHashCL)

	payload := convertLightrpcExecutionPayloadToEthbacked(beaconBlock.Body.ExecutionPayload)

	_, err = l.execution.EngineNewPayloadV1(l.ctx, payload)
	if err != nil {
		return err
	}

	// Wait a bit
	time.Sleep(500 * time.Millisecond)
	_, err = l.execution.EngineForkChoiceUpdatedV1(l.ctx, &remote.EngineForkChoiceUpdatedRequest{
		ForkchoiceState: &remote.EngineForkChoiceState{
			HeadBlockHash:      payloadHash,
			SafeBlockHash:      payloadHash,
			FinalizedBlockHash: gointerfaces.ConvertHashToH256(l.finalizedEth1Hash),
		},
	})
	return err
}
