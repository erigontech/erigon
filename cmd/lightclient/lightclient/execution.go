package lightclient

import (
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
)

func convertLightrpcExecutionPayloadToEthbacked(e *cltypes.Eth1Block) (*types.ExecutionPayload, error) {
	var baseFee *uint256.Int
	header, err := e.RlpHeader()
	if err != nil {
		return nil, err
	}

	if header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			panic("NewPayload BaseFeePerGas overflow")
		}
	}

	res := &types.ExecutionPayload{
		Version:       1,
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
		BlockHash:     gointerfaces.ConvertHashToH256(e.BlockHash),
		Transactions:  e.Transactions,
		Withdrawals:   privateapi.ConvertWithdrawalsToRpc(e.Withdrawals),
	}
	if e.Withdrawals != nil {
		res.Version = 2
		res.Withdrawals = privateapi.ConvertWithdrawalsToRpc(e.Withdrawals)
	}

	return res, nil
}

func (l *LightClient) processBeaconBlock(beaconBlock *cltypes.BeaconBlock) error {
	if l.execution == nil && l.executionClient == nil {
		return nil
	}
	// If we recently imported the beacon block, skip.
	bcRoot, err := beaconBlock.HashSSZ()
	if err != nil {
		return err
	}
	if l.recentHashesCache.Contains(bcRoot) {
		return nil
	}
	// Save as recent
	l.recentHashesCache.Add(bcRoot, struct{}{})

	payloadHash := gointerfaces.ConvertHashToH256(beaconBlock.Body.ExecutionPayload.BlockHash)

	payload, err := convertLightrpcExecutionPayloadToEthbacked(beaconBlock.Body.ExecutionPayload)
	if err != nil {
		return err
	}
	if l.execution != nil {
		_, err = l.execution.EngineNewPayload(l.ctx, payload)
		if err != nil {
			return err
		}
	}

	if l.executionClient != nil {
		_, err = l.executionClient.EngineNewPayload(l.ctx, payload)
		if err != nil {
			return err
		}
	}

	// Wait a bit
	time.Sleep(500 * time.Millisecond)
	if l.execution != nil {
		_, err = l.execution.EngineForkChoiceUpdated(l.ctx, &remote.EngineForkChoiceUpdatedRequest{
			ForkchoiceState: &remote.EngineForkChoiceState{
				HeadBlockHash:      payloadHash,
				SafeBlockHash:      payloadHash,
				FinalizedBlockHash: gointerfaces.ConvertHashToH256(l.finalizedEth1Hash),
			},
		})
		if err != nil {
			return err
		}
	}
	if l.executionClient != nil {
		_, err = l.executionClient.EngineForkChoiceUpdated(l.ctx, &remote.EngineForkChoiceUpdatedRequest{
			ForkchoiceState: &remote.EngineForkChoiceState{
				HeadBlockHash:      payloadHash,
				SafeBlockHash:      payloadHash,
				FinalizedBlockHash: gointerfaces.ConvertHashToH256(l.finalizedEth1Hash),
			},
		})
		if err != nil {
			return err
		}
	}
	return err
}
