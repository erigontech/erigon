package clcore

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/rpc/consensusrpc"
)

const blocksPerRequest = 1024

func GetBlocksFromCheckpoint(ctx context.Context, sc *consensusrpc.SentinelClient, cp *cltypes.BeaconState, fd [4]byte) ([]*cltypes.SignedBeaconBlockBellatrix, error) {
	sr := &cltypes.Status{
		ForkDigest:     fd,
		FinalizedRoot:  cp.FinalizedCheckpoint.Root,
		FinalizedEpoch: cp.FinalizedCheckpoint.Epoch,
		HeadRoot:       cp.LatestBlockHeader.Root,
		HeadSlot:       cp.LatestBlockHeader.Slot,
	}
	status, err := rpc.SendStatusReq(ctx, sr, *sc)
	// TODO(issues/5965): Confirm that the current response matches our expected slot given the genisis time.
	if err != nil {
		return nil, fmt.Errorf("unable to receive external status: %v", err)
	}

	numBlocksNeeded := int(status.HeadSlot - cp.LatestBlockHeader.Slot)
	// We are ahead or at the same slot as the other client.
	if numBlocksNeeded <= 0 {
		return nil, nil
	}
	// TODO(issues/5965): Add logic to support this case?
	if numBlocksNeeded > blocksPerRequest {
		return nil, fmt.Errorf("too far behind head: our slot %d, head slot %d", cp.LatestBlockHeader.Slot, status.HeadSlot)
	}

	// Request the most recent blocks from the checkpoint.
	resp, err := rpc.SendBeaconBlocksByRangeReq(ctx, cp.LatestBlockHeader.Slot, uint64(numBlocksNeeded), *sc)
	if err != nil {
		return nil, fmt.Errorf("unable to receive BeaconBlocksByRange: %v", err)
	}

	result := make([]*cltypes.SignedBeaconBlockBellatrix, len(resp))
	// Type assert that the returned object are the correct type.
	for _, obj := range resp {
		block, ok := obj.(*cltypes.SignedBeaconBlockBellatrix)
		if !ok {
			return nil, fmt.Errorf("unable to cast object: %+v into block", obj)
		}
		result = append(result, block)
	}
	return result, nil
}
