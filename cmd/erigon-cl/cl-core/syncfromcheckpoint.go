package clcore

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/rpc/consensusrpc"
	"github.com/ledgerwatch/log/v3"
)

const blocksPerRequest = 1024

func GetCheckpointBlock(ctx context.Context, sc consensusrpc.SentinelClient, cp *cltypes.BeaconState, fd [4]byte) (*cltypes.SignedBeaconBlockBellatrix, error) {
	// Request for blocks by root given the finalized checkpoint root.
	finalizedRootSlice := [][32]byte{cp.FinalizedCheckpoint.Root}

	var resp []cltypes.ObjectSSZ
	var err error
	for {
		log.Info("Getting checkpoint block by root", "root", hex.EncodeToString(cp.FinalizedCheckpoint.Root[:]))
		resp, err = rpc.SendBeaconBlocksByRootReq(ctx, finalizedRootSlice, sc)
		if err != nil && strings.Contains(err.Error(), "no peers") {
			log.Info("Retrying request, no peers found", "retrying", err)
		} else if err != nil {
			return nil, fmt.Errorf("unable to receive BeaconBlocksByRoot: %v", err)
		} else {
			break
		}
		time.Sleep(10 * time.Second)
	}

	if len(resp) != 1 {
		return nil, fmt.Errorf("unexpected response length, got %d want %d", len(resp), 1)
	}
	result, ok := resp[0].(*cltypes.SignedBeaconBlockBellatrix)
	if !ok {
		return nil, fmt.Errorf("unable to cast object: %+v into block", resp[0])
	}
	return result, nil
}
