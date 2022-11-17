package clcore

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/rpc/consensusrpc"
	"github.com/ledgerwatch/log/v3"
)

const blocksPerRequest = 1024

// Debug function to recieve test packets on the req/resp domain.
func sendRequest(ctx context.Context, s consensusrpc.SentinelClient, req *consensusrpc.RequestData) {
	newReqTicker := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
		case <-newReqTicker.C:
			go func() {

			}()
		}
	}
}

func GetCheckpointBlock(ctx context.Context, sc consensusrpc.SentinelClient, cp *cltypes.BeaconState, fd [4]byte) (*cltypes.SignedBeaconBlockBellatrix, error) {
	// Request for blocks by root given the finalized checkpoint root.
	finalizedRootSlice := [][32]byte{cp.FinalizedCheckpoint.Root}

	newReqTicker := time.NewTicker(1000 * time.Millisecond)

	respChan := make(chan *cltypes.SignedBeaconBlockBellatrix)
	errChan := make(chan error)
	for {
		select {
		case <-newReqTicker.C:
			go func() {
				log.Info("Getting checkpoint block by root", "root", hex.EncodeToString(cp.FinalizedCheckpoint.Root[:]))
				resp, err := rpc.SendBeaconBlocksByRootReq(ctx, finalizedRootSlice, sc)
				if err != nil {
					errChan <- err
					return
				}
				if len(resp) != 1 {
					errChan <- fmt.Errorf("unexpected response length, got %d want %d", len(resp), 1)
					return
				}
				result, ok := resp[0].(*cltypes.SignedBeaconBlockBellatrix)
				if !ok {
					errChan <- fmt.Errorf("unable to cast object: %+v into block", resp[0])
				}
				respChan <- result
			}()
		case err := <-errChan:
			log.Error("Error received from peer", "error", err)
		case block := <-respChan:
			log.Info("Block received by root")
			return block, nil
		}
	}
}
