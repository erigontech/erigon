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

type sentinelRequestOpts struct {
	respChan chan interface{}
	errChan  chan error
	req      interface{}
	sc       consensusrpc.SentinelClient
}

type singleReq func(context.Context, sentinelRequestOpts)

func sendSentinelRequest(ctx context.Context, srFn singleReq, opts sentinelRequestOpts) (interface{}, error) {
	newReqTicker := time.NewTicker(1000 * time.Millisecond)
	respChan := make(chan interface{})
	errChan := make(chan error)
	opts.respChan = respChan
	opts.errChan = errChan
	for {
		select {
		case <-newReqTicker.C:
			go srFn(ctx, opts)
		case err := <-errChan:
			log.Error("Error received from peer", "error", err)
		case resp := <-respChan:
			log.Info("Response received.")
			return resp, nil
		}
	}
}

func GetCheckpointBlock(ctx context.Context, sc consensusrpc.SentinelClient, cp *cltypes.BeaconState) (*cltypes.SignedBeaconBlockBellatrix, error) {
	root := cp.FinalizedCheckpoint.Root
	log.Info("Getting checkpoint block by root", "root", hex.EncodeToString(root[:]))

	// Send request
	resp, err := sendSentinelRequest(ctx, getCPBlockSRFn, sentinelRequestOpts{
		req: root,
		sc:  sc,
	})
	if err != nil {
		return nil, fmt.Errorf("unexpected error sending sentinel request: %v", err)
	}
	ret, ok := resp.(*cltypes.SignedBeaconBlockBellatrix)
	if !ok {
		return nil, fmt.Errorf("unable to cast response to type: %+v", resp)
	}
	return ret, nil
}

func getCPBlockSRFn(ctx context.Context, opts sentinelRequestOpts) {
	// Assert that the request is a single root.
	root := opts.req.([32]byte)

	log.Info("Getting checkpoint block by root", "root", hex.EncodeToString(root[:]))
	finalizedRootSlice := [][32]byte{root}
	// Request for blocks by root given the finalized checkpoint root.
	resp, err := rpc.SendBeaconBlocksByRootReq(ctx, finalizedRootSlice, opts.sc)
	if err != nil {
		opts.errChan <- err
		return
	}
	if len(resp) != 1 {
		opts.errChan <- fmt.Errorf("unexpected response length, got %d want %d", len(resp), 1)
		return
	}
	result, ok := resp[0].(*cltypes.SignedBeaconBlockBellatrix)
	if !ok {
		opts.errChan <- fmt.Errorf("unable to cast object: %+v into block", resp[0])
	}
	opts.respChan <- result
}

func GetStatus(ctx context.Context, sc consensusrpc.SentinelClient, req *cltypes.Status) (*cltypes.Status, error) {
	// Send request
	resp, err := sendSentinelRequest(ctx, getStatusSRFn, sentinelRequestOpts{
		req: req,
		sc:  sc,
	})
	if err != nil {
		return nil, fmt.Errorf("unexpected error sending sentinel request: %v", err)
	}
	ret, ok := resp.(*cltypes.Status)
	if !ok {
		return nil, fmt.Errorf("unable to cast response to type: %+v", resp)
	}
	return ret, nil
}

func getStatusSRFn(ctx context.Context, opts sentinelRequestOpts) {
	// Assert that the request is a single root.
	req := opts.req.(*cltypes.Status)

	log.Info("Getting status for epoch", "epoch", req.FinalizedEpoch)
	resp, err := rpc.SendStatusReq(ctx, req, opts.sc)
	if err != nil {
		opts.errChan <- err
		return
	}
	opts.respChan <- resp
}

func GetBlocksByRange(ctx context.Context, sc consensusrpc.SentinelClient, start, numBlocks uint64) ([]*cltypes.SignedBeaconBlockBellatrix, error) {
	var i uint64

	totalBlocks := []*cltypes.SignedBeaconBlockBellatrix{}
	// Fetch blocks in batches of 10 to avoid hitting max message size.
	for i = 0; i < numBlocks; i += 10 {
		req := cltypes.BeaconBlocksByRangeRequest{
			StartSlot: start + i,
			Count:     10,
		}

		// Send request
		resp, err := sendSentinelRequest(ctx, getBlocksByRangeSRFn, sentinelRequestOpts{
			req: &req,
			sc:  sc,
		})
		if err != nil {
			return nil, fmt.Errorf("unexpected error sending sentinel request: %v", err)
		}
		ret, ok := resp.([]*cltypes.SignedBeaconBlockBellatrix)
		if !ok {
			return nil, fmt.Errorf("unable to cast response to type: %+v", resp)
		}
		for j := 0; j < len(ret); j++ {
			totalBlocks = append(totalBlocks, ret[j])
		}
	}
	return totalBlocks, nil
}

func getBlocksByRangeSRFn(ctx context.Context, opts sentinelRequestOpts) {
	// Assert that the request the correct type.
	req := opts.req.(*cltypes.BeaconBlocksByRangeRequest)

	log.Info("Getting blocks by range", "num blocks", req.Count)
	log.Info("Getting blocks by range", "start slot", req.StartSlot)

	// Request for blocks by root given the finalized checkpoint root.
	resp, err := rpc.SendBeaconBlocksByRangeReq(ctx, req.StartSlot, req.Count, opts.sc)
	if err != nil {
		opts.errChan <- err
		return
	}
	if len(resp) != int(req.Count) {
		opts.errChan <- fmt.Errorf("unexpected response length, got %d want %d", len(resp), int(req.Count))
		return
	}
	result := make([]*cltypes.SignedBeaconBlockBellatrix, len(resp))
	// Type assert that the returned object are the correct type.
	for _, obj := range resp {
		block, ok := obj.(*cltypes.SignedBeaconBlockBellatrix)
		if !ok {
			log.Error("unable to cast object into block", "object", obj)
			return
		}
		result = append(result, block)
	}
	opts.respChan <- result
}
