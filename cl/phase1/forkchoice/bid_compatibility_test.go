package forkchoice

import (
	"context"
	"sync"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type blockingBidCompatibilityForkGraph struct {
	fork_graph.ForkGraph
	header  *cltypes.BeaconBlockHeader
	block   *cltypes.SignedBeaconBlock
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *blockingBidCompatibilityForkGraph) GetHeader(common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	return g.header, g.header != nil
}

func (g *blockingBidCompatibilityForkGraph) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	if g.started != nil {
		g.once.Do(func() { close(g.started) })
		<-g.release
	}
	return g.block, g.block != nil
}

func (g *blockingBidCompatibilityForkGraph) HasEnvelope(common.Hash) bool {
	return false
}

func TestIsBuilderBidCompatibleWithHeadRejectsHeadFlipDuringLookup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		newRoot   common.Hash
		newStatus cltypes.PayloadStatus
	}{
		{name: "root changed", newRoot: common.HexToHash("0x2000"), newStatus: cltypes.PayloadStatusFull},
		{name: "payload status changed", newRoot: common.HexToHash("0x1000"), newStatus: cltypes.PayloadStatusEmpty},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headRoot := common.HexToHash("0x1000")
			graph, bid := testBidCompatibilityForkGraph(headRoot)
			graph.started = make(chan struct{})
			graph.release = make(chan struct{})
			store := &ForkChoiceStore{headHash: headRoot, headPayloadStatus: cltypes.PayloadStatusFull, forkGraph: graph}
			result := make(chan bool, 1)
			errs := make(chan error, 1)
			go func() {
				compatible, err := store.IsBuilderBidCompatibleWithHead(t.Context(), bid)
				result <- compatible
				errs <- err
			}()
			<-graph.started
			store.mu.Lock()
			store.headHash = tc.newRoot
			store.headPayloadStatus = tc.newStatus
			store.mu.Unlock()
			close(graph.release)

			require.NoError(t, <-errs)
			require.False(t, <-result)
		})
	}
}

func TestIsBuilderBidCompatibleWithHeadAcceptsStableHead(t *testing.T) {
	headRoot := common.HexToHash("0x1000")
	graph, bid := testBidCompatibilityForkGraph(headRoot)
	graph.header.Slot = bid.Slot - 1
	store := &ForkChoiceStore{headHash: headRoot, headPayloadStatus: cltypes.PayloadStatusFull, forkGraph: graph}

	compatible, err := store.IsBuilderBidCompatibleWithHead(context.Background(), bid)

	require.NoError(t, err)
	require.True(t, compatible)
}

func TestIsBuilderBidCompatibleWithHeadRejectsDerivedDecisionFlip(t *testing.T) {
	headRoot := common.HexToHash("0x1000")
	graph, bid := testBidCompatibilityForkGraph(headRoot)
	graph.header.Slot = bid.Slot - 1
	graph.started = make(chan struct{})
	graph.release = make(chan struct{})
	store := &ForkChoiceStore{headHash: headRoot, headPayloadStatus: cltypes.PayloadStatusFull, forkGraph: graph}
	store.payloadDataAvailabilityVote.Store(headRoot, [clparams.PtcSize]int8{})
	result := make(chan bool, 1)
	errs := make(chan error, 1)
	go func() {
		compatible, err := store.IsBuilderBidCompatibleWithHead(t.Context(), bid)
		result <- compatible
		errs <- err
	}()
	<-graph.started
	store.payloadDataAvailabilityVote.Delete(headRoot)
	close(graph.release)

	require.NoError(t, <-errs)
	require.False(t, <-result)
}

func TestIsBuilderBidCompatibleWithHeadFailsClosedWithoutHeadBlock(t *testing.T) {
	headRoot := common.HexToHash("0x1000")
	graph, bid := testBidCompatibilityForkGraph(headRoot)
	graph.block = nil
	store := &ForkChoiceStore{headHash: headRoot, headPayloadStatus: cltypes.PayloadStatusFull, forkGraph: graph}

	compatible, err := store.IsBuilderBidCompatibleWithHead(t.Context(), bid)

	require.ErrorContains(t, err, "head block unavailable")
	require.False(t, compatible)
}

func testBidCompatibilityForkGraph(headRoot common.Hash) (*blockingBidCompatibilityForkGraph, *cltypes.ExecutionPayloadBid) {
	parentPayload := common.HexToHash("0x3000")
	headPayload := common.HexToHash("0x4000")
	headBid := &cltypes.ExecutionPayloadBid{ParentBlockHash: parentPayload, BlockHash: headPayload}
	headBlock := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Body: &cltypes.BeaconBody{
		Version:                   clparams.GloasVersion,
		SignedExecutionPayloadBid: &cltypes.SignedExecutionPayloadBid{Message: headBid},
	}}}
	graph := &blockingBidCompatibilityForkGraph{
		header: &cltypes.BeaconBlockHeader{Slot: 98, ParentRoot: common.HexToHash("0x5000")},
		block:  headBlock,
	}
	bid := &cltypes.ExecutionPayloadBid{Slot: 100, ParentBlockRoot: headRoot, ParentBlockHash: headPayload}
	return graph, bid
}
