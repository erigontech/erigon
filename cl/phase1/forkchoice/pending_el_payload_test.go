package forkchoice

import (
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"
)

type queuedPruneForkGraph struct {
	payloadVoteForkGraph
	primaryRoot   common.Hash
	outerEntered  chan struct{}
	pruneQueued   chan struct{}
	outerReturned chan struct{}
}

func (g *queuedPruneForkGraph) WithRetainedBlock(root common.Hash, fn func(func(common.Hash) bool)) bool {
	if root == g.primaryRoot {
		close(g.outerEntered)
		<-g.pruneQueued
		fn(g.IsBlockRetained)
		close(g.outerReturned)
		return true
	}
	<-g.outerReturned
	fn(g.IsBlockRetained)
	return true
}

func TestMarkPayloadStatusAndGasLimitIfRetained(t *testing.T) {
	root := common.HexToHash("0x1234")
	executionHash := common.HexToHash("0xabcd")
	gasLimit := uint64(36_000_000)

	for _, test := range []struct {
		name     string
		retained bool
	}{
		{name: "retained", retained: true},
		{name: "pruned", retained: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache, err := lru.New[common.Hash, uint64](1)
			require.NoError(t, err)
			f := &ForkChoiceStore{
				forkGraph:                payloadVoteForkGraph{retained: &test.retained},
				executionPayloadGasLimit: cache,
			}

			status, retained := f.MarkPayloadStatusAndGasLimitIfRetained(
				root,
				executionHash,
				execution_client.PayloadStatusNotValidated,
				gasLimit,
			)

			require.Equal(t, test.retained, retained)
			if test.retained {
				require.Equal(t, execution_client.PayloadStatus(execution_client.PayloadStatusNotValidated), status)
				got, ok := f.GetExecutionPayloadGasLimit(executionHash)
				require.True(t, ok)
				require.Equal(t, gasLimit, got)
			} else {
				_, ok := f.GetExecutionPayloadGasLimit(executionHash)
				require.False(t, ok)
			}
		})
	}
}

func TestInvalidPayloadStatusDoesNotNestRetainedBlockGuard(t *testing.T) {
	primaryRoot := common.HexToHash("0x1234")
	sharedRoot := common.HexToHash("0x5678")
	executionHash := common.HexToHash("0xabcd")
	graph := &queuedPruneForkGraph{
		primaryRoot:   primaryRoot,
		outerEntered:  make(chan struct{}),
		pruneQueued:   make(chan struct{}),
		outerReturned: make(chan struct{}),
	}
	f := &ForkChoiceStore{
		forkGraph: graph,
		executionPayloadRoots: map[common.Hash]map[common.Hash]struct{}{
			executionHash: {primaryRoot: {}, sharedRoot: {}},
		},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.MarkPayloadStatusAndGasLimitIfRetained(primaryRoot, executionHash, execution_client.PayloadStatusInvalidated, 36_000_000)
	}()

	<-graph.outerEntered
	close(graph.pruneQueued)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("invalid payload update nested the retained-block guard behind a queued prune")
	}
}

func TestStalePayloadRetryAfterPruneIsDropped(t *testing.T) {
	root := common.HexToHash("0x1234")
	retained := false
	accepted := map[common.Hash]bool{}
	f := &ForkChoiceStore{forkGraph: payloadVoteForkGraph{hasEnvelope: true, retained: &retained, acceptedPayloads: accepted}}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}}

	_, applied := f.MarkPayloadStatusIfRetained(root, common.HexToHash("0xabcd"), execution_client.PayloadStatusNotValidated)
	require.False(t, applied)
	require.Empty(t, accepted)
	f.RequeuePendingELPayload(PendingELPayload{Block: &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 1}}, Envelope: envelope})
	require.Empty(t, f.DrainPendingELPayloads())
}

func TestPendingELPayloadsDropOldestAtCap(t *testing.T) {
	f := &ForkChoiceStore{}

	for i := range maxPendingELPayloads + 1 {
		f.addPendingELPayload(&cltypes.SignedBeaconBlock{
			Block: &cltypes.BeaconBlock{Slot: uint64(i)},
		}, nil)
	}

	payloads := f.DrainPendingELPayloads()
	require.Len(t, payloads, maxPendingELPayloads)
	require.Equal(t, uint64(1), payloads[0].Block.Block.Slot)
	require.Equal(t, uint64(maxPendingELPayloads), payloads[len(payloads)-1].Block.Block.Slot)
}

func TestDrainPendingELPayloadsReleasesLargeBackingArray(t *testing.T) {
	f := &ForkChoiceStore{}

	for range pendingELPayloadsShrinkCap + 1 {
		f.addPendingELPayload(&cltypes.SignedBeaconBlock{}, nil)
	}

	payloads := f.DrainPendingELPayloads()
	require.Len(t, payloads, pendingELPayloadsShrinkCap+1)
	require.Nil(t, f.pendingELPayloads)
}

func TestDrainPendingELPayloadsLimitPreservesRemainingOrder(t *testing.T) {
	f := &ForkChoiceStore{}
	for i := range 5 {
		f.addPendingELPayload(&cltypes.SignedBeaconBlock{
			Block: &cltypes.BeaconBlock{Slot: uint64(i + 1)},
		}, nil)
	}

	first := f.DrainPendingELPayloadsLimit(2)
	require.Equal(t, []uint64{1, 2}, []uint64{first[0].Block.Block.Slot, first[1].Block.Block.Slot})
	remaining := f.DrainPendingELPayloads()
	require.Equal(t, []uint64{3, 4, 5}, []uint64{
		remaining[0].Block.Block.Slot,
		remaining[1].Block.Block.Slot,
		remaining[2].Block.Block.Slot,
	})
}

func TestPendingELPayloadsDeduplicateByEnvelopeRoot(t *testing.T) {
	f := &ForkChoiceStore{}
	root := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BeaconBlockRoot: root,
		},
	}

	f.addPendingELPayload(&cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 1}}, envelope)
	f.addPendingELPayload(&cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 2}}, envelope)

	payloads := f.DrainPendingELPayloads()
	require.Len(t, payloads, 1)
	require.Equal(t, uint64(1), payloads[0].Block.Block.Slot)
}
