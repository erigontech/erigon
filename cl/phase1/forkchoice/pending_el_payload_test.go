package forkchoice

import (
	"testing"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type requeueRootOnlyForkGraph struct {
	fork_graph.ForkGraph
}

func (requeueRootOnlyForkGraph) WithRetainedBlock(_ common.Hash, fn func()) bool {
	fn()
	return true
}

func (requeueRootOnlyForkGraph) IsBlockRetained(common.Hash) bool { return true }

func (requeueRootOnlyForkGraph) HasEnvelope(common.Hash) bool {
	panic("requeue must not reacquire fork graph lifecycle state")
}

func TestRequeuePendingELPayloadDoesNotRecheckEnvelopeWhileRootIsRetained(t *testing.T) {
	f := &ForkChoiceStore{forkGraph: requeueRootOnlyForkGraph{}}
	root := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root}}

	f.RequeuePendingELPayload(PendingELPayload{
		Block:    &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 1}},
		Envelope: envelope,
	})

	pending := f.DrainPendingELPayloads()
	require.Len(t, pending, 1)
	require.Equal(t, root, pending[0].Root)
	require.Equal(t, uint64(1), pending[0].Slot)
	require.Nil(t, pending[0].Block)
	require.Nil(t, pending[0].Envelope)
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

func TestDrainPendingELPayloadsPrioritizingHighestSlotPreservesOldestProgress(t *testing.T) {
	f := &ForkChoiceStore{}
	for i := range 5 {
		f.addPendingELPayload(&cltypes.SignedBeaconBlock{
			Block: &cltypes.BeaconBlock{Slot: uint64(i + 1)},
		}, nil)
	}

	drained := f.DrainPendingELPayloadsPrioritizingHighestSlot(3)
	require.Equal(t, []uint64{1, 2, 5}, []uint64{
		drained[0].Block.Block.Slot,
		drained[1].Block.Block.Slot,
		drained[2].Block.Block.Slot,
	})
	remaining := f.DrainPendingELPayloads()
	require.Equal(t, []uint64{3, 4}, []uint64{
		remaining[0].Block.Block.Slot,
		remaining[1].Block.Block.Slot,
	})
}

func TestDrainPendingELPayloadsPrioritizingHighestSlotWithLimitOne(t *testing.T) {
	f := &ForkChoiceStore{}
	for i := range 3 {
		f.addPendingELPayload(&cltypes.SignedBeaconBlock{
			Block: &cltypes.BeaconBlock{Slot: uint64(i + 1)},
		}, nil)
	}

	drained := f.DrainPendingELPayloadsPrioritizingHighestSlot(1)
	require.Equal(t, uint64(3), drained[0].Block.Block.Slot)
	remaining := f.DrainPendingELPayloads()
	require.Equal(t, []uint64{1, 2}, []uint64{
		remaining[0].Block.Block.Slot,
		remaining[1].Block.Block.Slot,
	})
}

func TestDrainPendingELPayloadsPrioritizingHighestSlotBoundaries(t *testing.T) {
	f := &ForkChoiceStore{}
	require.Nil(t, f.DrainPendingELPayloadsPrioritizingHighestSlot(1))

	f.addPendingELPayload(&cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{Slot: 1},
	}, nil)
	require.Nil(t, f.DrainPendingELPayloadsPrioritizingHighestSlot(0))

	drained := f.DrainPendingELPayloadsPrioritizingHighestSlot(2)
	require.Len(t, drained, 1)
	require.Equal(t, uint64(1), drained[0].Block.Block.Slot)
	require.Empty(t, f.DrainPendingELPayloads())
}

func TestDrainPendingELPayloadsPrioritizesHighestSlotAfterRequeue(t *testing.T) {
	f := &ForkChoiceStore{}
	queueSlot := func(slot byte) {
		f.addPendingELPayload(
			&cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: uint64(slot)}},
			&cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: common.Hash{slot}}},
		)
	}
	for slot := byte(1); slot <= 5; slot++ {
		queueSlot(slot)
	}

	drained := f.DrainPendingELPayloadsPrioritizingHighestSlot(3)
	queueSlot(6)
	for _, pending := range drained {
		f.RequeuePendingELPayload(pending)
	}

	drained = f.DrainPendingELPayloadsPrioritizingHighestSlot(3)
	require.Equal(t, []uint64{3, 4, 6}, []uint64{
		drained[0].Block.Block.Slot,
		drained[1].Block.Block.Slot,
		drained[2].Block.Block.Slot,
	})
}

func TestDrainPendingELPayloadsPrioritizingHighestSlotPreservesMiddleOrder(t *testing.T) {
	f := &ForkChoiceStore{}
	for _, slot := range []uint64{1, 2, 3, 9, 4, 5} {
		f.addPendingELPayload(&cltypes.SignedBeaconBlock{
			Block: &cltypes.BeaconBlock{Slot: slot},
		}, nil)
	}

	drained := f.DrainPendingELPayloadsPrioritizingHighestSlot(3)
	require.Equal(t, []uint64{1, 2, 9}, []uint64{
		drained[0].Block.Block.Slot,
		drained[1].Block.Block.Slot,
		drained[2].Block.Block.Slot,
	})
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

func TestPendingELPayloadDeduplicationRetainsKnownSlot(t *testing.T) {
	f := &ForkChoiceStore{}
	root := common.HexToHash("0x1234")
	f.queuePendingELPayload(PendingELPayload{Root: root})
	f.queuePendingELPayload(PendingELPayload{Root: root, Slot: 9})

	payloads := f.DrainPendingELPayloads()
	require.Len(t, payloads, 1)
	require.Equal(t, uint64(9), payloads[0].Slot)
}

func TestRequeuePendingELPayloadUsesBlockSlot(t *testing.T) {
	f := &ForkChoiceStore{}
	root := common.HexToHash("0x1234")
	f.RequeuePendingELPayload(PendingELPayload{
		Root:  root,
		Slot:  99,
		Block: &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 7}},
	})

	payloads := f.DrainPendingELPayloads()
	require.Len(t, payloads, 1)
	require.Equal(t, uint64(7), payloads[0].Slot)
}
