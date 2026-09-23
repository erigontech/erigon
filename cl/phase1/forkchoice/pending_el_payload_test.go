package forkchoice

import (
	"testing"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

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
