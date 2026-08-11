package forkchoice

import (
	"testing"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

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

func TestDrainPendingELPayloadsLimitLeavesRemainderQueued(t *testing.T) {
	f := &ForkChoiceStore{}
	for i := range 3 {
		f.RequeuePendingELPayload(PendingELPayload{Envelope: &cltypes.SignedExecutionPayloadEnvelope{
			Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: common.Hash{byte(i + 1)}},
		}})
	}

	first := f.DrainPendingELPayloadsLimit(2)
	require.Len(t, first, 2)
	require.Equal(t, common.Hash{1}, first[0].Envelope.Message.BeaconBlockRoot)
	require.Equal(t, common.Hash{2}, first[1].Envelope.Message.BeaconBlockRoot)
	remaining := f.DrainPendingELPayloads()
	require.Len(t, remaining, 1)
	require.Equal(t, common.Hash{3}, remaining[0].Envelope.Message.BeaconBlockRoot)
}
