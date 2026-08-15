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
	require.False(t, f.hasPendingELPayload(root))
}

func TestPendingELPayloadsEvictionUpdatesRootMembership(t *testing.T) {
	f := &ForkChoiceStore{}
	firstRoot := common.Hash{31: 1}

	for i := range maxPendingELPayloads + 1 {
		root := common.Hash{30: byte((i + 1) >> 8), 31: byte(i + 1)}
		f.addPendingELPayload(nil, &cltypes.SignedExecutionPayloadEnvelope{
			Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: root},
		})
	}

	require.False(t, f.hasPendingELPayload(firstRoot))
	require.True(t, f.hasPendingELPayload(common.Hash{
		30: byte((maxPendingELPayloads + 1) >> 8), 31: byte((maxPendingELPayloads + 1) & 0xff),
	}))
}
