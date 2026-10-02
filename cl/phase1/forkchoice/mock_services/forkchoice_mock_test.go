package mock_services

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func TestForkChoiceStorageMockStoresEnvelopeWithNilMap(t *testing.T) {
	mock := &ForkChoiceStorageMock{}
	root := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = root

	require.NotPanics(t, func() {
		require.NoError(t, mock.OnExecutionPayload(t.Context(), envelope, false, false))
	})
	require.True(t, mock.HasEnvelope(root))
	persisted, err := mock.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	require.Same(t, envelope, persisted)
}

func TestForkChoiceStorageMockSetEnvelopeWithNilMap(t *testing.T) {
	mock := &ForkChoiceStorageMock{}
	root := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{}

	require.NotPanics(t, func() { mock.SetEnvelope(root, envelope) })
	require.True(t, mock.HasEnvelope(root))
	persisted, err := mock.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	require.Same(t, envelope, persisted)
}

func TestForkChoiceStorageMockEnvelopeAccessIsConcurrentSafe(t *testing.T) {
	mock := &ForkChoiceStorageMock{}
	root := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = root

	var workers sync.WaitGroup
	for range 32 {
		workers.Add(4)
		go func() {
			defer workers.Done()
			require.NoError(t, mock.OnExecutionPayload(t.Context(), envelope, false, false))
		}()
		go func() {
			defer workers.Done()
			mock.SetEnvelope(root, envelope)
		}()
		go func() {
			defer workers.Done()
			mock.HasEnvelope(root)
		}()
		go func() {
			defer workers.Done()
			_, err := mock.ReadEnvelopeFromDisk(root)
			require.NoError(t, err)
		}()
	}
	workers.Wait()
	require.True(t, mock.HasEnvelope(root))
}
