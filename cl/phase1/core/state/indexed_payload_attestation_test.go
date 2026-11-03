package state_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

func TestGetIndexedPayloadAttestationLimit(t *testing.T) {
	for _, minimal := range []bool{false, true} {
		t.Run(fmt.Sprint("minimal=", minimal), func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			if minimal {
				clparams.ApplyMinimalPreset(&cfg)
			}
			cfg.GloasForkEpoch = 0
			s := state.New(&cfg)
			s.SetVersion(clparams.GloasVersion)
			ptc := s.GetPtcWindow().Get(int(cfg.SlotsPerEpoch))
			ptc.Set(0, 3)
			ptc.Set(1, 1)
			ptc.Set(2, 2)
			for _, votes := range []int{0, 3} {
				t.Run(fmt.Sprint(votes), func(t *testing.T) {
					payload := &cltypes.PayloadAttestation{AggregationBits: solid.NewBitVector(int(cfg.PtcSize)), Data: &cltypes.PayloadAttestationData{}, Signature: [96]byte{9}}
					for i := range votes {
						require.NoError(t, payload.AggregationBits.SetBitAt(i, true))
					}
					indexed, err := s.GetIndexedPayloadAttestation(payload)
					require.NoError(t, err)
					indices := []uint64{}
					if votes > 0 {
						indices = []uint64{1, 2, 3}
					}
					expected := &cltypes.IndexedPayloadAttestation{AttestingIndices: solid.NewRawUint64List(int(cfg.PtcSize), indices), Data: payload.Data, Signature: payload.Signature}
					root, err := indexed.HashSSZ()
					require.NoError(t, err)
					expectedRoot, err := expected.HashSSZ()
					require.NoError(t, err)
					require.Equal(t, expectedRoot, root)
					require.Equal(t, int(cfg.PtcSize), indexed.AttestingIndices.Cap())
					require.Equal(t, votes, indexed.AttestingIndices.Length())
					for i, v := range indices {
						require.Equal(t, v, indexed.AttestingIndices.Get(i))
					}
				})
			}
		})
	}
}
