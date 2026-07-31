package state

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestIsValidIndexedAttestationRejectsOversizedGloasIndicesBeforeLookup(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	limit := int(cfg.MaxValidatorsPerCommittee * cfg.MaxCommitteesPerSlot)
	indices := make([]uint64, limit+1)
	for i := range indices {
		indices[i] = uint64(i)
	}
	attestation := cltypes.NewIndexedAttestationWithConfig(clparams.GloasVersion, &cfg)
	attestation.AttestingIndices = solid.NewRawUint64List(limit, indices)

	valid, err := IsValidIndexedAttestation(New(&cfg), attestation)
	require.False(t, valid)
	require.ErrorContains(t, err, "too many attesting indices")
}
