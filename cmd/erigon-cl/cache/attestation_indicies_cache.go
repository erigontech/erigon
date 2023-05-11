package cache

import (
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/lru"
)

var attestationIndiciesCache *lru.Cache[*cltypes.AttestationData, []uint64]

const attestationIndiciesCacheSize = 256

func LoadAttestatingIndicies(attestation *cltypes.AttestationData) ([]uint64, bool) {
	return attestationIndiciesCache.Get(attestation)
}

func StoreAttestation(attestation *cltypes.AttestationData, indicies []uint64) {
	attestationIndiciesCache.Add(attestation, indicies)
}
