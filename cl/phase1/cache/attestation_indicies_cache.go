package cache

import (
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
)

var attestationIndiciesCache *lru.Cache[*solid.AttestationData, []uint64]

const attestationIndiciesCacheSize = 256

func LoadAttestatingIndicies(attestation *solid.AttestationData) ([]uint64, bool) {
	return attestationIndiciesCache.Get(attestation)
}

func StoreAttestation(attestation *solid.AttestationData, indicies []uint64) {
	attestationIndiciesCache.Add(attestation, indicies)
}
