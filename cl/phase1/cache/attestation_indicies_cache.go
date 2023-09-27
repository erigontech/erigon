package cache

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	"github.com/ledgerwatch/erigon/cl/utils"
)

var attestationIndiciesCache *lru.Cache[common.Hash, []uint64]

const attestationIndiciesCacheSize = 1024

func LoadAttestatingIndicies(attestation *solid.AttestationData, aggregationBits []byte) ([]uint64, bool) {
	bitsHash := utils.Keccak256(aggregationBits)
	hash, err := attestation.HashSSZ()
	if err != nil {
		return nil, false
	}
	return attestationIndiciesCache.Get(utils.Keccak256(hash[:], bitsHash[:]))
}

func StoreAttestation(attestation *solid.AttestationData, aggregationBits []byte, indicies []uint64) {
	bitsHash := utils.Keccak256(aggregationBits)
	hash, err := attestation.HashSSZ()
	if err != nil {
		return
	}
	attestationIndiciesCache.Add(utils.Keccak256(hash[:], bitsHash[:]), indicies)
}
