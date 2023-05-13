package cache

import (
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
)

func init() {
	var err error
	if attestationIndiciesCache, err = lru.New[*cltypes.AttestationData, []uint64]("attestationIndiciesCacheSize", attestationIndiciesCacheSize); err != nil {
		panic(err)
	}
}
