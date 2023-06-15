package cache_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/cache"
	"github.com/stretchr/testify/require"
)

func TestAttestationsCache(t *testing.T) {
	input := []uint64{1}
	a := solid.NewAttestationData()
	cache.StoreAttestation(&a, []byte{2}, []uint64{1})
	output, valid := cache.LoadAttestatingIndicies(&a, []byte{2})
	require.True(t, valid)
	require.Equal(t, input, output)
}
