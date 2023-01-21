package state_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// Prev: 151849172
// Curr: 5463452
func BenchmarkStateRootNonCached(b *testing.B) {
	for i := 0; i < b.N; i++ {
		base := state.GetEmptyBeaconState()
		base.HashSSZ()
	}
}

// Prev: 13953
// Curr: 2093
func BenchmarkStateRootCached(b *testing.B) {
	// Re-use same fields
	base := state.GetEmptyBeaconState()
	for i := 0; i < b.N; i++ {
		base.HashSSZ()
	}
}
