package state_test

import (
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"testing"
)

// Curr: 4358340
func BenchmarkStateRootNonCached(b *testing.B) {
	for i := 0; i < b.N; i++ {
		base := state.GetEmptyBeaconState()
		base.HashSSZ()
	}
}

// Prev: 1400
// Curr: 139.4
func BenchmarkStateRootCached(b *testing.B) {
	// Re-use same fields
	base := state.GetEmptyBeaconState()
	for i := 0; i < b.N; i++ {
		base.HashSSZ()
	}
}
