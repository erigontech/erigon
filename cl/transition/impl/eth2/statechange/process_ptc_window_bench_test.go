package statechange_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"
)

func BenchmarkProcessPtcWindowFreshState(b *testing.B) {
	base := ptcWindowTestState(b, 500_000)
	useFreshValidatorCaches(b)
	b.ResetTimer()

	for range b.N {
		b.StopTimer()
		s, err := base.Copy()
		require.NoError(b, err)
		b.StartTimer()
		require.NoError(b, statechange.ProcessPtcWindow(s))
	}
}
