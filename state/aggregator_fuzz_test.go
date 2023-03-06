//go:build !nofuzz

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Fuzz_BtreeIndex_Allocation(f *testing.F) {
	f.Add(uint64(1_000_000), uint64(1024))
	f.Fuzz(func(t *testing.T, keyCount, M uint64) {
		if keyCount < M*4 || M < 4 {
			t.Skip()
		}
		bt := newBtAlloc(keyCount, M, false)
		bt.traverseDfs()
		require.GreaterOrEqual(t, bt.N, keyCount)

		require.LessOrEqual(t, float64(bt.N-keyCount)/float64(bt.N), 0.05)

	})
}
