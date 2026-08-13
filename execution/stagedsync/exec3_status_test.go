package stagedsync

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRestoreHeldBack(t *testing.T) {
	m := &execStatusList{pending: []int{10, 11, 12, 15}}
	m.ensureLen(15)
	m.restoreHeldBack([]int{3, 5, 7})
	require.Equal(t, []int{3, 5, 7, 10, 11, 12, 15}, m.pending)
}

func TestRestoreHeldBackEdgeCases(t *testing.T) {
	t.Run("empty holdBack leaves pending untouched", func(t *testing.T) {
		m := &execStatusList{pending: []int{4, 8}}
		m.restoreHeldBack(nil)
		require.Equal(t, []int{4, 8}, m.pending)
	})
	t.Run("empty pending", func(t *testing.T) {
		m := &execStatusList{}
		m.restoreHeldBack([]int{1, 2, 3})
		require.Equal(t, []int{1, 2, 3}, m.pending)
	})
	t.Run("interleaved holdBack and pending", func(t *testing.T) {
		m := &execStatusList{pending: []int{2, 6, 9}}
		m.restoreHeldBack([]int{1, 4, 7})
		require.Equal(t, []int{1, 2, 4, 6, 7, 9}, m.pending)
	})
}

// restoreHeldBack must produce exactly the same pending list as restoring each
// held-back tx with pushPending, across a range of holdBack/suffix shapes.
func TestRestoreHeldBackMatchesPushPending(t *testing.T) {
	shapes := []struct {
		holdBack []int
		suffix   []int
	}{
		{[]int{0, 1, 2}, []int{3, 4, 5, 6, 7}},
		{[]int{2, 4, 9}, []int{11, 20, 30}},
		{[]int{}, []int{5, 6}},
		{[]int{5, 6}, []int{}},
		{[]int{1, 3, 5, 7}, []int{8, 9, 10, 11, 12, 13}},
	}
	for i, s := range shapes {
		t.Run(fmt.Sprintf("shape%d", i), func(t *testing.T) {
			maxIdx := 0
			for _, v := range append(append([]int{}, s.holdBack...), s.suffix...) {
				if v > maxIdx {
					maxIdx = v
				}
			}
			want := &execStatusList{pending: append([]int{}, s.suffix...)}
			want.ensureLen(maxIdx)
			for _, tx := range s.holdBack {
				want.pushPending(tx)
			}
			got := &execStatusList{pending: append([]int{}, s.suffix...)}
			got.ensureLen(maxIdx)
			got.restoreHeldBack(append([]int{}, s.holdBack...))
			require.Equal(t, want.pending, got.pending)
		})
	}
}

func holdBackBenchInput(total int) (holdBack, suffix []int) {
	h := total / 2
	holdBack = make([]int, h)
	for i := range holdBack {
		holdBack[i] = i
	}
	suffix = make([]int, total-h)
	for i := range suffix {
		suffix[i] = h + i
	}
	return
}

// BenchmarkHoldBackReinsertion contrasts the previous per-tx pushPending restore
// (O(H*R)) with the bulk merge (O(H+R)) on the issue's reinsertion shape:
// a sorted low-index held-back prefix in front of a large untouched suffix.
func BenchmarkHoldBackReinsertion(b *testing.B) {
	for _, total := range []int{1024, 2048, 4096, 8192} {
		holdBack, suffix := holdBackBenchInput(total)
		b.Run(fmt.Sprintf("perTx/%d", total), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				m := &execStatusList{pending: append([]int(nil), suffix...)}
				m.ensureLen(total)
				b.StartTimer()
				for _, tx := range holdBack {
					m.pushPending(tx)
				}
			}
		})
		b.Run(fmt.Sprintf("merge/%d", total), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				m := &execStatusList{pending: append([]int(nil), suffix...)}
				m.ensureLen(total)
				hb := append([]int(nil), holdBack...)
				b.StartTimer()
				m.restoreHeldBack(hb)
			}
		})
	}
}
