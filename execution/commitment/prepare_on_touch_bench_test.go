package commitment

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Benchmark_PrepareOnTouch(b *testing.B) {
	pk, upds := buildWhaleCorpus(whale1M())
	for b.Loop() {
		b.StopTimer()
		ms := NewMockState(b)
		require.NoError(b, ms.applyPlainUpdates(pk, upds))
		ts := sortedTriples(pk, upds)
		ps := newPreparedSplits(b, mockTrieCtxFactory(ms))

		tp := time.Now()
		for _, tr := range ts {
			require.NoError(b, ps.touch(tr.hk, tr.pk, tr.upd))
		}
		prepare := time.Since(tp)

		b.StartTimer()
		_, err := ps.process()
		b.StopTimer()
		require.NoError(b, err)
		b.ReportMetric(float64(prepare.Nanoseconds()), "prepare-ns")
		ps.release()
		b.StartTimer()
	}
}
