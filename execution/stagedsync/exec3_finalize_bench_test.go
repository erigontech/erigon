package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/state"
)

func BenchmarkCalcFees(b *testing.B) {
	for _, sc := range []struct {
		name  string
		build func() *testFinalizeScenario
	}{
		{"pre_london", simpleTransferScenario},
		{"london", londonTransferScenario},
	} {
		for _, bc := range []struct {
			name     string
			recredit bool
		}{
			{"first_credit", false},
			{"redundant_recredit", true},
		} {
			b.Run(sc.name+"/"+bc.name, func(b *testing.B) {
				r := newFeeCreditRound(b, sc.build())
				var credited *state.WriteSet
				if bc.recredit {
					require.NotNil(b, r.run(b))
					credited = r.credited()
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tip, _, err := r.result.calcFees(r.task, r.vm, r.reader, r.rules, credited)
					if err != nil {
						b.Fatal(err)
					}
					feeCreditSink = tip
					// The apply loop recycles these maps, so the benchmark must
					// too, or the emit arm is measured against a cold pool.
					tip.ReleaseMaps()
				}
			})
		}
	}
}
