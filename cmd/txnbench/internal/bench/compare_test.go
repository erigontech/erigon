package bench

import (
	"fmt"
	"strings"
	"testing"
)

func TestCompareResultsSmallDataset(t *testing.T) {
	for blocks := 1; blocks <= 10; blocks++ {
		t.Run(fmt.Sprint(blocks), func(t *testing.T) {
			var output BenchOutput
			for i := range blocks {
				output.Results = append(output.Results, TxBenchEntry{
					BlockNumber:    uint64(i + 1),
					FirstLatencyMs: 10,
					AvgLatencyMs:   2,
				})
			}

			lines := strings.Split(strings.TrimSuffix(CompareResults(output, output), "\n"), "\n")
			if got := len(lines) - 2; got != blocks {
				t.Fatalf("got %d cluster rows, want %d", got, blocks)
			}
			for i, row := range lines[2:] {
				got, _, _ := strings.Cut(row, "\t")
				want := fmt.Sprintf("%2d: %d–%d", i+1, i+1, i+1)
				if got != want {
					t.Errorf("cluster row %d has range %q, want %q", i+1, got, want)
				}
			}
		})
	}
}
