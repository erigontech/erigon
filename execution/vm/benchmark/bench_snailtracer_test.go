package benchmark

import (
	_ "embed"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

//go:embed testdata/snailtracer.hex
var snailtracerHex string

// snailtracerSelector is Benchmark(), the entry point that renders one pixel.
const snailtracerSelector = "30627b7c"

// BenchmarkSnailtracer renders with the Snailtracer ray tracer, the standard
// compute-heavy EVM benchmark: a long single-frame run dominated by arithmetic,
// memory and jumps rather than by state access.
func BenchmarkSnailtracer(b *testing.B) {
	code := common.FromHex(strings.TrimSpace(snailtracerHex))
	input := common.FromHex(snailtracerSelector)

	cfg, statedb := benchConfig(b, 1_000_000_000)
	deployContract(statedb, addrContract, code)

	// callComplete checks the call did work; only this benchmark also has a
	// rendered frame to check.
	ret, _, err := prepareAndCall(cfg, addrContract, input)
	require.NoError(b, err)
	require.NotEmpty(b, ret)

	b.ReportAllocs()
	for b.Loop() {
		callComplete(b, cfg, statedb, addrContract, input)
	}
}
