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

	vmenv := benchConfig(b, 1_000_000_000)
	statedb := vmenv.IntraBlockState()
	deployContract(b, statedb, addrContract, code)

	// callComplete checks the call did work; only this benchmark also has a
	// rendered frame to check.
	ret, _, err := prepareAndCall(vmenv, addrContract, input)
	require.NoError(b, err)
	require.NotEmpty(b, ret)

	b.ReportAllocs()
	for b.Loop() {
		callComplete(b, vmenv, addrContract, input)
	}
}

// TestSnailtracerPathsAgree pins that the parallel path the benchmark measures
// renders the same frame as the materializing path, so a timing comparison
// between the two is comparing the same work.
func TestSnailtracerPathsAgree(t *testing.T) {
	code := common.FromHex(strings.TrimSpace(snailtracerHex))
	input := common.FromHex(snailtracerSelector)

	render := func(noMaterialize bool) []byte {
		vmenv := newBenchEnv(t, 1_000_000_000, noMaterialize)
		deployContract(t, vmenv.IntraBlockState(), addrContract, code)
		ret, _, err := prepareAndCall(vmenv, addrContract, input)
		require.NoError(t, err)
		require.NotEmpty(t, ret)
		return ret
	}

	require.Equal(t, render(false), render(true))
}
