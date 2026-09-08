package benchmark

import (
	_ "embed"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

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
