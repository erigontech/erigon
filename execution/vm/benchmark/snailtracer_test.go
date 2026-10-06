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

// TestCreateStormAgreesWithObjectCache pins that serving reads from resident
// stateObjects under noMaterialize renders the same state as rebuilding them
// from cells on every read: a CREATE2 storm writes nonce, code and balance for
// each child, which is where a stale object would show.
func TestCreateStormAgreesWithObjectCache(t *testing.T) {
	// 5b 58 61 0100 80 600080f5 600152 600056:
	// JUMPDEST; PC; PUSH2 0x100; DUP1; PUSH1 0 PUSH1 0 CREATE2; PUSH1 1 MSTORE; PUSH1 0 JUMP
	code := common.FromHex("5b58610100806000600080f5600152600056")

	run := func(cacheObjects bool) (int, uint64) {
		vmenv := newBenchEnv(t, 20_000_000, true)
		ibs := vmenv.IntraBlockState()
		if cacheObjects {
			ibs.SetNoConflictDetection()
		}
		deployContract(t, ibs, addrContract, code)
		_, left, err := prepareAndCall(vmenv, addrContract, nil)
		require.Error(t, err, "the storm runs until it is out of gas")
		writes := ibs.VersionedWrites()
		require.NotNil(t, writes)
		require.Positive(t, writes.Count(), "the storm must have written cells")
		return writes.Count(), left.Total()
	}

	plain, plainGas := run(false)
	cached, cachedGas := run(true)
	require.Equal(t, plainGas, cachedGas, "the object cache must not change gas")
	require.Equal(t, plain, cached, "the object cache must not change the written cells")
}
