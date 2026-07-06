package ethapi

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

type stubPrecompile struct{ name string }

func (s stubPrecompile) RequiredGas([]byte) uint64  { return 0 }
func (s stubPrecompile) Run([]byte) ([]byte, error) { return nil, nil }
func (s stubPrecompile) Name() string               { return s.name }

// TestStateOverrides_MovePrecompileDeterministicError verifies that when multiple
// non-precompile accounts carry MovePrecompileToAddress, the error always names
// the lexicographically smallest address rather than a random one from map iteration.
func TestStateOverrides_MovePrecompileDeterministicError(t *testing.T) {
	addr1 := common.HexToAddress("0x0100000000000000000000000000000000000000")
	addr2 := common.HexToAddress("0x0200000000000000000000000000000000000000")
	target := common.HexToAddress("0xc200000000000000000000000000000000000000")

	so := StateOverrides{
		accounts.InternAddress(addr1): Account{MovePrecompileTo: &target},
		accounts.InternAddress(addr2): Account{MovePrecompileTo: &target},
	}

	want := fmt.Sprintf("account %s is not a precompile", addr1)

	for i := 0; i < 500; i++ {
		ibs := state.New(state.NewNoopReader())
		err := so.Override(ibs, vm.PrecompiledContracts{}, &chain.Rules{})
		require.EqualError(t, err, want, "iteration %d: error must be deterministic", i)
	}
}

func TestStateOverrides_MovePrecompileSuccess(t *testing.T) {
	srcAddr := common.HexToAddress("0x0000000000000000000000000000000000000001")
	dstAddr := common.HexToAddress("0xd000000000000000000000000000000000000001")

	src := accounts.InternAddress(srcAddr)
	dst := accounts.InternAddress(dstAddr)
	stub := stubPrecompile{name: "ecrecover"}

	so := StateOverrides{
		src: Account{MovePrecompileTo: &dstAddr},
	}

	precompiles := vm.PrecompiledContracts{src: stub}
	ibs := state.New(state.NewNoopReader())
	err := so.Override(ibs, precompiles, &chain.Rules{})
	require.NoError(t, err)

	_, atSrc := precompiles[src]
	got, atDst := precompiles[dst]
	require.False(t, atSrc, "precompile must be removed from source")
	require.True(t, atDst, "precompile must be present at destination")
	require.Equal(t, stub.Name(), got.Name())
}
