package vm

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestExpSmallOperandBoundaries(t *testing.T) {
	t.Parallel()
	modulus := new(big.Int).Lsh(big.NewInt(1), 256)
	for _, base := range []uint64{0, 1, 2, 3, 7, 13, 136279841, 1 << 31, 1 << 32, 1 << 63, ^uint64(0)} {
		for power := range uint64(66) {
			scope := &CallContext{}
			scope.Stack.push(*uint256.NewInt(power))
			scope.Stack.push(*uint256.NewInt(base))
			_, _, err := opExp(0, nil, scope)
			require.NoError(t, err)
			want := new(big.Int).Exp(new(big.Int).SetUint64(base), new(big.Int).SetUint64(power), modulus)
			got := scope.Stack.popCopy()
			require.Zero(t, want.Cmp(got.ToBig()), "base=%d exponent=%d", base, power)
		}
	}
}
