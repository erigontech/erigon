package vm

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// beneficiaryErrReader fails the account read for one address only, standing in
// for a domain read failure on a cold account.
type beneficiaryErrReader struct {
	state.StateReader
	fail accounts.Address
	err  error
}

func (r beneficiaryErrReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr == r.fail {
		return nil, r.err
	}
	return r.StateReader.ReadAccountData(addr)
}

// TestOpSelfdestruct_PropagatesBalanceError verifies that a state failure while
// crediting the beneficiary aborts the opcode instead of silently dropping the
// transferred balance.
func TestOpSelfdestruct_PropagatesBalanceError(t *testing.T) {
	t.Parallel()

	self := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	beneficiary := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))

	boom := errors.New("domain read failed")
	ibs := state.New(beneficiaryErrReader{
		StateReader: state.NewNoopReader(),
		fail:        beneficiary,
		err:         boom,
	})
	require.NoError(t, ibs.AddBalance(self, *uint256.NewInt(1000), 0))

	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.TestChainOsakaConfig, Config{})
	scope := &CallContext{Contract: *NewContract(self, self, self, uint256.Int{})}
	// The interpreter bumps cacheGen before each dispatch; without it the
	// zero-valued gen collides with cachedAddrGen and peekAddress returns a
	// stale address instead of reading the stack.
	scope.cacheGen++
	benVal := beneficiary.Value()
	scope.Stack.push(*new(uint256.Int).SetBytes(benVal[:]))

	_, _, err := opSelfdestruct(0, evm, scope)
	require.ErrorIs(t, err, boom)
}
