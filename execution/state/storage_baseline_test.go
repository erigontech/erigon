package state_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestStorageBaseline(t *testing.T) {
	t.Parallel()

	contract := accounts.InternAddress(common.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"))
	key := accounts.InternKey(common.HexToHash("0xf1e9242398de526b8dd9c25d38e65fbb01926b8940377762d7884b8b0dcdc3b0"))
	baseline := uint256.MustFromHex("0xf6a7831804efd2cd0a")

	_, tx, sd := state.NewTestRwTx(t)
	ibs := state.New(state.NewReaderV3(sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer ibs.Close()

	ibs.SetTxContext(35547779, 196)
	committed, err := ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.True(t, committed.IsZero())

	ibs.SetStorageBaseline(contract, key, *baseline)

	value, err := ibs.GetState(contract, key)
	require.NoError(t, err)
	require.Equal(t, baseline.String(), value.String())
	committed, err = ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.Equal(t, baseline.String(), committed.String())

	// The transaction's own write wins over the baseline, while the baseline stays
	// the committed original that SSTORE prices against.
	written := new(uint256.Int).AddUint64(baseline, 1234)
	require.NoError(t, ibs.SetState(contract, key, *written))
	value, err = ibs.GetState(contract, key)
	require.NoError(t, err)
	require.Equal(t, written.String(), value.String())
	committed, err = ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.Equal(t, baseline.String(), committed.String())

	// A baseline belongs to one transaction only.
	ibs.SetTxContext(35547779, 197)
	committed, err = ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.True(t, committed.IsZero())
}

type storageChange struct{ prev, new uint256.Int }

func newBaselineTestIBS(t *testing.T, versioned bool) *state.IntraBlockState {
	_, tx, sd := state.NewTestRwTx(t)
	reader := state.NewReaderV3(sd.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	var ibs *state.IntraBlockState
	if versioned {
		ibs = state.NewWithVersionMap(reader, state.NewVersionMap(nil))
	} else {
		ibs = state.New(reader)
	}
	t.Cleanup(ibs.Close)
	return ibs
}

// The baseline is the value the transaction starts from, so SSTORE's no-op
// check compares against it rather than against the storage underneath.
func TestStorageBaselineIsThePreviousValueOfAWrite(t *testing.T) {
	t.Parallel()

	contract := accounts.InternAddress(common.HexToAddress("0x00000000001f8b68515EfB546542397d3293CCfd"))
	key := accounts.InternKey(common.HexToHash("0x65c95177950b486c2071bf2304da1427b9136564150fb97266ffb318b03a71cc"))
	baseline := *uint256.NewInt(1)

	for _, versioned := range []bool{false, true} {
		t.Run(map[bool]string{false: "serial", true: "versioned"}[versioned], func(t *testing.T) {
			t.Parallel()
			ibs := newBaselineTestIBS(t, versioned)
			var changes []storageChange
			ibs.SetHooks(&tracing.Hooks{OnStorageChange: func(_ accounts.Address, _ accounts.StorageKey, prev, new uint256.Int) {
				changes = append(changes, storageChange{prev, new})
			}})
			ibs.SetTxContext(33851236, 89)
			ibs.SetStorageBaseline(contract, key, baseline)

			require.NoError(t, ibs.SetState(contract, key, baseline))
			require.Empty(t, changes)

			require.NoError(t, ibs.SetState(contract, key, uint256.Int{}))
			require.Equal(t, []storageChange{{prev: baseline}}, changes)
			value, err := ibs.GetState(contract, key)
			require.NoError(t, err)
			require.True(t, value.IsZero(), "a write of the underlying value must shadow the baseline, got %s", value.String())
		})
	}
}

func TestStorageBaselineEndsWithTheTransaction(t *testing.T) {
	t.Parallel()

	contract := accounts.InternAddress(common.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"))
	key := accounts.InternKey(common.HexToHash("0xf1e9242398de526b8dd9c25d38e65fbb01926b8940377762d7884b8b0dcdc3b0"))
	baseline := *uint256.MustFromHex("0xf6a7831804efd2cd0a")

	for name, end := range map[string]func(*state.IntraBlockState) error{
		"FinalizeTx": func(ibs *state.IntraBlockState) error {
			return ibs.FinalizeTx(&chain.Rules{}, state.NewNoopWriter())
		},
		"Reset": func(ibs *state.IntraBlockState) error {
			ibs.Reset()
			return nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ibs := newBaselineTestIBS(t, false)
			ibs.SetTxContext(35547779, 196)
			ibs.SetStorageBaseline(contract, key, baseline)
			require.NoError(t, end(ibs))

			committed, err := ibs.GetCommittedState(contract, key)
			require.NoError(t, err)
			require.True(t, committed.IsZero(), "baseline leaked past the transaction: %s", committed.String())
		})
	}
}

// Under parallel execution the baseline also shadows an earlier transaction's
// write to the slot.
func TestStorageBaselineShadowsTheVersionMap(t *testing.T) {
	t.Parallel()

	contract := accounts.InternAddress(common.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"))
	key := accounts.InternKey(common.HexToHash("0xf1e9242398de526b8dd9c25d38e65fbb01926b8940377762d7884b8b0dcdc3b0"))
	baseline := *uint256.MustFromHex("0xf6a7831804efd2cd0a")

	_, tx, sd := state.NewTestRwTx(t)
	vm := state.NewVersionMap(nil)
	vm.WriteStorage(contract, key, state.Version{BlockNum: 35547779, TxIndex: 195}, *uint256.NewInt(7), true)
	ibs := state.NewWithVersionMap(state.NewReaderV3(sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})), vm)
	defer ibs.Close()
	ibs.SetTxContext(35547779, 196)
	ibs.SetStorageBaseline(contract, key, baseline)

	value, err := ibs.GetState(contract, key)
	require.NoError(t, err)
	require.Equal(t, baseline.String(), value.String())
	committed, err := ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.Equal(t, baseline.String(), committed.String())
}
