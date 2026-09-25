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

func TestStorageOverride(t *testing.T) {
	t.Parallel()

	contract := accounts.InternAddress(common.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"))
	key := accounts.InternKey(common.HexToHash("0xf1e9242398de526b8dd9c25d38e65fbb01926b8940377762d7884b8b0dcdc3b0"))
	override := uint256.MustFromHex("0xf6a7831804efd2cd0a")

	_, tx, sd := state.NewTestRwTx(t)
	ibs := state.New(state.NewReaderV3(sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer ibs.Close()

	ibs.SetTxContext(35547779, 196)
	committed, err := ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.True(t, committed.IsZero())

	ibs.SetStorageOverride(contract, key, *override)

	value, err := ibs.GetState(contract, key)
	require.NoError(t, err)
	require.Equal(t, override.String(), value.String())
	committed, err = ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.Equal(t, override.String(), committed.String())

	// The transaction's own write wins over the override, while the override stays
	// the committed original that SSTORE prices against.
	written := new(uint256.Int).AddUint64(override, 1234)
	require.NoError(t, ibs.SetState(contract, key, *written))
	value, err = ibs.GetState(contract, key)
	require.NoError(t, err)
	require.Equal(t, written.String(), value.String())
	committed, err = ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.Equal(t, override.String(), committed.String())

	// An override belongs to one transaction only.
	ibs.SetTxContext(35547779, 197)
	committed, err = ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.True(t, committed.IsZero())
}

type storageChange struct{ prev, new uint256.Int }

func newOverrideTestIBS(t *testing.T, versioned bool, opts ...state.Option) *state.IntraBlockState {
	_, tx, sd := state.NewTestRwTx(t)
	reader := state.NewReaderV3(sd.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	ibs := state.New(reader, opts...)
	if versioned {
		ibs.SetVersionMap(state.NewVersionMap(nil))
	}
	t.Cleanup(ibs.Close)
	return ibs
}

// The override is the value the transaction starts from, so SSTORE's no-op
// check compares against it rather than against the storage underneath.
func TestStorageOverrideIsThePreviousValueOfAWrite(t *testing.T) {
	t.Parallel()

	contract := accounts.InternAddress(common.HexToAddress("0x00000000001f8b68515EfB546542397d3293CCfd"))
	key := accounts.InternKey(common.HexToHash("0x65c95177950b486c2071bf2304da1427b9136564150fb97266ffb318b03a71cc"))
	override := *uint256.NewInt(1)

	for _, versioned := range []bool{false, true} {
		t.Run(map[bool]string{false: "serial", true: "versioned"}[versioned], func(t *testing.T) {
			t.Parallel()
			ibs := newOverrideTestIBS(t, versioned)
			var changes []storageChange
			ibs.SetHooks(&tracing.Hooks{OnStorageChange: func(_ accounts.Address, _ accounts.StorageKey, prev, new uint256.Int) {
				changes = append(changes, storageChange{prev, new})
			}})
			ibs.SetTxContext(33851236, 89)
			ibs.SetStorageOverride(contract, key, override)

			require.NoError(t, ibs.SetState(contract, key, override))
			require.Empty(t, changes)

			require.NoError(t, ibs.SetState(contract, key, uint256.Int{}))
			require.Equal(t, []storageChange{{prev: override}}, changes)
			value, err := ibs.GetState(contract, key)
			require.NoError(t, err)
			require.True(t, value.IsZero(), "a write of the underlying value must shadow the override, got %s", value.String())
		})
	}
}

func TestStorageOverrideEndsWithTheTransaction(t *testing.T) {
	t.Parallel()

	contract := accounts.InternAddress(common.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"))
	key := accounts.InternKey(common.HexToHash("0xf1e9242398de526b8dd9c25d38e65fbb01926b8940377762d7884b8b0dcdc3b0"))
	override := *uint256.MustFromHex("0xf6a7831804efd2cd0a")

	for name, end := range map[string]func(*state.IntraBlockState) error{
		"next transaction": func(ibs *state.IntraBlockState) error {
			ibs.SetTxContext(35547779, 197)
			return nil
		},
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
			ibs := newOverrideTestIBS(t, false)
			ibs.SetTxContext(35547779, 196)
			ibs.SetStorageOverride(contract, key, override)
			require.NoError(t, end(ibs))

			committed, err := ibs.GetCommittedState(contract, key)
			require.NoError(t, err)
			require.True(t, committed.IsZero(), "override leaked past the transaction: %s", committed.String())
		})
	}
}

// Under parallel execution the override also shadows an earlier transaction's
// write to the slot.
func TestStorageOverrideShadowsTheVersionMap(t *testing.T) {
	t.Parallel()

	contract := accounts.InternAddress(common.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"))
	key := accounts.InternKey(common.HexToHash("0xf1e9242398de526b8dd9c25d38e65fbb01926b8940377762d7884b8b0dcdc3b0"))
	override := *uint256.MustFromHex("0xf6a7831804efd2cd0a")

	_, tx, sd := state.NewTestRwTx(t)
	vm := state.NewVersionMap(nil)
	vm.WriteStorage(contract, key, state.Version{BlockNum: 35547779, TxIndex: 195}, *uint256.NewInt(7), true)
	ibs := state.NewWithVersionMap(state.NewReaderV3(sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})), vm)
	defer ibs.Close()
	ibs.SetTxContext(35547779, 196)
	ibs.SetStorageOverride(contract, key, override)

	value, err := ibs.GetState(contract, key)
	require.NoError(t, err)
	require.Equal(t, override.String(), value.String())
	committed, err := ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.Equal(t, override.String(), committed.String())
}

type fixedOverrider state.StorageOverrideTable

func (o fixedOverrider) StorageOverrides() state.StorageOverrideTable {
	return state.StorageOverrideTable(o)
}

func TestSetTxContextInstallsAttachedOverrides(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"))
	key := accounts.InternKey(common.HexToHash("0xf1e9242398de526b8dd9c25d38e65fbb01926b8940377762d7884b8b0dcdc3b0"))
	value := *uint256.NewInt(0x1234)
	committed := func(ibs *state.IntraBlockState) uint256.Int {
		v, err := ibs.GetCommittedState(addr, key)
		require.NoError(t, err)
		return v
	}

	ibs := newOverrideTestIBS(t, false, state.WithStorageOverrides(fixedOverrider{
		{BlockNum: 35547779, TxIndex: 196}: {{Address: addr, Key: key, Value: value}},
	}))

	ibs.SetTxContext(35547779, 196)
	require.Equal(t, value, committed(ibs))

	ibs.SetTxContext(35547779, 197)
	require.Equal(t, uint256.Int{}, committed(ibs), "the next transaction must not inherit the override")

	ibs.Reset()
	ibs.SetTxContext(35547779, 196)
	require.Equal(t, value, committed(ibs), "the overrider survives Reset")

}
