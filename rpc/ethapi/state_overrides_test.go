package ethapi

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
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

	for i := range 500 {
		func(i int) {
			ibs := newVersionedCallState(state.NewNoopReader())
			defer ibs.Close()
			err := so.Override(ibs, vm.PrecompiledContracts{}, &chain.Rules{})
			require.EqualError(t, err, want, "iteration %d: error must be deterministic", i)
		}(i)
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
	ibs := newVersionedCallState(state.NewNoopReader())
	defer ibs.Close()
	err := so.Override(ibs, precompiles, &chain.Rules{})
	require.NoError(t, err)

	_, atSrc := precompiles[src]
	got, atDst := precompiles[dst]
	require.False(t, atSrc, "precompile must be removed from source")
	require.True(t, atDst, "precompile must be present at destination")
	require.Equal(t, stub.Name(), got.Name())
}

func TestStateOverridesPreserveStorageOnlyAccounts(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000001"))
	key := common.HexToHash("0x01")

	for _, tc := range []struct {
		name      string
		stateDiff bool
	}{
		{name: "state"},
		{name: "stateDiff", stateDiff: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := map[common.Hash]common.Hash{key: common.HexToHash("0x2a")}
			account := Account{State: &storage}
			if tc.stateDiff {
				account = Account{StateDiff: &storage}
			}

			overrides := StateOverrides{addr: account}
			ibs := newVersionedCallState(state.NewNoopReader())
			defer ibs.Close()

			rules := &chain.Rules{IsSpuriousDragon: true}
			require.NoError(t, overrides.Override(ibs, vm.PrecompiledContracts{}, rules))

			exists, err := ibs.Exist(addr)
			require.NoError(t, err)
			require.True(t, exists)

			value, err := ibs.GetState(addr, accounts.InternKey(key))
			require.NoError(t, err)
			require.Equal(t, uint64(0x2a), value.Uint64())
		})
	}
}

// An empty stateDiff writes no slot, so it must not create an absent account.
func TestStateOverrideEmptyStateDiffKeepsAccountAbsent(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000003"))
	ibs := newVersionedCallState(state.NewNoopReader())
	defer ibs.Close()
	empty := map[common.Hash]common.Hash{}
	overrides := StateOverrides{addr: Account{StateDiff: &empty}}
	require.NoError(t, overrides.Override(ibs, vm.PrecompiledContracts{}, &chain.Rules{IsSpuriousDragon: true}))

	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestStateOverridesBalanceDecoding(t *testing.T) {
	maxU256 := strings.Repeat("f", 64)
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	var so StateOverrides
	require.NoError(t, json.Unmarshal([]byte(`{"0x00000000000000000000000000000000000000aa":{"balance":"0x`+maxU256+`"}}`), &so))
	ibs := newVersionedCallState(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, so.Override(ibs, vm.PrecompiledContracts{}, &chain.Rules{}))
	balance, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, maxU256, balance.Hex()[2:])
}

func newVersionedCallState(reader state.StateReader) *state.IntraBlockState {
	ibs := state.New(reader)
	ibs.SetVersionMap(state.NewVersionMap(nil))
	ibs.SetNoConflictDetection()
	ibs.SetTxContext(0, 0)
	return ibs
}

func TestStateOverridesOnTheVersionedPath(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000002"))
	key := common.HexToHash("0x01")
	storage := map[common.Hash]common.Hash{key: common.HexToHash("0x2a")}
	balance := hexutil.U256(*uint256.NewInt(7))
	balancePtr := &balance
	overrides := StateOverrides{addr: Account{Balance: &balancePtr, State: &storage}}
	ibs := newVersionedCallState(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, overrides.Override(ibs, vm.PrecompiledContracts{}, &chain.Rules{IsSpuriousDragon: true}))
	got, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(7), got.Uint64())
	value, err := ibs.GetState(addr, accounts.InternKey(key))
	require.NoError(t, err)
	require.Equal(t, uint64(0x2a), value.Uint64())
	require.Equal(t, 1, ibs.TxnIndex(), "the call runs after the override transaction")
}

type storageReader struct {
	*state.NoopReader
	code    []byte
	nonce   uint64
	balance uint64
	storage map[accounts.StorageKey]uint256.Int
}

func (r storageReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	account := accounts.NewAccount()
	account.Incarnation = 1
	account.Nonce = r.nonce
	account.Balance.SetUint64(r.balance)
	if len(r.code) > 0 {
		account.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(r.code))
	}
	return &account, nil
}

func (r storageReader) ReadAccountCode(accounts.Address) ([]byte, error) { return r.code, nil }

func (r storageReader) ReadAccountStorage(_ accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	value, ok := r.storage[key]
	return value, ok, nil
}

func TestStateOverrideReplacesStoredSlotsOnTheVersionedPath(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000003"))
	changed, kept, dropped := common.HexToHash("0x01"), common.HexToHash("0x02"), common.HexToHash("0x03")
	reader := storageReader{storage: map[accounts.StorageKey]uint256.Int{
		accounts.InternKey(changed): *uint256.NewInt(5),
		accounts.InternKey(kept):    *uint256.NewInt(7),
		accounts.InternKey(dropped): *uint256.NewInt(9),
	}}
	storage := map[common.Hash]common.Hash{changed: common.HexToHash("0x2a"), kept: common.HexToHash("0x07")}
	overrides := StateOverrides{addr: Account{State: &storage}}
	ibs := newVersionedCallState(reader)
	defer ibs.Close()
	require.NoError(t, overrides.Override(ibs, vm.PrecompiledContracts{}, &chain.Rules{IsSpuriousDragon: true}))
	for key, want := range map[common.Hash]uint64{changed: 0x2a, kept: 7, dropped: 0} {
		value, err := ibs.GetState(addr, accounts.InternKey(key))
		require.NoError(t, err)
		require.Equal(t, want, value.Uint64(), "slot %x", key)
	}
}

func TestStateOverrideReplacesSlotsOfEarlierTransactionsOnTheVersionedPath(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000005"))
	written := accounts.InternKey(common.HexToHash("0x03"))
	ibs := newVersionedCallState(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.SetState(addr, written, *uint256.NewInt(9)))
	require.NoError(t, FinalizeCallTx(ibs, &chain.Rules{}))
	ibs.SetTxContext(0, 1)
	storage := map[common.Hash]common.Hash{common.HexToHash("0x01"): common.HexToHash("0x2a")}
	overrides := StateOverrides{addr: Account{State: &storage}}
	require.NoError(t, overrides.Override(ibs, vm.PrecompiledContracts{}, &chain.Rules{IsSpuriousDragon: true}))
	value, err := ibs.GetState(addr, written)
	require.NoError(t, err)
	require.True(t, value.IsZero())
}

func TestStateOverrideKeepsTheCodeOnTheVersionedPath(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000004"))
	code := []byte{0x60, 0x00, 0x54}
	storage := map[common.Hash]common.Hash{common.HexToHash("0x01"): common.HexToHash("0x2a")}
	overrides := StateOverrides{addr: Account{State: &storage}}
	ibs := newVersionedCallState(storageReader{code: code})
	defer ibs.Close()
	require.NoError(t, overrides.Override(ibs, vm.PrecompiledContracts{}, &chain.Rules{IsSpuriousDragon: true}))
	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	require.Equal(t, code, got)
	size, err := ibs.GetCodeSize(addr)
	require.NoError(t, err)
	require.Equal(t, len(code), size)
}

// On the versioned path the next transaction index of the same state reads the finalized
// writes, and the log index keeps counting across transactions.
func TestFinalizeCallTxPublishesWritesToNextTx(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x0201"))
	key := accounts.InternKey(common.HexToHash("0x11"))
	ibs := newVersionedCallState(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.SetState(addr, key, *uint256.NewInt(7)))
	ibs.AddLog(&types.Log{Address: addr.Value()})
	require.NoError(t, FinalizeCallTx(ibs, &chain.Rules{}))

	ibs.SetTxContext(0, 1)
	got, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(7), got)
	ibs.AddLog(&types.Log{Address: addr.Value()})
	require.Equal(t, hexutil.Uint(1), ibs.Logs()[1].Index)
}

type emptyAccountReader struct {
	*state.NoopReader
	addr accounts.Address
}

func (r emptyAccountReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr == r.addr {
		acc := accounts.NewAccount()
		return &acc, nil
	}
	return nil, nil
}

// An existing account a transaction touches and leaves empty is gone for the next transaction
// of the same request (EIP-161), as with FinalizeTx on the materializing path. A request can
// run on top of any block, the genesis block included.
func TestFinalizeCallTxRemovesTouchedEmptyAccount(t *testing.T) {
	for _, block := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("block=%d", block), func(t *testing.T) {
			addr := accounts.InternAddress(common.HexToAddress("0x0202"))
			ibs := state.New(emptyAccountReader{NoopReader: state.NewNoopReader(), addr: addr})
			defer ibs.Close()
			ibs.SetVersionMap(state.NewVersionMap(nil))
			ibs.SetNoConflictDetection()
			ibs.SetTxContext(block, 0)
			exists, err := ibs.Exist(addr)
			require.NoError(t, err)
			require.True(t, exists)
			require.NoError(t, ibs.TouchAccount(addr))
			require.NoError(t, FinalizeCallTx(ibs, &chain.Rules{IsSpuriousDragon: true}))

			ibs.SetTxContext(block, 1)
			exists, err = ibs.Exist(addr)
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}

// An account deleted under EIP-161 by one transaction, recreated empty by the next (a zero-value
// call to a precompile) and deleted again, stays gone for the transaction after that.
func TestFinalizeCallTxKeepsARecreatedEmptyAccountDeleted(t *testing.T) {
	for _, block := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("block=%d", block), func(t *testing.T) {
			addr := accounts.InternAddress(common.HexToAddress("0x0203"))
			ibs := state.New(emptyAccountReader{NoopReader: state.NewNoopReader(), addr: addr})
			defer ibs.Close()
			ibs.SetVersionMap(state.NewVersionMap(nil))
			ibs.SetNoConflictDetection()
			rules := &chain.Rules{IsSpuriousDragon: true}

			ibs.SetTxContext(block, 0)
			require.NoError(t, ibs.TouchAccount(addr))
			require.NoError(t, FinalizeCallTx(ibs, rules))

			ibs.SetTxContext(block, 1)
			exists, err := ibs.Exist(addr)
			require.NoError(t, err)
			require.False(t, exists)
			require.NoError(t, ibs.CreateAccount(addr, false))
			require.NoError(t, ibs.TouchAccount(addr))
			require.NoError(t, FinalizeCallTx(ibs, rules))

			ibs.SetTxContext(block, 2)
			exists, err = ibs.Exist(addr)
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}

func TestFinalizeCallTxRejectsAStateWithoutAVersionMap(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	ibs.SetVersionMap(nil)
	require.ErrorContains(t, FinalizeCallTx(ibs, &chain.Rules{}), "no version map")
}

// Where EIP-161 is active, an account that the overrides write and leave empty does not
// exist after them, with no storage, unless the override sets a non-zero slot.
func TestStateOverrideRemovesEmptiedAccounts(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000006"))
	slot := common.HexToHash("0x01")
	empty := hexutil.Bytes{}
	zero := hexutil.U256{}
	zeroPtr := &zero
	one := hexutil.U256(*uint256.NewInt(1))
	onePtr := &one
	slots := func(value string) *map[common.Hash]common.Hash {
		return &map[common.Hash]common.Hash{slot: common.HexToHash(value)}
	}
	stored := storageReader{balance: 5, storage: map[accounts.StorageKey]uint256.Int{accounts.InternKey(slot): *uint256.NewInt(9)}}
	contract := storageReader{code: []byte{0x00}, nonce: 1, storage: map[accounts.StorageKey]uint256.Int{accounts.InternKey(slot): *uint256.NewInt(9)}}
	nonce := hexutil.Uint64(0)
	for _, tc := range []struct {
		name      string
		reader    state.StateReader
		account   Account
		preEIP161 bool
		disabled  bool
		exists    bool
		value     uint64
	}{
		{name: "balance on a missing account", account: Account{Balance: &onePtr}, exists: true},
		{name: "empty code on a missing account", account: Account{Code: &empty}},
		{name: "empty state on a missing account", account: Account{State: &map[common.Hash]common.Hash{}}},
		{name: "zero slot on a missing account", account: Account{StateDiff: slots("0x00")}},
		{name: "non-zero slot on a missing account", account: Account{Code: &empty, StateDiff: slots("0x2a")}, exists: true, value: 0x2a},
		{name: "zero balance on a funded account", reader: stored, account: Account{Balance: &zeroPtr}},
		{name: "zero balance and a non-zero slot on a funded account", reader: stored, account: Account{Balance: &zeroPtr, State: slots("0x2a")}, exists: true, value: 0x2a},
		{name: "zero balance and a zero slot on a funded account", reader: stored, account: Account{Balance: &zeroPtr, StateDiff: slots("0x00")}},
		{name: "zero balance and a zero slot in state on a funded account", reader: stored, account: Account{Balance: &zeroPtr, State: slots("0x00")}},
		{name: "zero nonce and empty code on a contract", reader: contract, account: Account{Nonce: &nonce, Code: &empty}},
		{name: "empty code on a contract keeps its nonce", reader: contract, account: Account{Code: &empty}, exists: true, value: 9},
		{name: "zero balance on an existing empty account", reader: storageReader{}, account: Account{Balance: &zeroPtr}},
		{name: "empty code with EIP-161 disabled", account: Account{Code: &empty}, disabled: true, exists: true},
		{name: "empty code before EIP-161", account: Account{Code: &empty}, preEIP161: true, exists: true},
		{name: "empty state diff on an existing empty account", reader: storageReader{}, account: Account{StateDiff: &map[common.Hash]common.Hash{}}, exists: true},
	} {
		for _, amsterdam := range []bool{false, true} {
			if amsterdam && tc.preEIP161 {
				continue
			}
			t.Run(fmt.Sprintf("%s/amsterdam=%v", tc.name, amsterdam), func(t *testing.T) {
				reader := tc.reader
				if reader == nil {
					reader = state.NewNoopReader()
				}
				ibs := newVersionedCallState(reader)
				defer ibs.Close()
				overrides := StateOverrides{addr: tc.account}
				rules := &chain.Rules{IsSpuriousDragon: !tc.preEIP161, IsAmsterdam: amsterdam}
				if tc.disabled {
					rules.DisabledEIPs = []int{161}
				}
				require.NoError(t, overrides.Override(ibs, vm.PrecompiledContracts{}, rules))
				exists, err := ibs.Exist(addr)
				require.NoError(t, err)
				require.Equal(t, tc.exists, exists)
				value, err := ibs.GetState(addr, accounts.InternKey(slot))
				require.NoError(t, err)
				require.Equal(t, tc.value, value.Uint64())
			})
		}
	}
}

// A later transaction that funds an account the overrides removed creates it again, without
// the storage it had before.
func TestStateOverrideRemovedAccountIsCreatedAgain(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000007"))
	slot := accounts.InternKey(common.HexToHash("0x01"))
	zero := hexutil.U256{}
	zeroPtr := &zero
	ibs := newVersionedCallState(storageReader{balance: 5, storage: map[accounts.StorageKey]uint256.Int{slot: *uint256.NewInt(9)}})
	defer ibs.Close()
	rules := &chain.Rules{IsSpuriousDragon: true}
	overrides := StateOverrides{addr: Account{Balance: &zeroPtr}}
	require.NoError(t, overrides.Override(ibs, vm.PrecompiledContracts{}, rules))
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(3), tracing.BalanceChangeUnspecified))
	require.NoError(t, EndCallTx(ibs, rules, nil))
	balance, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(3), balance.Uint64())
	value, err := ibs.GetState(addr, slot)
	require.NoError(t, err)
	require.True(t, value.IsZero())
}

// Aura keeps the empty system address, so an override that leaves it empty keeps it too.
// Other accounts are removed as on any chain.
func TestStateOverrideKeepsAuraSystemAddress(t *testing.T) {
	t.Parallel()
	other := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000008"))
	zero := hexutil.U256{}
	zeroPtr := &zero
	ibs := newVersionedCallState(storageReader{balance: 5})
	defer ibs.Close()
	overrides := StateOverrides{params.SystemAddress: Account{Balance: &zeroPtr}, other: Account{Balance: &zeroPtr}}
	require.NoError(t, overrides.Override(ibs, vm.PrecompiledContracts{}, &chain.Rules{IsSpuriousDragon: true, IsAura: true}))
	exists, err := ibs.Exist(params.SystemAddress)
	require.NoError(t, err)
	require.True(t, exists)
	exists, err = ibs.Exist(other)
	require.NoError(t, err)
	require.False(t, exists)
}
