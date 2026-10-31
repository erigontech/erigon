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
	"github.com/erigontech/erigon/execution/state"
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
			ibs := state.New(state.NewNoopReader())
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
	ibs := state.New(state.NewNoopReader())
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
		versioned bool
	}{
		{name: "state"},
		{name: "stateDiff", stateDiff: true},
		{name: "state/versioned", versioned: true},
		{name: "stateDiff/versioned", stateDiff: true, versioned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := map[common.Hash]common.Hash{key: common.HexToHash("0x2a")}
			account := Account{State: &storage}
			if tc.stateDiff {
				account = Account{StateDiff: &storage}
			}

			overrides := StateOverrides{addr: account}
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			if tc.versioned {
				ibs.SetVersionMap(state.NewVersionMap(nil))
				ibs.SetNoConflictDetection()
				ibs.SetTxContext(0, 0)
			}

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

// An empty stateDiff writes no slot, so it must not create an absent account on either path.
func TestStateOverrideEmptyStateDiffKeepsAccountAbsent(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress(common.HexToAddress("0x1000000000000000000000000000000000000003"))
	for _, versioned := range []bool{false, true} {
		ibs := state.New(state.NewNoopReader())
		if versioned {
			ibs.SetVersionMap(state.NewVersionMap(nil))
			ibs.SetNoConflictDetection()
			ibs.SetTxContext(0, 0)
		}
		empty := map[common.Hash]common.Hash{}
		overrides := StateOverrides{addr: Account{StateDiff: &empty}}
		require.NoError(t, overrides.Override(ibs, vm.PrecompiledContracts{}, &chain.Rules{IsSpuriousDragon: true}))

		exists, err := ibs.Exist(addr)
		require.NoError(t, err)
		require.False(t, exists, "versioned=%v", versioned)
		ibs.Close()
	}
}

func TestStateOverridesBalanceDecoding(t *testing.T) {
	maxU256 := strings.Repeat("f", 64)
	addr := accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
	var so StateOverrides
	require.NoError(t, json.Unmarshal([]byte(`{"0x00000000000000000000000000000000000000aa":{"balance":"0x`+maxU256+`"}}`), &so))
	ibs := state.New(state.NewNoopReader())
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
	storage map[accounts.StorageKey]uint256.Int
}

func (r storageReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	account := accounts.NewAccount()
	account.Incarnation = 1
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

// Only code and movePrecompileToAddress change what runs at a precompile address.
func TestStateOverridesKeepPrecompileUnlessCodeOrMove(t *testing.T) {
	t.Parallel()

	identity := accounts.InternAddress(common.HexToAddress("0x04"))
	modexp := accounts.InternAddress(common.HexToAddress("0x05"))
	moveTo := accounts.InternAddress(common.HexToAddress("0x1111"))
	rules := &chain.Rules{IsByzantium: true}

	for _, tc := range []struct {
		name     string
		override string
		kept     bool
		movedTo  accounts.Address
	}{
		{name: "balance", override: `{"balance":"0x1"}`, kept: true},
		{name: "nonce", override: `{"nonce":"0x1"}`, kept: true},
		{name: "state", override: `{"state":{"0x0000000000000000000000000000000000000000000000000000000000000001":"0x0000000000000000000000000000000000000000000000000000000000000002"}}`, kept: true},
		{name: "stateDiff", override: `{"stateDiff":{"0x0000000000000000000000000000000000000000000000000000000000000001":"0x0000000000000000000000000000000000000000000000000000000000000002"}}`, kept: true},
		{name: "empty", override: `{}`, kept: true},
		{name: "unknown field", override: `{"balanse":"0x1"}`, kept: true},
		{name: "null code", override: `{"code":null,"balance":"0x1"}`, kept: true},
		{name: "code", override: `{"code":"0x60006000fd"}`},
		{name: "empty code", override: `{"code":"0x"}`},
		{name: "code with balance", override: `{"code":"0x00","balance":"0x1"}`},
		{name: "move", override: `{"movePrecompileToAddress":"0x0000000000000000000000000000000000001111"}`, movedTo: moveTo},
		{name: "move with balance", override: `{"movePrecompileToAddress":"0x0000000000000000000000000000000000001111","balance":"0x1"}`, movedTo: moveTo},
		{name: "move with code", override: `{"movePrecompileToAddress":"0x0000000000000000000000000000000000001111","code":"0x00"}`, movedTo: moveTo},
		{name: "move onto precompile", override: `{"movePrecompileToAddress":"0x0000000000000000000000000000000000000005"}`, movedTo: modexp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var so StateOverrides
			require.NoError(t, json.Unmarshal([]byte(`{"0x0000000000000000000000000000000000000004":`+tc.override+`}`), &so))

			precompiles := vm.ActivePrecompiledContracts(rules)
			want := precompiles[identity]
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			require.NoError(t, so.Override(ibs, precompiles, rules))

			got, ok := precompiles[identity]
			require.Equal(t, tc.kept, ok)
			if tc.kept {
				require.Equal(t, want, got)
			}
			if !tc.movedTo.IsNil() {
				require.Equal(t, want, precompiles[tc.movedTo])
			}
			if so[identity].Balance != nil {
				balance, err := ibs.GetBalance(identity)
				require.NoError(t, err)
				require.Equal(t, uint64(1), balance.Uint64())
			}
		})
	}
}
