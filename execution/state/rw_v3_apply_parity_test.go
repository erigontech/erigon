package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// applyToFreshDomains creates a fresh SharedDomains, applies an optional
// pre-block setup writeset (committed as prior state), then applies view at
// txIdx, and reads back the resulting account/code/storage state for every
// address (and its storage keys) touched. It returns a comparable snapshot so
// the raw-view and Normalize-output runs can be diffed field-for-field.
func applyToFreshDomains(
	t *testing.T,
	setup *WriteSet,
	makeView func(reader StateReader) WriteSetView,
	addrs []accounts.Address,
	storageKeys map[accounts.Address][]accounts.StorageKey,
	txIdx uint64,
	rules *chain.Rules,
) map[string][]byte {
	t.Helper()
	_, tx, domains := NewTestRwTx(t)

	if setup != nil {
		domains.SetTxNum(txIdx - 1)
		require.NoError(t, ApplyWrites(setup, domains, tx, 1, txIdx-1, nil, rules, nil, false))
	}

	reader := NewReaderV3(domains.AsGetter(tx))
	view := makeView(reader)

	domains.SetTxNum(txIdx)
	require.NoError(t, ApplyWrites(view, domains, tx, 1, txIdx, nil, rules, nil, false))

	snap := make(map[string][]byte)
	for _, addr := range addrs {
		av := addr.Value()
		accEnc, _, err := domains.GetLatest(kv.AccountsDomain, tx, av[:])
		require.NoError(t, err)
		snap["acc:"+string(av[:])] = accEnc
		codeEnc, _, err := domains.GetLatest(kv.CodeDomain, tx, av[:])
		require.NoError(t, err)
		snap["code:"+string(av[:])] = codeEnc
		for _, k := range storageKeys[addr] {
			kv0 := k.Value()
			composite := append(append([]byte{}, av[:]...), kv0[:]...)
			stEnc, _, err := domains.GetLatest(kv.StorageDomain, tx, composite)
			require.NoError(t, err)
			snap["stor:"+string(composite)] = stEnc
		}
	}
	return snap
}

// TestRawViewVsNormalize_ApplyParity is the apply-side dual of
// TestRawViewVsNormalize_CalcParity: it applies the RAW versionMap view and,
// separately, the Normalize output into two fresh SharedDomains and asserts the
// resulting domain state is identical. Divergences map the Normalize semantics
// that ApplyWrites must own once Normalize is removed:
//   - R2 SD account-field drop (so apply reaches the pure-delete branch),
//   - R9 EIP-161 empty-account removal,
//   - R6 SD storage cascade (apply wipes via DomainDelPrefix, natively),
//   - R3 storage no-op filter (value-neutral for the domain).
func TestRawViewVsNormalize_ApplyParity(t *testing.T) {
	const txIdx, inc = 5, 0
	ver := Version{TxIndex: txIdx, Incarnation: inc}
	A := accounts.InternAddress([20]byte{0xaa})
	k := accounts.InternKey(common.Hash{0x01})
	rules := &chain.Rules{IsSpuriousDragon: true}

	newWS := func(build func(ws *WriteSet)) *WriteSet {
		ws := &WriteSet{}
		build(ws)
		return ws
	}
	balW := func(v uint256.Int) *VersionedWrite[uint256.Int] {
		return &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: A, Path: BalancePath, Version: ver}, Val: v}
	}
	nonceW := func(v uint64) *VersionedWrite[uint64] {
		return &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: A, Path: NoncePath, Version: ver}, Val: v}
	}
	incW := func(v uint64) *VersionedWrite[uint64] {
		return &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: A, Path: IncarnationPath, Version: ver}, Val: v}
	}
	storW := func(key accounts.StorageKey, v uint256.Int) *VersionedWrite[uint256.Int] {
		return &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: A, Path: StoragePath, Key: key, Version: ver}, Val: v}
	}
	sdW := func(v bool) *VersionedWrite[bool] {
		return &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: A, Path: SelfDestructPath, Version: ver}, Val: v}
	}
	codeHashW := func(v accounts.CodeHash) *VersionedWrite[accounts.CodeHash] {
		return &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: A, Path: CodeHashPath, Version: ver}, Val: v}
	}

	// A pre-block account with one storage slot, for the cascade / no-op cases.
	setupAcct := newWS(func(ws *WriteSet) {
		ws.SetBalance(A, balW(*uint256.NewInt(500)))
		ws.SetNonce(A, nonceW(1))
		ws.SetStorage(A, k, storW(k, *uint256.NewInt(7)))
	})

	cases := []struct {
		name        string
		setup       *WriteSet
		ws          *WriteSet
		storageKeys []accounts.StorageKey
	}{
		{
			name: "simple account update",
			ws:   newWS(func(ws *WriteSet) { ws.SetBalance(A, balW(*uint256.NewInt(100))); ws.SetNonce(A, nonceW(3)) }),
		},
		{
			name: "account + storage",
			ws: newWS(func(ws *WriteSet) {
				ws.SetBalance(A, balW(*uint256.NewInt(100)))
				ws.SetStorage(A, k, storW(k, *uint256.NewInt(7)))
			}),
			storageKeys: []accounts.StorageKey{k},
		},
		{
			// A contract that SSTOREs but whose account fields are unchanged from
			// block start — the "storage-only dirty" case Normalize's R7 fill
			// targets. The account must be non-empty (real EVM: SSTORE implies
			// code), else R7's synthesized zero fields would trip R9 into a delete.
			name:        "storage-only dirty (R7 fill)",
			setup:       setupAcct,
			ws:          newWS(func(ws *WriteSet) { ws.SetStorage(A, k, storW(k, *uint256.NewInt(9))) }),
			storageKeys: []accounts.StorageKey{k},
		},
		{
			name: "self-destruct pre-existing (R2 drop)",
			ws: newWS(func(ws *WriteSet) {
				ws.SetSelfDestruct(A, sdW(true))
				ws.SetBalance(A, balW(*uint256.NewInt(0)))
				ws.SetIncarnation(A, incW(1))
			}),
		},
		{
			name: "touched-empty account (R9 EIP-161 removal)",
			ws: newWS(func(ws *WriteSet) {
				ws.SetBalance(A, balW(uint256.Int{}))
				ws.SetNonce(A, nonceW(0))
				ws.SetCodeHash(A, codeHashW(accounts.EmptyCodeHash))
			}),
		},
		{
			name:        "storage no-op write-back (R3)",
			setup:       setupAcct,
			ws:          newWS(func(ws *WriteSet) { ws.SetStorage(A, k, storW(k, *uint256.NewInt(7))) }),
			storageKeys: []accounts.StorageKey{k},
		},
		{
			name: "create-then-self-destruct same tx (DeployAndDestruct)",
			ws: newWS(func(ws *WriteSet) {
				ws.SetCreateContract(A, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: A, Path: CreateContractPath, Version: ver}, Val: true})
				ws.SetIncarnation(A, incW(1))
				ws.SetNonce(A, nonceW(1))
				code := accounts.NewCode([]byte{0x60, 0x0a, 0xff})
				ws.SetCodeHash(A, codeHashW(code.Hash))
				ws.SetCode(A, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: A, Path: CodePath, Version: ver}, Val: code})
				ws.SetBalance(A, balW(uint256.Int{}))
				ws.SetSelfDestruct(A, sdW(true))
			}),
		},
		{
			name:  "self-destruct with pre-block storage (R6 cascade)",
			setup: setupAcct,
			ws: newWS(func(ws *WriteSet) {
				ws.SetSelfDestruct(A, sdW(true))
				ws.SetBalance(A, balW(*uint256.NewInt(0)))
				ws.SetIncarnation(A, incW(1))
			}),
			storageKeys: []accounts.StorageKey{k},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			vm := NewVersionMap(nil)
			vm.FlushVersionedWrites(tc.ws, true, "")

			storageKeys := map[accounts.Address][]accounts.StorageKey{A: tc.storageKeys}

			snapN := applyToFreshDomains(t, tc.setup, func(reader StateReader) WriteSetView {
				normalized, err := tc.ws.Normalize(vm, txIdx, inc, reader, nil, true /*emptyRemoval*/, false, false)
				require.NoError(t, err)
				return normalized
			}, []accounts.Address{A}, storageKeys, txIdx, rules)

			snapR := applyToFreshDomains(t, tc.setup, func(reader StateReader) WriteSetView {
				return NewVersionMapWriteView(tc.ws, vm, txIdx)
			}, []accounts.Address{A}, storageKeys, txIdx, rules)

			require.Equal(t, snapN, snapR, "apply domain state: raw-view vs normalize")
		})
	}
}
