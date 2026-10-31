package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// selfDestructBurnWrites drives a real SELFDESTRUCT burn (preserveBalance=false)
// through IntraBlockState and returns the writeset it emits. The burn stamps the
// balance write ValueDeleted; tests use this instead of hand-crafting cells so
// the stamp — not the cell value — carries the preserve/burn decision.
func selfDestructBurnWrites(t *testing.T, addr accounts.Address, balance uint256.Int) *state.WriteSet {
	t.Helper()
	vm := state.NewVersionMap(nil)
	ibs := state.NewWithVersionMap(state.NewNoopReader(), vm)
	ibs.SetTxContext(100, 0)
	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.AddBalance(addr, balance, tracing.BalanceChangeTransfer))
	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	return finalizedWrites(t, ibs, &chain.Rules{})
}

// seededReader returns one pre-existing account; every other address is absent.
type seededReader struct {
	*state.NoopReader
	addr accounts.Address
	acc  *accounts.Account
}

func (r *seededReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	if address == r.addr {
		cp := *r.acc
		return &cp, nil
	}
	return nil, nil
}

// sdPreExistingWrites drives a real SELFDESTRUCT of a pre-existing contract
// (seeded via the base reader) holding `balance` at destruct time, and returns
// the writeset IBS emits. preserve selects the EIP-8246 balance-preserving
// destruct: with a non-zero balance it keeps the balance (ValueChanged → EOA);
// a burn, or a preserve with zero balance, stamps the balance ValueDeleted — the
// same preserveBalance-driven decision production makes.
func sdPreExistingWrites(t *testing.T, addr accounts.Address, balance uint256.Int, preserve bool) *state.WriteSet {
	t.Helper()
	original := sdEIP8246Original()
	original.Balance = balance
	reader := &seededReader{NoopReader: state.NewNoopReader(), addr: addr, acc: original}
	vm := state.NewVersionMap(nil)
	ibs := state.NewWithVersionMap(reader, vm)
	ibs.SetTxContext(1, 0)
	_, err := ibs.Selfdestruct(addr, preserve)
	require.NoError(t, err)
	return finalizedWrites(t, ibs, &chain.Rules{})
}

// countViewPath counts the writes a read-only WriteSetView yields for the given
// path. The view exposes no AddressPath/CodeSizePath accessor, so those paths
// are structurally absent (count 0) — the field-level exclusion the old
// Normalize did explicitly.
func countViewPath(v state.WriteSetView, path state.AccountPath) int {
	n := 0
	switch path {
	case state.BalancePath:
		for range v.Balances() {
			n++
		}
	case state.NoncePath:
		for range v.Nonces() {
			n++
		}
	case state.IncarnationPath:
		for range v.Incarnations() {
			n++
		}
	case state.CodeHashPath:
		for range v.CodeHashes() {
			n++
		}
	case state.CodePath:
		for range v.Codes() {
			n++
		}
	case state.SelfDestructPath:
		for range v.SelfDestructs() {
			n++
		}
	case state.CreateContractPath:
		for range v.CreateContracts() {
			n++
		}
	case state.StoragePath:
		for _, inner := range v.Storages() {
			n += len(inner)
		}
	}
	return n
}

func viewStorageVal(v state.WriteSetView, addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool) {
	for a, inner := range v.Storages() {
		if a != addr {
			continue
		}
		if w, ok := inner[key]; ok {
			return w.Val, true
		}
	}
	return uint256.Int{}, false
}

func viewBalanceVal(v state.WriteSetView, addr accounts.Address) (uint256.Int, bool) {
	for a, w := range v.Balances() {
		if a == addr {
			return w.Val, true
		}
	}
	return uint256.Int{}, false
}

func viewCodeVal(v state.WriteSetView, addr accounts.Address) (accounts.Code, bool) {
	for a, w := range v.Codes() {
		if a == addr {
			return w.Val, true
		}
	}
	return accounts.Code{}, false
}

func viewCodeHashVal(v state.WriteSetView, addr accounts.Address) (accounts.CodeHash, bool) {
	for a, w := range v.CodeHashes() {
		if a == addr {
			return w.Val, true
		}
	}
	return accounts.CodeHash{}, false
}

// wsb is a small fluent builder for assembling a typed *state.WriteSet in tests.
type wsb struct{ ws *state.WriteSet }

func newWS() *wsb { return &wsb{ws: &state.WriteSet{}} }

func (b *wsb) build() *state.WriteSet { return b.ws }

func (b *wsb) stor(addr accounts.Address, key accounts.StorageKey, ver state.Version, val uint256.Int) *wsb {
	b.ws.SetStorage(addr, key, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.StoragePath, Key: key, Version: ver}, Val: val})
	return b
}

func (b *wsb) bal(addr accounts.Address, ver state.Version, val uint256.Int) *wsb {
	b.ws.SetBalance(addr, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath, Version: ver}, Val: val})
	return b
}

func (b *wsb) nonce(addr accounts.Address, ver state.Version, val uint64) *wsb {
	b.ws.SetNonce(addr, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath, Version: ver}, Val: val})
	return b
}

func (b *wsb) inc(addr accounts.Address, ver state.Version, val uint64) *wsb {
	b.ws.SetIncarnation(addr, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr, Path: state.IncarnationPath, Version: ver}, Val: val})
	return b
}

func (b *wsb) codeHash(addr accounts.Address, ver state.Version, val accounts.CodeHash) *wsb {
	b.ws.SetCodeHash(addr, &state.VersionedWrite[accounts.CodeHash]{WriteHeader: state.WriteHeader{Address: addr, Path: state.CodeHashPath, Version: ver}, Val: val})
	return b
}

func (b *wsb) code(addr accounts.Address, ver state.Version, val accounts.Code) *wsb {
	b.ws.SetCode(addr, &state.VersionedWrite[accounts.Code]{WriteHeader: state.WriteHeader{Address: addr, Path: state.CodePath, Version: ver}, Val: val})
	return b
}

func (b *wsb) selfDestruct(addr accounts.Address, ver state.Version, val bool) *wsb {
	b.ws.SetSelfDestruct(addr, &state.VersionedWrite[bool]{WriteHeader: state.WriteHeader{Address: addr, Path: state.SelfDestructPath, Version: ver}, Val: val})
	return b
}

func (b *wsb) createContract(addr accounts.Address, ver state.Version, val bool) *wsb {
	b.ws.SetCreateContract(addr, &state.VersionedWrite[bool]{WriteHeader: state.WriteHeader{Address: addr, Path: state.CreateContractPath, Version: ver}, Val: val})
	return b
}

func (b *wsb) addr(addr accounts.Address, ver state.Version, val *accounts.Account) *wsb {
	b.ws.SetAddress(addr, &state.VersionedWrite[*accounts.Account]{WriteHeader: state.WriteHeader{Address: addr, Path: state.AddressPath, Version: ver}, Val: val})
	return b
}

// testWriteSetInt records an int-valued write into ws under the path-appropriate
// typed slot, used by the parallel-exec harness which models all values as ints.
func testWriteSetInt(ws *state.WriteSet, addr accounts.Address, path state.AccountPath, key accounts.StorageKey, version state.Version, val int) {
	h := state.WriteHeader{Address: addr, Path: path, Key: key, Version: version}
	switch path {
	case state.NoncePath:
		ws.SetNonce(addr, &state.VersionedWrite[uint64]{WriteHeader: h, Val: uint64(val)})
	case state.IncarnationPath:
		ws.SetIncarnation(addr, &state.VersionedWrite[uint64]{WriteHeader: h, Val: uint64(val)})
	case state.StoragePath:
		ws.SetStorage(addr, key, &state.VersionedWrite[uint256.Int]{WriteHeader: h, Val: *uint256.NewInt(uint64(val))})
	default:
		ws.SetBalance(addr, &state.VersionedWrite[uint256.Int]{WriteHeader: h, Val: *uint256.NewInt(uint64(val))})
	}
}

func finalizedWrites(t testing.TB, ibs *state.IntraBlockState, rules *chain.Rules) *state.WriteSet {
	t.Helper()
	writes, err := ibs.FinalizedWrites(rules)
	require.NoError(t, err)
	return writes
}
