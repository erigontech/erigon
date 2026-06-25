package stagedsync

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// countPath counts writes in ws whose header has the given path.
func countPath(ws *state.WriteSet, path state.AccountPath) int {
	n := 0
	for h := range ws.AllHeaders() {
		if h.Path == path {
			n++
		}
	}
	return n
}

// writeSetLen is the total number of writes across all paths.
func writeSetLen(ws *state.WriteSet) int { return ws.Count() }

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

// testWriteValToInt collapses a versionMap-read value (typed per path) back to
// the int the parallel-exec harness wrote.
func testWriteValToInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case uint64:
		return int(x)
	case uint256.Int:
		return int(x.Uint64())
	case *uint256.Int:
		return int(x.Uint64())
	default:
		return 0
	}
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
