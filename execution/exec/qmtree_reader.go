package exec

import (
	"bytes"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state"
	ethtypes "github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// stateOp records a single state read or write.
// Sorting follows Block Access List (EIP-7928) ordering rules:
// primarily by domain (accounts < storage < code), then by key bytes
// (address strictly increasing; for storage, address then slot strictly increasing).
type stateOp struct {
	domain kv.Domain
	key    []byte
	value  []byte
}

// hashingReader wraps a StateReader and records all state reads for
// preStateHash computation.
type hashingReader struct {
	inner   state.StateReader
	reads   []stateOp
	enabled bool
}

func newHashingReader() *hashingReader {
	return &hashingReader{
		reads: make([]stateOp, 0, 16),
	}
}

// Reset clears accumulated reads and enables recording.
func (r *hashingReader) Reset() {
	r.reads = r.reads[:0]
	r.enabled = true
}

// Finalize returns the DeriveSha root over the collected reads.
func (r *hashingReader) Finalize() common.Hash {
	r.enabled = false
	return stateOpsRoot(r.reads)
}

func (r *hashingReader) addRead(domain kv.Domain, key, value []byte) {
	if !r.enabled {
		return
	}
	r.reads = append(r.reads, stateOp{
		domain: domain,
		key:    common.Copy(key),
		value:  common.Copy(value),
	})
}

// --- StateReader interface ---

func (r *hashingReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	acc, err := r.inner.ReadAccountData(address)
	if err == nil {
		addr := address.Value()
		var val []byte
		if acc != nil {
			val = accounts.SerialiseV3(acc)
		}
		r.addRead(kv.AccountsDomain, addr[:], val)
	}
	return acc, err
}

func (r *hashingReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.inner.ReadAccountDataForDebug(address)
}

func (r *hashingReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	val, ok, err := r.inner.ReadAccountStorage(address, key)
	if err == nil {
		addr := address.Value()
		k := key.Value()
		composite := make([]byte, 52) // 20 addr + 32 key
		copy(composite[:20], addr[:])
		copy(composite[20:], k[:])
		r.addRead(kv.StorageDomain, composite, val.Bytes())
	}
	return val, ok, err
}

func (r *hashingReader) HasStorage(address accounts.Address) (bool, error) {
	return r.inner.HasStorage(address)
}

func (r *hashingReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	code, err := r.inner.ReadAccountCode(address)
	if err == nil {
		addr := address.Value()
		r.addRead(kv.CodeDomain, addr[:], code)
	}
	return code, err
}

func (r *hashingReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	return r.inner.ReadAccountCodeSize(address)
}

func (r *hashingReader) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return r.inner.ReadAccountIncarnation(address)
}

func (r *hashingReader) SetTrace(trace bool, tracePrefix string) {
	r.inner.SetTrace(trace, tracePrefix)
}

func (r *hashingReader) Trace() bool         { return r.inner.Trace() }
func (r *hashingReader) TracePrefix() string { return r.inner.TracePrefix() }

// --- shared Merkle root logic ---

// stateOpsRoot returns a DeriveSha root over a set of state operations,
// using the same Merkle Patricia Trie format as Ethereum's transaction and
// receipt roots.
//
// Ordering follows Block Access List (EIP-7928) rules:
//   - Sorted first by domain (AccountsDomain < StorageDomain < CodeDomain)
//   - Within domain, sorted by key bytes ascending
//     (20-byte address for accounts/code; 52-byte addr+slot for storage)
//   - This gives addresses strictly increasing and, within each address,
//     slots strictly increasing — matching the BAL canonical ordering
//
// Each item is RLP-encoded as a 3-element list: [domain_uint, key_bytes, value_bytes].
// The DeriveSha trie keys are the sequential integer indices 0, 1, 2 …
// (same convention as transactions and receipts).
//
// Empty set returns trie.EmptyRoot (not the zero hash) — same as DeriveSha
// on an empty list.
func stateOpsRoot(ops []stateOp) common.Hash {
	sort.Slice(ops, func(i, j int) bool {
		if ops[i].domain != ops[j].domain {
			return ops[i].domain < ops[j].domain
		}
		return bytes.Compare(ops[i].key, ops[j].key) < 0
	})
	return ethtypes.DeriveSha(sortedStateOps(ops))
}

// sortedStateOps implements types.DerivableList over a pre-sorted []stateOp.
type sortedStateOps []stateOp

func (s sortedStateOps) Len() int { return len(s) }

// EncodeIndex writes the RLP encoding of ops[i] as [domain, key, value].
func (s sortedStateOps) EncodeIndex(i int, w *bytes.Buffer) {
	op := s[i]
	if err := rlp.Encode(w, stateOpRLP{Domain: uint(op.domain), Key: op.key, Value: op.value}); err != nil {
		panic(err) // only fails on OOM
	}
}

// stateOpRLP is the canonical wire format for a single state op leaf.
// RLP-encoded as a 3-element list: [domain_uint, key_bytes, value_bytes].
type stateOpRLP struct {
	Domain uint
	Key    []byte
	Value  []byte
}
