package exec

import (
	"encoding/binary"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// stateOp records a single state read or write for hashing.
// Encoding per entry: [1B domain][2B key_len BE][key][4B val_len BE][value]
type stateOp struct {
	domain kv.Domain
	key    []byte
	value  []byte
}

// hashingReader wraps a StateReader and records all state reads for
// preStateHash computation. Reads are sorted by (domain, key) before
// hashing to ensure determinism regardless of read order.
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

// Finalize sorts the collected reads and returns their keccak256 hash.
func (r *hashingReader) Finalize() common.Hash {
	r.enabled = false
	return hashStateOps(r.reads)
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

// --- shared hashing logic ---

// hashStateOps sorts ops by (domain, key) and returns their keccak256 hash.
// Encoding per op: [1B domain_id][2B key_len BE][key][4B val_len BE][value]
func hashStateOps(ops []stateOp) common.Hash {
	sort.Slice(ops, func(i, j int) bool {
		if ops[i].domain != ops[j].domain {
			return ops[i].domain < ops[j].domain
		}
		return string(ops[i].key) < string(ops[j].key)
	})

	h := crypto.NewKeccakState()
	defer crypto.ReturnToPool(h)

	var buf [7]byte
	for _, op := range ops {
		buf[0] = byte(op.domain)
		binary.BigEndian.PutUint16(buf[1:3], uint16(len(op.key)))
		h.Write(buf[:3])
		h.Write(op.key)
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(op.value)))
		h.Write(buf[:4])
		h.Write(op.value)
	}

	var out common.Hash
	h.Read(out[:])
	return out
}
