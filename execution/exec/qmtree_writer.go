package exec

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// hashingWriter wraps a StateWriter and records all state changes for
// stateChangeHash computation. Changes are sorted by (domain, key) before
// hashing to ensure determinism regardless of write order.
type hashingWriter struct {
	inner   state.StateWriter
	writes  []stateOp
	enabled bool
}

func newHashingWriter() *hashingWriter {
	return &hashingWriter{
		writes: make([]stateOp, 0, 16),
	}
}

// Reset clears accumulated writes and enables recording.
func (w *hashingWriter) Reset() {
	w.writes = w.writes[:0]
	w.enabled = true
}

// Finalize returns the DeriveSha root over the collected writes.
func (w *hashingWriter) Finalize() common.Hash {
	w.enabled = false
	return stateOpsRoot(w.writes)
}

func (w *hashingWriter) addWrite(domain kv.Domain, key, value []byte) {
	if !w.enabled {
		return
	}
	w.writes = append(w.writes, stateOp{
		domain: domain,
		key:    common.Copy(key),
		value:  common.Copy(value),
	})
}

// --- StateWriter interface ---

func (w *hashingWriter) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	err := w.inner.UpdateAccountData(address, original, account)
	if err == nil {
		addr := address.Value()
		val := accounts.SerialiseV3(account)
		w.addWrite(kv.AccountsDomain, addr[:], val)
	}
	return err
}

func (w *hashingWriter) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	err := w.inner.UpdateAccountCode(address, incarnation, codeHash, code)
	if err == nil {
		addr := address.Value()
		w.addWrite(kv.CodeDomain, addr[:], code)
	}
	return err
}

func (w *hashingWriter) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	err := w.inner.DeleteAccount(address, original)
	if err == nil {
		addr := address.Value()
		w.addWrite(kv.AccountsDomain, addr[:], nil) // nil value = deletion
	}
	return err
}

func (w *hashingWriter) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	err := w.inner.WriteAccountStorage(address, incarnation, key, original, value)
	if err == nil && original != value {
		addr := address.Value()
		k := key.Value()
		composite := make([]byte, 52) // 20 addr + 32 key
		copy(composite[:20], addr[:])
		copy(composite[20:], k[:])
		w.addWrite(kv.StorageDomain, composite, value.Bytes())
	}
	return err
}

func (w *hashingWriter) CreateContract(address accounts.Address) error {
	return w.inner.CreateContract(address)
}
