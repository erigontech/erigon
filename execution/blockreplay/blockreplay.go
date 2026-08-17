// Package blockreplay provides a lightweight, exec-only block-replay harness.
//
// A Fixture is the minimal "exec witness" for one block: the pre-state VALUES
// the block's execution reads (accounts, storage, code), plus the block payload
// RLP and the ancestor data execution needs (parent header, BLOCKHASH hashes).
// It carries no merkle proofs and no commitment state — replay runs
// ExecuteBlockEphemerally against an in-memory reader and never computes a trie.
//
// Capture: wrap a real StateReader in a recordingReader and run the block once;
// every first read records the pre-block value. Replay: serve those values from
// an inMemReader. Same block + same reads => deterministic, portable, tiny.
package blockreplay

import (
	"bytes"
	"encoding/gob"
	"os"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Save/Load serialize a Fixture (gob) so a captured block becomes a drop-in
// testdata file.
func (fx *Fixture) Save(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return gob.NewEncoder(f).Encode(fx)
}

func Load(path string) (*Fixture, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fx := newFixture()
	if err := gob.NewDecoder(f).Decode(fx); err != nil {
		return nil, err
	}
	return fx, nil
}

// acctData is a captured pre-state account. Present=false means the account did
// not exist at capture time (ReadAccountData returned nil).
type acctData struct {
	Present     bool
	Nonce       uint64
	Balance     [32]byte // uint256 big-endian
	CodeHash    [32]byte
	Incarnation uint64
}

// Fixture is the serializable exec-witness for a single block: the pre-state
// the block reads (inputs) plus the post-state it writes (Outputs), so a replay
// can be checked against known-correct data — not just profiled.
type Fixture struct {
	BlockRLP        []byte
	ParentHeaderRLP []byte
	Senders         [][20]byte          // recovered tx senders; RLP does not preserve them
	Ancestors       map[uint64][32]byte // block number -> hash, for BLOCKHASH
	Accounts        map[[20]byte]acctData
	Storage         map[[20]byte]map[[32]byte][32]byte
	Code            map[[20]byte][]byte
	// HasStorageResult is the per-address HasStorage answer captured at query time.
	// It cannot be derived from Storage (which holds only slots that were read):
	// storage may exist only in unread slots, and reading an absent/zero slot does
	// not imply storage exists. The EVM uses HasStorage for CREATE collision, so
	// replay must reproduce the captured answer, not infer it.
	HasStorageResult map[[20]byte]bool
	Outputs          *Outputs // post-state the block wrote; nil until captured/backfilled
	BALBytes         []byte   // encoded block access list sidecar; empty pre-Amsterdam
}

// BAL decodes the fixture's block access list sidecar, or nil when the block
// carried none (pre-Amsterdam).
func (f *Fixture) BAL() (types.BlockAccessList, error) {
	if len(f.BALBytes) == 0 {
		return nil, nil
	}
	return types.DecodeBlockAccessListBytes(f.BALBytes)
}

// Outputs is the post-state a block's execution produced: the accounts it
// updated (with final values), deleted, and the storage/code it wrote. It is
// the reference the replay's post-state is checked against (data, not root).
type Outputs struct {
	Accounts map[[20]byte]acctData
	Deleted  map[[20]byte]bool
	Storage  map[[20]byte]map[[32]byte][32]byte
	Code     map[[20]byte][]byte
}

func newOutputs() *Outputs {
	return &Outputs{
		Accounts: map[[20]byte]acctData{},
		Deleted:  map[[20]byte]bool{},
		Storage:  map[[20]byte]map[[32]byte][32]byte{},
		Code:     map[[20]byte][]byte{},
	}
}

func newFixture() *Fixture {
	return &Fixture{
		Ancestors:        map[uint64][32]byte{},
		Accounts:         map[[20]byte]acctData{},
		Storage:          map[[20]byte]map[[32]byte][32]byte{},
		Code:             map[[20]byte][]byte{},
		HasStorageResult: map[[20]byte]bool{},
	}
}

func toAcctData(a *accounts.Account) acctData {
	if a == nil {
		return acctData{Present: false}
	}
	return acctData{
		Present:     true,
		Nonce:       a.Nonce,
		Balance:     a.Balance.Bytes32(),
		CodeHash:    a.CodeHash.Value(),
		Incarnation: a.Incarnation,
	}
}

func (d acctData) toAccount() *accounts.Account {
	if !d.Present {
		return nil
	}
	a := accounts.NewAccount()
	a.Nonce = d.Nonce
	a.Balance.SetBytes(d.Balance[:])
	a.CodeHash = accounts.InternCodeHash(common.BytesToHash(d.CodeHash[:]))
	a.Incarnation = d.Incarnation
	return &a
}

// recordingReader delegates to inner and records the first (pre-block) value of
// every read into fx.
type recordingReader struct {
	inner       state.StateReader
	fx          *Fixture
	trace       bool
	tracePrefix string
}

// NewRecordingReader wraps inner; capture the accumulated Fixture with Fixture().
func NewRecordingReader(inner state.StateReader) *recordingReader {
	return &recordingReader{inner: inner, fx: newFixture()}
}

func (r *recordingReader) Fixture() *Fixture { return r.fx }

func (r *recordingReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	a, err := r.inner.ReadAccountData(address)
	if err != nil {
		return a, err
	}
	k := address.Value()
	if _, ok := r.fx.Accounts[k]; !ok {
		r.fx.Accounts[k] = toAcctData(a)
	}
	return a, nil
}

func (r *recordingReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(address)
}

func (r *recordingReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	v, ok, err := r.inner.ReadAccountStorage(address, key)
	if err != nil {
		return v, ok, err
	}
	a := address.Value()
	m := r.fx.Storage[a]
	if m == nil {
		m = map[[32]byte][32]byte{}
		r.fx.Storage[a] = m
	}
	if _, seen := m[key.Value()]; !seen {
		m[key.Value()] = v.Bytes32()
	}
	return v, ok, nil
}

func (r *recordingReader) HasStorage(address accounts.Address) (bool, error) {
	has, err := r.inner.HasStorage(address)
	if err != nil {
		return has, err
	}
	if _, seen := r.fx.HasStorageResult[address.Value()]; !seen {
		r.fx.HasStorageResult[address.Value()] = has
	}
	return has, nil
}

func (r *recordingReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	c, err := r.inner.ReadAccountCode(address)
	if err != nil {
		return c, err
	}
	k := address.Value()
	if _, ok := r.fx.Code[k]; !ok {
		r.fx.Code[k] = bytes.Clone(c)
	}
	return c, nil
}

func (r *recordingReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	c, err := r.ReadAccountCode(address)
	return len(c), err
}

func (r *recordingReader) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return r.inner.ReadAccountIncarnation(address)
}

func (r *recordingReader) SetTrace(t bool, p string) { r.trace, r.tracePrefix = t, p }
func (r *recordingReader) Trace() bool               { return r.trace }
func (r *recordingReader) TracePrefix() string       { return r.tracePrefix }

// inMemReader serves reads from a Fixture with no DB behind it. Reads are
// ~free, so it models the COMPUTE path, not read cost — set StorageReadNanos to
// busy-spin a modelled per-storage-read latency (measured cold-file cost) when
// reproducing read-bound blocks matters.
type inMemReader struct {
	fx               *Fixture
	trace            bool
	tracePrefix      string
	StorageReadNanos int64 // >0: model per-storage-read latency
}

// NewInMemReader serves StateReader reads from fx.
func NewInMemReader(fx *Fixture) *inMemReader { return &inMemReader{fx: fx} }

func spin(ns int64) {
	if ns <= 0 {
		return
	}
	deadline := time.Now().Add(time.Duration(ns))
	for time.Now().Before(deadline) {
	}
}

func (r *inMemReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	return r.fx.Accounts[address.Value()].toAccount(), nil
}

func (r *inMemReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(address)
}

func (r *inMemReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	spin(r.StorageReadNanos)
	m, ok := r.fx.Storage[address.Value()]
	if !ok {
		return uint256.Int{}, false, nil
	}
	b, ok := m[key.Value()]
	if !ok {
		return uint256.Int{}, false, nil
	}
	var v uint256.Int
	v.SetBytes(b[:])
	return v, !v.IsZero(), nil
}

func (r *inMemReader) HasStorage(address accounts.Address) (bool, error) {
	// Use the captured answer; do NOT infer from read slots (see HasStorageResult).
	if has, ok := r.fx.HasStorageResult[address.Value()]; ok {
		return has, nil
	}
	return len(r.fx.Storage[address.Value()]) > 0, nil
}

func (r *inMemReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	return r.fx.Code[address.Value()], nil
}

func (r *inMemReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	return len(r.fx.Code[address.Value()]), nil
}

func (r *inMemReader) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return r.fx.Accounts[address.Value()].Incarnation, nil
}

func (r *inMemReader) SetTrace(t bool, p string) { r.trace, r.tracePrefix = t, p }
func (r *inMemReader) Trace() bool               { return r.trace }
func (r *inMemReader) TracePrefix() string       { return r.tracePrefix }

var (
	_ state.StateReader = (*recordingReader)(nil)
	_ state.StateReader = (*inMemReader)(nil)
)
