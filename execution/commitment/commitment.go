// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commitment

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"unsafe"

	"github.com/google/btree"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/v2/branch"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type Trie interface {
	RootHash() (hash []byte, err error)

	SetTraceWriter(io.Writer)

	Variant() TrieVariant

	Reset()

	ResetContext(ctx PatriciaContext)

	Process(ctx context.Context, updates *Updates, logPrefix string, onProgress func(*CommitProgress), warmup WarmupConfig) (rootHash []byte, err error)

	Release()
}

type TrieStateCodec interface {
	EncodeState(blockNum, txNum uint64) ([]byte, error)
	RestoreState(value []byte) (blockNum, txNum uint64, err error)
}

type CommitProgress struct {
	KeyIndex    uint64
	UpdateCount uint64
	Metrics     MetricValues
}

type PatriciaContext interface {
	Branch(prefix []byte) ([]byte, kv.Step, error)
	// Implementations must copy prefix and data rather than retain them: callers may pass
	// pooled buffers that are recycled for a later, unrelated update.
	// A nil prevData asks the writer to read the previous value.
	// A non-nil empty slice means the branch was absent from the trie,
	// even if an unreachable record remains in storage.
	PutBranch(prefix []byte, data []byte, prevData []byte) error
	Account(plainKey []byte) (*Update, error)
	Storage(plainKey []byte) (*Update, error)
}

type TrieVariant string

const (
	VariantHexPatriciaTrie     TrieVariant = "hex-patricia-hashed"
	VariantParallelHexPatricia TrieVariant = "hex-parallel-patricia-hashed"
	VariantCommitmentV3        TrieVariant = "commitment-v3"
	CommitmentV3StateMarker    byte        = 0x04
)

var (
	KeyCommitmentState   = []byte("state")
	KeyCommitmentV3State = []byte{0x42}
	CommitmentStateKeys  = [][]byte{KeyCommitmentV3State, KeyCommitmentState}
)

const CommitmentV3StateSize = 1 + 8 + 8 + 32

var (
	ErrCommitmentV3StateMarker = errors.New("commitment v3: invalid state variant marker")
	ErrCommitmentV3StateSize   = errors.New("commitment v3: invalid state size")
)

func EncodeCommitmentV3State(root []byte, blockNum, txNum uint64) ([]byte, error) {
	if len(root) != length.Hash {
		return nil, ErrCommitmentV3StateSize
	}
	dst := make([]byte, 1, CommitmentV3StateSize)
	dst[0] = CommitmentV3StateMarker
	dst = binary.BigEndian.AppendUint64(dst, txNum)
	dst = binary.BigEndian.AppendUint64(dst, blockNum)
	return append(dst, root...), nil
}

func DecodeCommitmentV3State(value []byte) (blockNum, txNum uint64, root []byte, err error) {
	if len(value) != CommitmentV3StateSize {
		return 0, 0, nil, ErrCommitmentV3StateSize
	}
	if value[0] != CommitmentV3StateMarker {
		return 0, 0, nil, ErrCommitmentV3StateMarker
	}
	return binary.BigEndian.Uint64(value[9:17]), binary.BigEndian.Uint64(value[1:9]), bytes.Clone(value[17:]), nil
}

func IsCommitmentStateKey(key []byte) bool {
	return slices.ContainsFunc(CommitmentStateKeys, func(k []byte) bool { return bytes.Equal(key, k) })
}

type DeferredBranchUpdate struct {
	Prefix  []byte
	Raw     branch.BranchData
	Prev    []byte
	Encoded branch.BranchData
	// Backing store for a merged encoded value. Encoded either aliases Raw or points
	// here, so it is never itself reused — this is the buffer that survives pooling.
	encodedBuf []byte
	// RawWrite writes Raw as-is without merging it against Prev, for a tombstone whose
	// Prev is carried only for the domain diff (see v2 BranchEncoder.CollectTombstone).
	RawWrite bool
}

var DeferredUpdatePool = &sync.Pool{
	New: func() any {
		return &DeferredBranchUpdate{}
	},
}

func GetDeferredUpdate(prefix []byte, raw, prev []byte) *DeferredBranchUpdate {
	upd := DeferredUpdatePool.Get().(*DeferredBranchUpdate)

	upd.Prefix = reuseBytes(upd.Prefix, prefix)
	upd.Raw = reuseBytes(upd.Raw, raw)
	upd.Prev = reuseBytes(upd.Prev, prev)
	upd.Encoded = nil
	upd.RawWrite = false

	return upd
}

// reuseBytes copies src into dst's backing array, matching bytes.Clone's nil handling:
// a nil src yields nil, so pool history cannot change the result's nil-ness. Callers
// distinguish a nil prev ("look it up") from an empty one ("absent from the trie").
func reuseBytes(dst, src []byte) []byte {
	if src == nil {
		return nil
	}
	if cap(dst) == 0 {
		dst = make([]byte, 0, len(src)) // non-nil even for an empty src, as bytes.Clone is
	}
	return append(dst[:0], src...)
}

// CapLen hides a recycled buffer's leftover capacity, so what a callback sees is derived
// from the input rather than from whichever update last used the pooled object.
func CapLen(b []byte) []byte {
	if b == nil {
		return nil
	}
	return slices.Clip(b)
}

// PutDeferredUpdate returns a DeferredBranchUpdate to the global pool.
func PutDeferredUpdate(upd *DeferredBranchUpdate) {
	if upd != nil {
		// encoded can alias raw, so it is dropped rather than recycled; prefix, raw,
		// prev and encodedBuf keep their backing arrays for the next checkout.
		upd.Encoded = nil
		DeferredUpdatePool.Put(upd)
	}
}

type BranchDelta struct {
	Key  []byte
	Data []byte
	Prev []byte
}

type FeedSlot struct {
	Hash  [32]byte
	Value []byte
}

type FeedAccount struct {
	Hash   [32]byte
	Update *Update
	Slots  []FeedSlot
}

type Feed struct {
	Accounts []FeedAccount
	Keys     int
}

type LeafRefs struct {
	Mask uint16
	Refs [][32]byte
}

type PendingCommitmentUpdate struct {
	BlockNum uint64
	// BlockHash disambiguates changeset lookups sharing a block number.
	BlockHash common.Hash
	TxNum     uint64
	Deferred  []*DeferredBranchUpdate
	Deltas    [][]BranchDelta
	// Metrics is the producing trie's, carried so the later apply still reaches
	// that trie's log counters. The Prometheus counters do not depend on it —
	// publishBranchWrites bills those where the write lands.
	Metrics *Metrics
}

func (p *PendingCommitmentUpdate) Clear() {
	p.Deltas = nil
	for _, upd := range p.Deferred {
		PutDeferredUpdate(upd)
	}
	p.Deferred = nil
}

func MergeDeferredUpdate(upd *DeferredBranchUpdate, merger *branch.BranchMerger) error {
	if upd.RawWrite {
		upd.Encoded = upd.Raw
		return nil
	}
	if len(upd.Prev) > 0 {
		if bytes.Equal(upd.Prev, upd.Raw) {
			upd.Encoded = nil
			return nil
		}
		merged, err := merger.Merge(upd.Prev, upd.Raw)
		if err != nil {
			return err
		}
		upd.encodedBuf = reuseBytes(upd.encodedBuf, merged)
		upd.Encoded = upd.encodedBuf
		return nil
	}
	upd.Encoded = upd.Raw
	return nil
}

var WorkerMergerPool = sync.Pool{New: func() any { return branch.NewHexBranchMerger(512) }}

// ApplyDeferredBranchUpdates applies the queued branch writes and returns how many
// were written. Writes are published to the branch-write counters as they land,
// not against a round: the caller-owned path applies from SharedDomains after the
// producing round has already closed, so there is no round left to bill. m, when
// non-nil, additionally carries them into that trie's log counters.
//
// putBranch must copy prefix and data rather than retain them: they are pooled and
// reused for a later, unrelated update. prevData is cloned per update and carries
// no such constraint.
func ApplyDeferredBranchUpdates(
	deferred []*DeferredBranchUpdate,
	numWorkers int,
	putBranch func(prefix []byte, data []byte, prevData []byte) error,
	m *Metrics,
) (n int, err error) {
	if len(deferred) == 0 {
		return 0, nil
	}
	if numWorkers <= 1 {
		numWorkers = 1
	}

	if numWorkers == 1 || len(deferred) <= numWorkers {
		merger := WorkerMergerPool.Get().(*branch.BranchMerger)
		defer WorkerMergerPool.Put(merger)

		var written, bytesOut int
		for _, upd := range deferred {
			if err := MergeDeferredUpdate(upd, merger); err != nil {
				PublishBranchWrites(written, bytesOut, m)
				return written, err
			}
			if upd.Encoded == nil {
				continue
			}
			if err := putBranch(CapLen(upd.Prefix), CapLen(upd.Encoded), CapLen(upd.Prev)); err != nil {
				PublishBranchWrites(written, bytesOut, m)
				return written, err
			}
			written++
			bytesOut += len(upd.Encoded)
		}
		PublishBranchWrites(written, bytesOut, m)
		return written, nil
	}

	chunk := (len(deferred) + numWorkers - 1) / numWorkers
	errs := make([]error, numWorkers)
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		w := w
		lo := w * chunk
		hi := min(lo+chunk, len(deferred))
		if lo >= hi {
			break
		}
		wg.Go(func() {
			merger := WorkerMergerPool.Get().(*branch.BranchMerger)
			defer WorkerMergerPool.Put(merger)
			for i := lo; i < hi; i++ {
				if err := MergeDeferredUpdate(deferred[i], merger); err != nil {
					errs[w] = err
					return
				}
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return 0, err
		}
	}

	var written, bytesOut int
	for _, upd := range deferred {
		if upd.Encoded == nil {
			continue
		}
		if err := putBranch(CapLen(upd.Prefix), CapLen(upd.Encoded), CapLen(upd.Prev)); err != nil {
			PublishBranchWrites(written, bytesOut, m)
			return written, err
		}
		written++
		bytesOut += len(upd.Encoded)
	}
	PublishBranchWrites(written, bytesOut, m)
	return written, nil
}

func ParseTrieVariant(s string) TrieVariant {
	var trieVariant TrieVariant
	switch s {
	case "v3":
		trieVariant = VariantCommitmentV3
	case "parallel":
		trieVariant = VariantParallelHexPatricia
	case "hex":
		fallthrough
	default:
		trieVariant = VariantHexPatriciaTrie
	}
	return trieVariant
}

type Mode uint

const (
	ModeDisabled Mode = 0
	ModeDirect   Mode = 1
	ModeUpdate   Mode = 2
	ModeParallel Mode = 3
	ModeCollect  Mode = 4
)

func (m Mode) String() string {
	switch m {
	case ModeDisabled:
		return "disabled"
	case ModeDirect:
		return "direct"
	case ModeUpdate:
		return "update"
	case ModeParallel:
		return "parallel"
	case ModeCollect:
		return "collect"
	default:
		return "unknown"
	}
}

type Updates struct {
	hasher   keyHasher
	keys     map[string]struct{}
	etl      *etl.Collector
	tree     *btree.BTreeG[*KeyUpdate]
	treeIdx  map[string]*KeyUpdate
	mode     Mode
	tmpdir   string
	Parallel *ParallelUpdate

	direct         []KeyUpdate
	directBytes    int
	directMemLimit int

	hashedTouches [][]byte

	batchSlab []KeyUpdate

	arenas   [arenaRingSize][]byte
	curArena int
	gen      uint64

	addrCache      AddrHashCache
	addrCacheReuse bool
}

const arenaRingSize = 2

func (t *Updates) arenaAlloc(b []byte) []byte {
	arena := t.arenas[t.curArena]
	off := len(arena)
	needed := off + len(b)
	if needed > cap(arena) {
		result := make([]byte, len(b))
		copy(result, b)
		return result
	}
	arena = arena[:needed]
	copy(arena[off:], b)
	t.arenas[t.curArena] = arena
	return arena[off:needed]
}

func (t *Updates) arenaEnsureCap(c int) {
	for i := range t.arenas {
		if cap(t.arenas[i]) < c {
			t.arenas[i] = make([]byte, 0, c)
		}
	}
}

type keyHasher func(key []byte) []byte

func hasherReusesAddrPrefix(h keyHasher) bool {
	return reflect.ValueOf(h).Pointer() == reflect.ValueOf(KeyToHexNibbleHash).Pointer()
}

func (t *Updates) hashKey(key []byte) []byte {
	if t.addrCacheReuse {
		return KeyToHexNibbleHashCached(key, &t.addrCache)
	}
	return t.hasher(key)
}

func (t *Updates) NewEmpty() *Updates {
	return NewUpdates(t.mode, t.tmpdir, t.hasher)
}

func NewUpdates(m Mode, tmpdir string, hasher keyHasher) *Updates {
	t := &Updates{
		hasher:         hasher,
		tmpdir:         tmpdir,
		mode:           m,
		addrCacheReuse: hasherReusesAddrPrefix(hasher),
		directMemLimit: defaultDirectMemLimit,
	}
	switch t.mode {
	case ModeDirect:
		t.keys = make(map[string]struct{})
	case ModeUpdate:
		t.tree = btree.NewG(64, keyUpdateLessFn)
		t.treeIdx = make(map[string]*KeyUpdate)
	case ModeParallel:
		t.keys = make(map[string]struct{})
		t.Parallel = newParallelUpdate()
	case ModeCollect:
		t.treeIdx = make(map[string]*KeyUpdate)
	}
	return t
}

func (t *Updates) SetMode(m Mode) {
	t.mode = m
	switch t.mode {
	case ModeDirect:
		if t.keys == nil {
			t.keys = make(map[string]struct{})
		}
	case ModeUpdate:
		if t.tree == nil {
			t.tree = btree.NewG(64, keyUpdateLessFn)
			t.treeIdx = make(map[string]*KeyUpdate)
		}
	case ModeParallel:
		if t.keys == nil {
			t.keys = make(map[string]struct{})
		}
		if t.Parallel == nil {
			t.Parallel = newParallelUpdate()
		}
	}
	t.Reset()
}

func (t *Updates) initCollector() {
	if t.etl != nil {
		t.etl.Close()
		t.etl = nil
	}
	t.etl = etl.NewCollectorWithAllocator("commitment", t.tmpdir, etl.SmallSortableBuffers, log.Root().New("component", "update-tree")).LogLvl(log.LvlDebug)
	t.etl.SortAndFlushInBackground(true)
}

const defaultDirectMemLimit = 512 << 20

const directEntryOverhead = 48

func plainKeyBytes(pk string) []byte {
	if len(pk) == 0 {
		return []byte{}
	}
	return common.ToBytesZeroCopy(pk)
}

func (t *Updates) collectDirect(hashedKey []byte, plainKey string) {
	if t.etl != nil {
		if err := t.etl.Collect(hashedKey, plainKeyBytes(plainKey)); err != nil {
			log.Warn("failed to collect updated key", "key", fmt.Sprintf("%x", plainKey), "err", err)
		}
		return
	}
	t.direct = append(t.direct, KeyUpdate{HashedKey: hashedKey, PlainKey: plainKey})
	t.directBytes += len(hashedKey) + len(plainKey) + directEntryOverhead
	if t.directBytes >= t.directMemLimit {
		t.spillDirect()
	}
}

func (t *Updates) spillDirect() {
	t.initCollector()
	for i := range t.direct {
		if err := t.etl.Collect(t.direct[i].HashedKey, plainKeyBytes(t.direct[i].PlainKey)); err != nil {
			log.Warn("failed to collect updated key", "key", fmt.Sprintf("%x", t.direct[i].PlainKey), "err", err)
		}
	}
	t.direct, t.directBytes = nil, 0
}

func (t *Updates) Mode() Mode { return t.mode }

func (t *Updates) PlainKeys() map[string]struct{} {
	if (t.mode != ModeDirect && t.mode != ModeParallel) || t.keys == nil {
		return nil
	}
	cp := make(map[string]struct{}, len(t.keys))
	for k := range t.keys {
		cp[k] = struct{}{}
	}
	return cp
}

func (t *Updates) Size() (updates uint64) {
	switch t.mode {
	case ModeDirect, ModeParallel:
		return uint64(len(t.keys))
	case ModeUpdate:
		return uint64(t.tree.Len())
	case ModeCollect:
		return uint64(len(t.treeIdx))
	default:
		return 0
	}
}

func (t *Updates) TouchPlainKey(key string, val []byte, fn func(c *KeyUpdate, val []byte)) {
	switch t.mode {
	case ModeUpdate:
		if existing, ok := t.treeIdx[key]; ok {
			fn(existing, val)
		} else {
			pivot := &KeyUpdate{
				PlainKey:  key,
				HashedKey: t.hashKey(common.ToBytesZeroCopy(key)),
				Update:    new(Update),
			}
			fn(pivot, val)
			t.tree.ReplaceOrInsert(pivot)
			t.treeIdx[key] = pivot
		}
	case ModeDirect:
		if _, ok := t.keys[key]; !ok {
			t.collectDirect(t.hashKey(common.ToBytesZeroCopy(key)), key)
			t.keys[key] = struct{}{}
		}
	case ModeParallel:
		if _, ok := t.keys[key]; ok {
			return
		}
		keyBytes := common.ToBytesZeroCopy(key)
		hashedKey := t.hashKey(keyBytes)
		ik := t.Parallel.internKey(keyBytes)
		t.keys[key] = struct{}{}
		t.Parallel.Collect(hashedKey, ik, nil)
	case ModeCollect:
		existing, ok := t.treeIdx[key]
		if !ok {
			existing = &KeyUpdate{PlainKey: key, Update: new(Update)}
			t.treeIdx[key] = existing
		}
		fn(existing, val)
	default:
	}
}

func (t *Updates) HashKey(key []byte) []byte { return t.hashKey(key) }

func (t *Updates) TouchPlainKeyDirect(key string, update *Update) {
	t.TouchPlainKeyDirectHashed(key, nil, update)
}

func (t *Updates) hashedOr(key string, hashed []byte) []byte {
	if hashed != nil {
		return hashed
	}
	return t.hashKey(common.ToBytesZeroCopy(key))
}

func (t *Updates) TouchPlainKeyDirectHashed(key string, hashed []byte, update *Update) {
	if dbg.TraceTouchKey {
		fmt.Printf("TOUCHDIRECT key=%x flags=%v balance=%s nonce=%d codeHash=%x\n",
			key, update.Flags, update.Balance.String(), update.Nonce, update.CodeHash)
	}
	switch t.mode {
	case ModeUpdate:
		if existing, ok := t.treeIdx[key]; ok {
			mergeUpdateInto(existing.Update, update)
		} else {
			pivot := &KeyUpdate{
				PlainKey:  key,
				HashedKey: t.hashedOr(key, hashed),
				Update:    new(Update),
			}
			*pivot.Update = *update
			t.tree.ReplaceOrInsert(pivot)
			t.treeIdx[key] = pivot
		}
	case ModeDirect:
		if _, ok := t.keys[key]; !ok {
			t.collectDirect(t.hashedOr(key, hashed), key)
			t.keys[key] = struct{}{}
		}
	case ModeParallel:
		keyBytes := common.ToBytesZeroCopy(key)
		hashedKey := t.hashedOr(key, hashed)
		u := new(Update)
		*u = *update
		ik := keyBytes
		if _, ok := t.keys[key]; !ok {
			ik = t.Parallel.internKey(keyBytes)
			t.keys[key] = struct{}{}
		}
		t.Parallel.Collect(hashedKey, ik, u)
	case ModeCollect:
		if existing, ok := t.treeIdx[key]; ok {
			mergeUpdateInto(existing.Update, update)
			return
		}
		u := *update
		t.treeIdx[key] = &KeyUpdate{PlainKey: key, Update: &u}
	default:
	}
}

func mergeUpdateInto(existing, update *Update) {
	if update.DeleteStorageSubtree {
		existing.DeleteStorageSubtree = true
	}
	if update.Flags&DeleteUpdate != 0 {
		existing.Flags = DeleteUpdate
		existing.CodeHash = empty.CodeHash
		return
	}
	existing.Flags &^= DeleteUpdate
	if update.Flags&BalanceUpdate != 0 {
		existing.Balance.Set(&update.Balance)
		existing.Flags |= BalanceUpdate
	}
	if update.Flags&NonceUpdate != 0 {
		existing.Nonce = update.Nonce
		existing.Flags |= NonceUpdate
	}
	if update.Flags&CodeUpdate != 0 {
		existing.CodeHash = update.CodeHash
		existing.Flags |= CodeUpdate
	}
	if update.Flags&StorageUpdate != 0 {
		existing.Storage = update.Storage
		existing.StorageLen = update.StorageLen
		existing.Flags |= StorageUpdate
	}
}

func (t *Updates) Drain(fn func(plainKey string, update *Update) error) error {
	t.hashedTouches = t.hashedTouches[:0]
	for key, item := range t.treeIdx {
		if err := fn(key, item.Update); err != nil {
			return err
		}
	}
	clear(t.treeIdx)
	return nil
}

func (t *Updates) Untouch(plainKey string) bool {
	if t.mode != ModeCollect {
		return false
	}
	_, ok := t.treeIdx[plainKey]
	delete(t.treeIdx, plainKey)
	return ok
}

func (t *Updates) UntouchAll() []string {
	if t.mode != ModeCollect {
		return nil
	}
	keys := slices.Collect(maps.Keys(t.treeIdx))
	clear(t.treeIdx)
	return keys
}

func (t *Updates) TouchHashedKey(hashedKey []byte) {
	switch t.mode {
	case ModeDirect:
		if len(hashedKey) == 0 {
			return
		}
		dedupKey := string(hashedKey)
		if _, ok := t.keys[dedupKey]; !ok {
			t.collectDirect(common.ToBytesZeroCopy(dedupKey), "")
			t.keys[dedupKey] = struct{}{}
		}
	case ModeParallel:
		if len(hashedKey) == 0 {
			return
		}
		dedupKey := string(hashedKey)
		if _, ok := t.keys[dedupKey]; !ok {
			t.Parallel.Collect(common.ToBytesZeroCopy(dedupKey), nil, nil)
			t.keys[dedupKey] = struct{}{}
		}
	case ModeUpdate:
		pivot := &KeyUpdate{HashedKey: bytes.Clone(hashedKey), Update: new(Update)}
		t.tree.ReplaceOrInsert(pivot)
	case ModeCollect:
		if len(hashedKey) != 0 {
			t.hashedTouches = append(t.hashedTouches, bytes.Clone(hashedKey))
		}
	default:
	}
}

func (t *Updates) CollectedHashedKeys() [][]byte {
	keys := make([][]byte, 0, len(t.treeIdx)+len(t.hashedTouches))
	for key := range t.treeIdx {
		keys = append(keys, bytes.Clone(t.hashKey([]byte(key))))
	}
	keys = append(keys, t.hashedTouches...)
	slices.SortFunc(keys, bytes.Compare)
	return slices.CompactFunc(keys, bytes.Equal)
}

func (t *Updates) TouchAccount(c *KeyUpdate, val []byte) {
	if len(val) == 0 {
		c.Update.Flags = DeleteUpdate
		return
	}
	if c.Update.Flags&DeleteUpdate != 0 {
		c.Update.Flags = 0
	}

	acc := accounts.Account{}
	err := accounts.DeserialiseV3(&acc, val)
	if err != nil {
		panic(err)
	}
	c.Update.Nonce = acc.Nonce
	c.Update.Balance.Set(&acc.Balance)
	if acc.CodeHash.IsEmpty() {
		c.Update.CodeHash = empty.CodeHash
	} else {
		c.Update.CodeHash = acc.CodeHash.Value()
	}
	// val is the whole account record, so flag every field: a cell may skip its state
	// read only when the update it was given covers the account completely.
	c.Update.Flags |= BalanceUpdate | NonceUpdate | CodeUpdate
}

func (t *Updates) TouchStorage(c *KeyUpdate, val []byte) {
	c.Update.StorageLen = int8(len(val))
	if len(val) == 0 {
		c.Update.Flags = DeleteUpdate
	} else {
		c.Update.Flags &^= DeleteUpdate
		c.Update.Flags |= StorageUpdate
		copy(c.Update.Storage[:], val)
	}
}

func (t *Updates) TouchCode(c *KeyUpdate, code []byte) {
	if len(code) == 0 {
		if t.mode == ModeCollect && c.Update.Flags == 0 {
			c.Update.Flags = DeleteUpdate
		} else {
			c.Update.Flags |= CodeUpdate
		}
		c.Update.CodeHash = empty.CodeHash
		return
	}
	c.Update.Flags |= CodeUpdate
	c.Update.CodeHash = crypto.Keccak256Hash(code)
}

func (t *Updates) Close() {
	if t.keys != nil {
		clear(t.keys)
	}
	if t.tree != nil {
		t.tree.Clear(true)
		t.tree = nil
	}
	if t.etl != nil {
		t.etl.Close()
		t.etl = nil
	}
	t.direct, t.directBytes = nil, 0
	if t.Parallel != nil {
		t.Parallel.Close()
		t.Parallel = nil
	}
}

const hashSortBatchSize = 10_000

func (t *Updates) hashSortDirectInMem(ctx context.Context, warmuper *Warmuper, fn func(hk, pk []byte, update *Update) error) error {
	slices.SortStableFunc(t.direct, func(a, b KeyUpdate) int {
		return bytes.Compare(a.HashedKey, b.HashedKey)
	})

	var prevKey []byte
	for start := 0; start < len(t.direct); start += hashSortBatchSize {
		batch := t.direct[start:min(start+hashSortBatchSize, len(t.direct))]
		if warmuper != nil {
			for i := range batch {
				hk := batch[i].HashedKey
				startDepth := 0
				minLen := min(len(prevKey), len(hk))
				for startDepth < minLen && prevKey[startDepth] == hk[startDepth] {
					startDepth++
				}
				warmuper.WarmKey(hk, startDepth, t.gen)
				prevKey = hk
			}
		}
		for i := range batch {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := fn(batch[i].HashedKey, plainKeyBytes(batch[i].PlainKey), nil); err != nil {
				return err
			}
		}
	}

	clear(t.direct)
	t.direct = t.direct[:0]
	t.directBytes = 0
	return nil
}

// fn must not retain hk or pk slices after returning: they're backed by reusable arena memory.
func (t *Updates) HashSort(ctx context.Context, warmuper *Warmuper, fn func(hk, pk []byte, update *Update) error) error {
	// [:cap] because the loops below reslice batchSlab many times.
	defer func() { clear(t.batchSlab[:cap(t.batchSlab)]) }()

	switch t.mode {
	case ModeDirect:
		cnt := len(t.keys)
		clear(t.keys)
		if t.etl == nil {
			return t.hashSortDirectInMem(ctx, warmuper, fn)
		}

		t.batchSlab = t.batchSlab[:0]
		if warmuper != nil {
			if err := warmuper.WaitBufferFree(t.curArena); err != nil {
				return err
			}
		}
		t.arenaEnsureCap(min(cnt, hashSortBatchSize) * 192)
		t.arenas[t.curArena] = t.arenas[t.curArena][:0]
		var prevKey []byte

		err := t.etl.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			hk := t.arenaAlloc(k)
			pk := t.arenaAlloc(v)
			t.batchSlab = append(t.batchSlab, KeyUpdate{HashedKey: hk, PlainKey: unsafe.String(unsafe.SliceData(pk), len(pk))})

			if warmuper != nil {
				startDepth := 0
				if prevKey != nil {
					minLen := min(len(prevKey), len(hk))
					for startDepth < minLen && prevKey[startDepth] == hk[startDepth] {
						startDepth++
					}
				}
				warmuper.WarmKey(hk, startDepth, t.gen)
				prevKey = append(prevKey[:0], hk...)
			}

			if len(t.batchSlab) >= hashSortBatchSize {
				for i := range t.batchSlab {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
					if err := fn(t.batchSlab[i].HashedKey, common.ToBytesZeroCopy(t.batchSlab[i].PlainKey), nil); err != nil {
						return err
					}
				}
				t.batchSlab = t.batchSlab[:0]
				nextGen := t.gen + 1
				slot := int(nextGen % arenaRingSize)
				if warmuper != nil {
					if err := warmuper.WaitBufferFree(slot); err != nil {
						return err
					}
				}
				t.gen = nextGen
				t.arenas[slot] = t.arenas[slot][:0]
				t.curArena = slot
			}
			return nil
		}, etl.TransformArgs{Quit: ctx.Done()})
		if err != nil {
			return err
		}

		for i := range t.batchSlab {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := fn(t.batchSlab[i].HashedKey, common.ToBytesZeroCopy(t.batchSlab[i].PlainKey), nil); err != nil {
				return err
			}
		}

		t.etl.Close()
		t.etl = nil

	case ModeUpdate:
		t.batchSlab = t.batchSlab[:0]
		if warmuper != nil {
			if err := warmuper.WaitBufferFree(t.curArena); err != nil {
				return err
			}
		}
		t.arenaEnsureCap(min(t.tree.Len(), hashSortBatchSize) * 144)
		t.arenas[t.curArena] = t.arenas[t.curArena][:0]
		var prevKey []byte
		var processErr error

		t.tree.Ascend(func(item *KeyUpdate) bool {
			select {
			case <-ctx.Done():
				processErr = ctx.Err()
				return false
			default:
			}

			hk := t.arenaAlloc(item.HashedKey)
			t.batchSlab = append(t.batchSlab, KeyUpdate{HashedKey: hk, PlainKey: item.PlainKey, Update: item.Update})

			if warmuper != nil {
				startDepth := 0
				if prevKey != nil {
					minLen := min(len(prevKey), len(hk))
					for startDepth < minLen && prevKey[startDepth] == hk[startDepth] {
						startDepth++
					}
				}
				warmuper.WarmKey(hk, startDepth, t.gen)
				prevKey = append(prevKey[:0], hk...)
			}

			if len(t.batchSlab) >= hashSortBatchSize {
				for i := range t.batchSlab {
					select {
					case <-ctx.Done():
						processErr = ctx.Err()
						return false
					default:
					}
					if err := fn(t.batchSlab[i].HashedKey, common.ToBytesZeroCopy(t.batchSlab[i].PlainKey), t.batchSlab[i].Update); err != nil {
						processErr = err
						return false
					}
				}
				t.batchSlab = t.batchSlab[:0]
				nextGen := t.gen + 1
				slot := int(nextGen % arenaRingSize)
				if warmuper != nil {
					if err := warmuper.WaitBufferFree(slot); err != nil {
						processErr = err
						return false
					}
				}
				t.gen = nextGen
				t.arenas[slot] = t.arenas[slot][:0]
				t.curArena = slot
			}
			return true
		})

		if processErr != nil {
			return processErr
		}

		for i := range t.batchSlab {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := fn(t.batchSlab[i].HashedKey, common.ToBytesZeroCopy(t.batchSlab[i].PlainKey), t.batchSlab[i].Update); err != nil {
				return err
			}
		}
		t.tree.Clear(true)

	case ModeCollect:
		return errors.New("commitment: ModeCollect has no HashSort; use Drain")

	default:
		return nil
	}
	return nil
}

func (t *Updates) ConsumeParallel() {
	if t.mode != ModeParallel {
		return
	}
	clear(t.keys)
	if t.Parallel != nil {
		t.Parallel.Reset()
	}
}

func (t *Updates) Reset() {
	switch t.mode {
	case ModeDirect:
		if t.keys == nil {
			t.keys = make(map[string]struct{})
		} else {
			clear(t.keys)
		}
		if t.etl != nil {
			t.etl.Close()
			t.etl = nil
		}
		clear(t.direct)
		t.direct = t.direct[:0]
		t.directBytes = 0
	case ModeUpdate:
		t.tree.Clear(true)
		clear(t.treeIdx)
	case ModeParallel:
		if t.keys == nil {
			t.keys = make(map[string]struct{})
		} else {
			clear(t.keys)
		}
		if t.Parallel != nil {
			t.Parallel.Reset()
		}
	case ModeCollect:
		clear(t.treeIdx)
		t.hashedTouches = t.hashedTouches[:0]
	default:
	}
	t.batchSlab = t.batchSlab[:0]
	for i := range t.arenas {
		t.arenas[i] = t.arenas[i][:0]
	}
	t.curArena = 0
	t.gen = 0
	t.addrCache.reset()
}

type KeyUpdate struct {
	PlainKey  string
	HashedKey []byte
	Update    *Update
}

// keyUpdateLessFn orders by hashedKey first: Process requires hashedKey-sorted order for
// fold/unfold to walk adjacent trie paths, else the root hash diverges. plainKey is only
// a tiebreaker, e.g. for TouchHashedKey entries sharing a hashedKey with a "real" entry.
func keyUpdateLessFn(i, j *KeyUpdate) bool {
	if c := bytes.Compare(i.HashedKey, j.HashedKey); c != 0 {
		return c < 0
	}
	return i.PlainKey < j.PlainKey
}

type UpdateFlags uint8

const (
	CodeUpdate    UpdateFlags = 1
	DeleteUpdate  UpdateFlags = 2
	BalanceUpdate UpdateFlags = 4
	NonceUpdate   UpdateFlags = 8
	StorageUpdate UpdateFlags = 16
)

func (uf UpdateFlags) String() string {
	var sb strings.Builder
	if uf&DeleteUpdate != 0 {
		sb.WriteString("Delete")
	}
	if uf&BalanceUpdate != 0 {
		sb.WriteString("+Balance")
	}
	if uf&NonceUpdate != 0 {
		sb.WriteString("+Nonce")
	}
	if uf&CodeUpdate != 0 {
		sb.WriteString("+Code")
	}
	if uf&StorageUpdate != 0 {
		sb.WriteString("+Storage")
	}
	return sb.String()
}

type Update struct {
	CodeHash   common.Hash
	Storage    common.Hash
	StorageLen int8
	Flags      UpdateFlags
	Balance    uint256.Int
	Nonce      uint64

	// DeleteStorageSubtree is a transient (never-serialized) signal that this
	// account was self-destructed or created as a contract in the block: its whole
	// storage subtree must be pruned before this account's update is applied, so the
	// account rebuilds from empty storage. Set by the commitment calculator; consumed
	// by the trie during Process. Not a Flags bit because Flags is serialized and merged.
	DeleteStorageSubtree bool
}

func (u *Update) Reset() {
	u.Flags = 0
	u.Balance.Clear()
	u.Nonce = 0
	u.StorageLen = 0
	u.CodeHash = empty.CodeHash
	u.DeleteStorageSubtree = false
}

func (u *Update) Copy() *Update {
	if u == nil {
		return nil
	}
	c := &Update{
		CodeHash:             u.CodeHash,
		Storage:              u.Storage,
		StorageLen:           u.StorageLen,
		Flags:                u.Flags,
		Nonce:                u.Nonce,
		DeleteStorageSubtree: u.DeleteStorageSubtree,
	}
	c.Balance.Set(&u.Balance)
	return c
}

func (u *Update) Merge(b *Update) {
	if b.DeleteStorageSubtree {
		u.DeleteStorageSubtree = true
	}
	if b.Flags == DeleteUpdate {
		u.Flags = DeleteUpdate
		return
	}
	if b.Flags&(BalanceUpdate|NonceUpdate|CodeUpdate|StorageUpdate) != 0 {
		u.Flags &^= DeleteUpdate
	}
	if b.Flags&BalanceUpdate != 0 {
		u.Flags |= BalanceUpdate
		u.Balance.Set(&b.Balance)
	}
	if b.Flags&NonceUpdate != 0 {
		u.Flags |= NonceUpdate
		u.Nonce = b.Nonce
	}
	if b.Flags&CodeUpdate != 0 {
		u.Flags |= CodeUpdate
		copy(u.CodeHash[:], b.CodeHash[:])
	}
	if b.Flags&StorageUpdate != 0 {
		u.Flags |= StorageUpdate
		copy(u.Storage[:], b.Storage[:b.StorageLen])
		u.StorageLen = b.StorageLen
	}
}

func (u *Update) Encode(buf []byte, numBuf []byte) []byte {
	buf = append(buf, byte(u.Flags))
	if u.Flags&BalanceUpdate != 0 {
		buf = append(buf, byte(u.Balance.ByteLen()))
		buf = append(buf, u.Balance.Bytes()...)
	}
	if u.Flags&NonceUpdate != 0 {
		n := binary.PutUvarint(numBuf, u.Nonce)
		buf = append(buf, numBuf[:n]...)
	}
	if u.Flags&CodeUpdate != 0 {
		buf = append(buf, u.CodeHash[:]...)
	}
	if u.Flags&StorageUpdate != 0 {
		n := binary.PutUvarint(numBuf, uint64(u.StorageLen))
		buf = append(buf, numBuf[:n]...)
		if u.StorageLen > 0 {
			buf = append(buf, u.Storage[:u.StorageLen]...)
		}
	}
	return buf
}

func (u *Update) Deleted() bool {
	return u.Flags&DeleteUpdate > 0
}

func (u *Update) Decode(buf []byte, pos int) (int, error) {
	if len(buf) < pos+1 {
		return 0, errors.New("decode Update: buffer too small for flags")
	}
	u.Reset()

	u.Flags = UpdateFlags(buf[pos])
	pos++
	if u.Flags&BalanceUpdate != 0 {
		if len(buf) < pos+1 {
			return 0, errors.New("decode Update: buffer too small for balance len")
		}
		balanceLen := int(buf[pos])
		pos++
		if len(buf) < pos+balanceLen {
			return 0, errors.New("decode Update: buffer too small for balance")
		}
		u.Balance.SetBytes(buf[pos : pos+balanceLen])
		pos += balanceLen
	}
	if u.Flags&NonceUpdate != 0 {
		var n int
		u.Nonce, n = binary.Uvarint(buf[pos:])
		if n == 0 {
			return 0, errors.New("decode Update: buffer too small for nonce")
		}
		if n < 0 {
			return 0, errors.New("decode Update: nonce overflow")
		}
		pos += n
	}
	if u.Flags&CodeUpdate != 0 {
		if len(buf) < pos+length.Hash {
			return 0, errors.New("decode Update: buffer too small for codeHash")
		}
		copy(u.CodeHash[:], buf[pos:pos+32])
		pos += length.Hash
	}
	if u.Flags&StorageUpdate != 0 {
		l, n := binary.Uvarint(buf[pos:])
		if n == 0 {
			return 0, errors.New("decode Update: buffer too small for storage len")
		}
		if n < 0 {
			return 0, errors.New("decode Update: storage pos overflow")
		}
		pos += n
		if len(buf) < pos+int(l) {
			return 0, errors.New("decode Update: buffer too small for storage")
		}
		u.StorageLen = int8(l)
		copy(u.Storage[:], buf[pos:pos+int(u.StorageLen)])
		pos += int(u.StorageLen)
	}
	return pos, nil
}

func (u *Update) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Flags: [%s]", u.Flags))
	if u.Deleted() {
		sb.WriteString(", DELETED")
	}
	if u.Flags&BalanceUpdate != 0 {
		sb.WriteString(fmt.Sprintf(", Balance: [%d]", &u.Balance))
	}
	if u.Flags&NonceUpdate != 0 {
		sb.WriteString(fmt.Sprintf(", Nonce: [%d]", u.Nonce))
	}
	if u.Flags&CodeUpdate != 0 {
		sb.WriteString(fmt.Sprintf(", CodeHash: [%x]", u.CodeHash))
	}
	if u.Flags&StorageUpdate != 0 {
		sb.WriteString(fmt.Sprintf(", Storage: [%x]", u.Storage[:u.StorageLen]))
	}
	return sb.String()
}

// CollapseTracer is invoked when a trie node collapses during a commitment update.
// hashedKeyPath is the surviving child's nibble path; branchPrefix is the collapsing branch's.
type CollapseTracer func(hashedKeyPath, branchPrefix []byte)
