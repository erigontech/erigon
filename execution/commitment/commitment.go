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
	"math/bits"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/google/btree"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	mxTrieProcessedKeys   = metrics.GetOrCreateCounter("domain_commitment_keys")
	mxTrieBranchesUpdated = metrics.GetOrCreateCounter("domain_commitment_updates_applied")

	mxTrieStateSkipRate                 = metrics.GetOrCreateCounter("trie_state_skip_rate")
	mxTrieStateLoadRate                 = metrics.GetOrCreateCounter("trie_state_load_rate")
	mxTrieStateLevelledSkipRatesAccount = [...]metrics.Counter{
		metrics.GetOrCreateCounter(`trie_state_levelled_skip_rate{level="L0",key="account"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_skip_rate{level="L1",key="account"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_skip_rate{level="L2",key="account"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_skip_rate{level="L3",key="account"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_skip_rate{level="L4",key="account"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_skip_rate{level="recent",key="account"}`),
	}
	mxTrieStateLevelledSkipRatesStorage = [...]metrics.Counter{
		metrics.GetOrCreateCounter(`trie_state_levelled_skip_rate{level="L0",key="storage"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_skip_rate{level="L1",key="storage"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_skip_rate{level="L2",key="storage"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_skip_rate{level="L3",key="storage"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_skip_rate{level="L4",key="storage"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_skip_rate{level="recent",key="storage"}`),
	}
	mxTrieStateLevelledLoadRatesAccount = [...]metrics.Counter{
		metrics.GetOrCreateCounter(`trie_state_levelled_load_rate{level="L0",key="account"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_load_rate{level="L1",key="account"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_load_rate{level="L2",key="account"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_load_rate{level="L3",key="account"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_load_rate{level="L4",key="account"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_load_rate{level="recent",key="account"}`),
	}
	mxTrieStateLevelledLoadRatesStorage = [...]metrics.Counter{
		metrics.GetOrCreateCounter(`trie_state_levelled_load_rate{level="L0",key="storage"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_load_rate{level="L1",key="storage"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_load_rate{level="L2",key="storage"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_load_rate{level="L3",key="storage"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_load_rate{level="L4",key="storage"}`),
		metrics.GetOrCreateCounter(`trie_state_levelled_load_rate{level="recent",key="storage"}`),
	}
)

type Trie interface {
	RootHash() (hash []byte, err error)

	SetTraceWriter(io.Writer)
	EnableCsvMetrics(filePathPrefix string)

	Variant() TrieVariant

	Reset()

	ResetContext(ctx PatriciaContext)

	Process(ctx context.Context, updates *Updates, logPrefix string, onProgress func(*CommitProgress), warmup WarmupConfig) (rootHash []byte, err error)

	Release()
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
	PutBranch(prefix []byte, data []byte, prevData []byte) error
	Account(plainKey []byte) (*Update, error)
	Storage(plainKey []byte) (*Update, error)
}

type TrieVariant string

const (
	VariantHexPatriciaTrie     TrieVariant = "hex-patricia-hashed"
	VariantParallelHexPatricia TrieVariant = "hex-parallel-patricia-hashed"
)

func InitializeTrieAndUpdates(mode Mode, tmpdir string, cfg TrieConfig) (Trie, *Updates) {
	switch cfg.Variant {
	case VariantParallelHexPatricia:
		// ParallelPatriciaHashed requires ModeParallel to allocate the prefix-trie state it reads.
		trie := NewParallelPatriciaHashed(nil, length.Addr, cfg)
		tree := NewUpdates(ModeParallel, tmpdir, KeyToHexNibbleHash)
		return trie, tree
	case VariantHexPatriciaTrie:
		fallthrough
	default:

		trie := NewHexPatriciaHashed(length.Addr, nil, cfg)
		tree := NewUpdates(mode, tmpdir, KeyToHexNibbleHash)
		return trie, tree
	}
}

type cellFields uint8

const (
	fieldExtension   cellFields = 1
	fieldAccountAddr cellFields = 2
	fieldStorageAddr cellFields = 4
	fieldHash        cellFields = 8
	fieldStateHash   cellFields = 16
)

func (p cellFields) String() string {
	var sb strings.Builder
	if p&fieldExtension != 0 {
		sb.WriteString("DownHash")
	}
	if p&fieldAccountAddr != 0 {
		sb.WriteString("+AccountPlain")
	}
	if p&fieldStorageAddr != 0 {
		sb.WriteString("+StoragePlain")
	}
	if p&fieldHash != 0 {
		sb.WriteString("+Hash")
	}
	if p&fieldStateHash != 0 {
		sb.WriteString("+LeafHash")
	}
	return sb.String()
}

// TODO: unify with cell by shrinking cell struct to eliminate this separate type
type cellEncodeData struct {
	extension   [64]byte
	accountAddr [20]byte
	storageAddr [length.Addr + length.Hash]byte
	hash        [32]byte
	stateHash   [32]byte

	extLen         int16
	accountAddrLen int16
	storageAddrLen int16
	hashLen        int16
	stateHashLen   int16
}

func cellEncodeDataFromCell(c *cell) cellEncodeData {
	var d cellEncodeData
	d.extLen = c.extLen
	d.accountAddrLen = c.accountAddrLen
	d.storageAddrLen = c.storageAddrLen
	d.hashLen = c.hashLen
	d.stateHashLen = c.stateHashLen
	copy(d.extension[:], c.extension[:c.extLen])
	copy(d.accountAddr[:], c.accountAddr[:c.accountAddrLen])
	copy(d.storageAddr[:], c.storageAddr[:c.storageAddrLen])
	copy(d.hash[:], c.hash[:c.hashLen])
	copy(d.stateHash[:], c.stateHash[:c.stateHashLen])
	return d
}

type DeferredBranchUpdate struct {
	prefix  []byte
	raw     BranchData
	prev    []byte
	encoded BranchData
}

var deferredUpdatePool = &sync.Pool{
	New: func() any {
		return &DeferredBranchUpdate{}
	},
}

var getDeferredUpdateCount atomic.Int64

func ResetDeferredUpdateMetrics() {
	getDeferredUpdateCount.Store(0)
}

func GetDeferredUpdateMetrics() int64 {
	return getDeferredUpdateCount.Load()
}

func getDeferredUpdate(prefix []byte, raw, prev []byte) *DeferredBranchUpdate {
	getDeferredUpdateCount.Add(1)
	upd := deferredUpdatePool.Get().(*DeferredBranchUpdate)

	upd.prefix = reuseBytes(upd.prefix, prefix)
	upd.raw = reuseBytes(upd.raw, raw)
	// prev stays cloned: it is the one argument that is legitimately nil or empty, and
	// callers read a nil prev as "look up the previous value". Deriving that shape from a
	// recycled buffer's capacity rather than from the input is not worth one allocation.
	upd.prev = bytes.Clone(prev)
	upd.encoded = nil

	return upd
}

// reuseBytes copies src into dst's backing array, matching bytes.Clone's nil handling:
// a nil src yields nil, so pool history cannot change the result's nil-ness. Callers
// distinguish a nil prev ("look it up") from an empty one ("known absent").
func reuseBytes(dst, src []byte) []byte {
	if src == nil {
		return nil
	}
	if cap(dst) == 0 {
		dst = make([]byte, 0, len(src)) // non-nil even for an empty src, as bytes.Clone is
	}
	return append(dst[:0], src...)
}

// capLen hides a recycled buffer's leftover capacity, so what a callback sees is derived
// from the input rather than from whichever update last used the pooled object.
func capLen(b []byte) []byte {
	if b == nil {
		return nil
	}
	return slices.Clip(b)
}

// putDeferredUpdate returns a DeferredBranchUpdate to the global pool.
func putDeferredUpdate(upd *DeferredBranchUpdate) {
	if upd != nil {
		upd.prev = nil
		upd.encoded = nil
		deferredUpdatePool.Put(upd)
	}
}

type PendingCommitmentUpdate struct {
	BlockNum uint64
	// BlockHash disambiguates changeset lookups sharing a block number.
	BlockHash common.Hash
	TxNum     uint64
	Deferred  []*DeferredBranchUpdate
}

func (p *PendingCommitmentUpdate) Clear() {
	for _, upd := range p.Deferred {
		putDeferredUpdate(upd)
	}
	p.Deferred = nil
}

// BranchCache is populated by SharedDomains.Commit, not by this encoder.
type BranchEncoder struct {
	buf       *bytes.Buffer
	bitmapBuf [binary.MaxVarintLen64]byte
	merger    *BranchMerger
	metrics   *Metrics

	deferUpdates       bool
	maxDeferredUpdates int
	deferred           []*DeferredBranchUpdate
	pendingPrefixes    *maphash.NonConcurrentMap[struct{}]
}

func NewBranchEncoder(sz uint64) *BranchEncoder {
	return &BranchEncoder{
		buf:    bytes.NewBuffer(make([]byte, sz)),
		merger: NewHexBranchMerger(sz / 2),
	}
}

func (be *BranchEncoder) setDeferUpdates(defer_ bool) {
	be.deferUpdates = defer_
	if defer_ {
		if be.deferred == nil {
			be.deferred = make([]*DeferredBranchUpdate, 0, 64)
		}
		if be.pendingPrefixes == nil {
			be.pendingPrefixes = maphash.NewNonConcurrentMap[struct{}]()
		}
	}
}

func (be *BranchEncoder) DeferUpdatesEnabled() bool {
	return be.deferUpdates
}

func (be *BranchEncoder) HasPendingPrefix(prefix []byte) bool {
	if be.pendingPrefixes == nil {
		return false
	}
	_, found := be.pendingPrefixes.Get(prefix)
	return found
}

func (be *BranchEncoder) ClearDeferred() {
	for _, upd := range be.deferred {
		putDeferredUpdate(upd)
	}
	be.deferred = be.deferred[:0]
	if be.pendingPrefixes != nil {
		be.pendingPrefixes.Clear()
	}
	ResetDeferredUpdateMetrics()
}

func mergeDeferredUpdate(upd *DeferredBranchUpdate, merger *BranchMerger) error {
	if len(upd.prev) > 0 {
		if bytes.Equal(upd.prev, upd.raw) {
			upd.encoded = nil
			return nil
		}
		merged, err := merger.Merge(upd.prev, upd.raw)
		if err != nil {
			return err
		}
		upd.encoded = bytes.Clone(merged)
		return nil
	}
	upd.encoded = upd.raw
	return nil
}

// putBranch is bound by the same no-retain rule as PatriciaContext.PutBranch.
func (be *BranchEncoder) ApplyDeferredUpdates(
	numWorkers int,
	putBranch func(prefix []byte, data []byte, prevData []byte) error,
) error {
	written, err := ApplyDeferredBranchUpdates(be.deferred, numWorkers, putBranch)
	if err != nil {
		return err
	}
	if be.metrics != nil {
		be.metrics.updateBranch.Add(uint64(written))
	}
	return nil
}

var workerMergerPool = sync.Pool{New: func() any { return NewHexBranchMerger(512) }}

// Returns the number of updates written. putBranch must copy prefix and data rather than
// retain them: they are pooled and reused for a later, unrelated update. prevData is
// cloned per update and carries no such constraint.
func ApplyDeferredBranchUpdates(
	deferred []*DeferredBranchUpdate,
	numWorkers int,
	putBranch func(prefix []byte, data []byte, prevData []byte) error,
) (int, error) {
	if len(deferred) == 0 {
		return 0, nil
	}
	if numWorkers <= 1 {
		numWorkers = 1
	}

	if numWorkers == 1 || len(deferred) <= numWorkers {
		merger := workerMergerPool.Get().(*BranchMerger)
		defer workerMergerPool.Put(merger)

		var written int
		for _, upd := range deferred {
			if err := mergeDeferredUpdate(upd, merger); err != nil {
				return written, err
			}
			if upd.encoded == nil {
				continue
			}
			if err := putBranch(capLen(upd.prefix), capLen(upd.encoded), capLen(upd.prev)); err != nil {
				return written, err
			}
			written++
		}
		mxTrieBranchesUpdated.AddInt(written)
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
			merger := workerMergerPool.Get().(*BranchMerger)
			defer workerMergerPool.Put(merger)
			for i := lo; i < hi; i++ {
				if err := mergeDeferredUpdate(deferred[i], merger); err != nil {
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

	var written int
	for _, upd := range deferred {
		if upd.encoded == nil {
			continue
		}
		if err := putBranch(capLen(upd.prefix), capLen(upd.encoded), capLen(upd.prev)); err != nil {
			return written, err
		}
		written++
	}
	mxTrieBranchesUpdated.AddInt(written)
	return written, nil
}

func (be *BranchEncoder) setMetrics(metrics *Metrics) {
	be.metrics = metrics
}

func (be *BranchEncoder) CollectUpdate(
	ctx PatriciaContext,
	prefix []byte,
	bitmap, touchMap, afterMap uint16,
	cells *[16]cellEncodeData,
	isNew bool,
) error {
	var prev []byte
	var err error

	if !isNew {
		prev, _, err = ctx.Branch(prefix)
		if err != nil {
			return err
		}
	}
	if prev == nil {
		prev = []byte{}
	}

	update, err := be.EncodeBranch(bitmap, touchMap, afterMap, cells)
	if err != nil {
		return err
	}

	if len(prev) > 0 {
		if bytes.Equal(prev, update) {
			return nil
		}
		update, err = be.merger.Merge(prev, update)
		if err != nil {
			return err
		}
	}

	prefixCopy := bytes.Clone(prefix)
	updateCopy := bytes.Clone(update)
	if err := ctx.PutBranch(prefixCopy, updateCopy, prev); err != nil {
		return err
	}
	if be.metrics != nil {
		be.metrics.updateBranch.Add(1)
	}
	mxTrieBranchesUpdated.Inc()
	return nil
}

func (be *BranchEncoder) CollectDeferredUpdate(
	ctx PatriciaContext,
	prefix []byte,
	bitmap, touchMap, afterMap uint16,
	cells *[16]cellEncodeData,
	isNew bool,
) error {
	limit := be.maxDeferredUpdates
	if limit == 0 {
		limit = DefaultMaxDeferredUpdates
	}
	needsFlush := len(be.deferred) >= limit
	if !needsFlush {
		_, needsFlush = be.pendingPrefixes.Get(prefix)
	}

	if needsFlush {
		if err := be.ApplyDeferredUpdates(16, ctx.PutBranch); err != nil {
			return err
		}
		be.ClearDeferred()
	}

	var prev []byte
	var err error

	if !isNew {
		prev, _, err = ctx.Branch(prefix)
		if err != nil {
			return err
		}
	}
	if prev == nil {
		prev = []byte{}
	}

	be.pendingPrefixes.Set(prefix, struct{}{})

	raw, err := be.EncodeBranch(bitmap, touchMap, afterMap, cells)
	if err != nil {
		return err
	}
	be.deferred = append(be.deferred, getDeferredUpdate(prefix, raw, prev))
	return nil
}

func (be *BranchEncoder) putUvarAndVal(size uint64, val []byte) error {
	n := binary.PutUvarint(be.bitmapBuf[:], size)
	if _, err := be.buf.Write(be.bitmapBuf[:n]); err != nil {
		return err
	}
	if _, err := be.buf.Write(val); err != nil {
		return err
	}
	return nil
}

func (be *BranchEncoder) EncodeBranch(bitmap, touchMap, afterMap uint16, cells *[16]cellEncodeData) (BranchData, error) {
	be.buf.Reset()

	var encoded [4]byte
	binary.BigEndian.PutUint16(encoded[:], touchMap)
	binary.BigEndian.PutUint16(encoded[2:], afterMap)
	if _, err := be.buf.Write(encoded[:]); err != nil {
		return nil, err
	}

	for bitset := afterMap; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		cell := &cells[nibble]

		if bitmap&bit != 0 {
			var fields cellFields
			if cell.extLen > 0 && cell.storageAddrLen == 0 {
				fields |= fieldExtension
			}
			if cell.accountAddrLen > 0 {
				fields |= fieldAccountAddr
			}
			if cell.storageAddrLen > 0 {
				fields |= fieldStorageAddr
			}
			if cell.hashLen > 0 {
				fields |= fieldHash
			}
			if cell.stateHashLen == 32 && (cell.accountAddrLen > 0 || cell.storageAddrLen > 0) {
				fields |= fieldStateHash
			}
			if err := be.buf.WriteByte(byte(fields)); err != nil {
				return nil, err
			}
			if fields&fieldExtension != 0 {
				if err := be.putUvarAndVal(uint64(cell.extLen), cell.extension[:cell.extLen]); err != nil {
					return nil, err
				}
			}
			if fields&fieldAccountAddr != 0 {
				if err := be.putUvarAndVal(uint64(cell.accountAddrLen), cell.accountAddr[:cell.accountAddrLen]); err != nil {
					return nil, err
				}
			}
			if fields&fieldStorageAddr != 0 {
				if err := be.putUvarAndVal(uint64(cell.storageAddrLen), cell.storageAddr[:cell.storageAddrLen]); err != nil {
					return nil, err
				}
			}
			if fields&fieldHash != 0 {
				if err := be.putUvarAndVal(uint64(cell.hashLen), cell.hash[:cell.hashLen]); err != nil {
					return nil, err
				}
			}
			if fields&fieldStateHash != 0 {
				if err := be.putUvarAndVal(uint64(cell.stateHashLen), cell.stateHash[:cell.stateHashLen]); err != nil {
					return nil, err
				}
			}
		}
		bitset ^= bit
	}
	return be.buf.Bytes(), nil
}

type BranchData []byte

func (branchData BranchData) ChildCount() int {
	if len(branchData) < 4 {
		return 0
	}
	return bits.OnesCount16(binary.BigEndian.Uint16(branchData[2:4]))
}

func (branchData BranchData) IsTombstone() bool { return len(branchData) == 0 }

func (branchData BranchData) String() string {
	if branchData.IsTombstone() {
		return ""
	}
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	pos := 4
	var sb strings.Builder
	var cell cell
	fmt.Fprintf(&sb, "(%d) touchMap %016b, afterMap %016b\n", len(branchData), touchMap, afterMap)
	for bitset, j := touchMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		fmt.Fprintf(&sb, "   %x => ", nibble)
		if afterMap&bit == 0 {
			sb.WriteString("{DELETED}\n")
		} else {
			fields := cellFields(branchData[pos])
			pos++
			var err error
			if pos, err = cell.fillFromFields(branchData, pos, fields); err != nil {
				panic(err)
			}
			sb.WriteString("{")
			var comma string
			if cell.hashedExtLen > 0 {
				fmt.Fprintf(&sb, "hashedExtension=[%x]", cell.hashedExtension[:cell.hashedExtLen])
				comma = ","
			}
			if cell.accountAddrLen > 0 {
				fmt.Fprintf(&sb, "%saccountAddr=[%x]", comma, cell.accountAddr[:cell.accountAddrLen])
				comma = ","
			}
			if cell.storageAddrLen > 0 {
				fmt.Fprintf(&sb, "%sstorageAddr=[%x]", comma, cell.storageAddr[:cell.storageAddrLen])
				comma = ","
			}
			if cell.hashLen > 0 {
				fmt.Fprintf(&sb, "%shash=[%x]", comma, cell.hash[:cell.hashLen])
			}
			if cell.stateHashLen > 0 {
				fmt.Fprintf(&sb, "%sleafHash=[%x]", comma, cell.stateHash[:cell.stateHashLen])
			}
			sb.WriteString("}\n")
		}
		bitset ^= bit
	}
	return sb.String()
}

var errShortenedKeyFound = errors.New("shortened key found")

// A malformed branch reports true: treat as referenced, never under-report.
func (branchData BranchData) HasShortenedKeys() bool {
	_, err := branchData.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		if isStorage {
			if len(key) != length.Addr+length.Hash {
				return nil, errShortenedKeyFound
			}
		} else if len(key) != length.Addr {
			return nil, errShortenedKeyFound
		}
		return nil, nil
	})
	return err != nil
}

// If fn returns nil, the original key is kept.
func (branchData BranchData) ReplacePlainKeys(newData []byte, fn func(key []byte, isStorage bool) (newKey []byte, err error)) (BranchData, error) {
	if len(branchData) < 4 {
		return branchData, nil
	}

	var numBuf [binary.MaxVarintLen64]byte
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	if touchMap&afterMap == 0 {
		return branchData, nil
	}

	pos := 4
	anyChanged := false
	spanStart := 0
	for bitset, j := touchMap&afterMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		fields := cellFields(branchData[pos])
		pos++
		if fields&fieldExtension != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for hashedKey len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for hashedKey len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for hashedKey: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				pos += int(l)
			}
		}
		if fields&fieldAccountAddr != 0 {
			keyFieldStart := pos
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for accountAddr len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for accountAddr len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for accountAddr: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				pos += int(l)
			}
			newKey, err := fn(branchData[pos-int(l):pos], false)
			if err != nil {
				return nil, err
			}
			if newKey != nil {
				if !anyChanged {
					if cap(newData) < len(branchData) {
						newData = make([]byte, 0, len(branchData))
					} else {
						newData = newData[:0]
					}
					anyChanged = true
				}
				newData = append(newData, branchData[spanStart:keyFieldStart]...)
				n = binary.PutUvarint(numBuf[:], uint64(len(newKey)))
				newData = append(newData, numBuf[:n]...)
				newData = append(newData, newKey...)
				spanStart = pos
			}
		}
		if fields&fieldStorageAddr != 0 {
			keyFieldStart := pos
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for storageAddr len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for storageAddr len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for storageAddr: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				pos += int(l)
			}
			newKey, err := fn(branchData[pos-int(l):pos], true)
			if err != nil {
				return nil, err
			}
			if newKey != nil {
				if !anyChanged {
					if cap(newData) < len(branchData) {
						newData = make([]byte, 0, len(branchData))
					} else {
						newData = newData[:0]
					}
					anyChanged = true
				}
				newData = append(newData, branchData[spanStart:keyFieldStart]...)
				n = binary.PutUvarint(numBuf[:], uint64(len(newKey)))
				newData = append(newData, numBuf[:n]...)
				newData = append(newData, newKey...)
				spanStart = pos
			}
		}
		if fields&fieldHash != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for hash len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for hash len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for hash: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				pos += int(l)
			}
		}
		if fields&fieldStateHash != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for acLeaf hash len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for acLeafhash len")
			}
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for LeafHash: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				pos += int(l)
			}
		}

		bitset ^= bit
	}

	if !anyChanged {
		return branchData, nil
	}
	newData = append(newData, branchData[spanStart:]...)
	return newData, nil
}

func (branchData BranchData) IsComplete() bool {
	if len(branchData) < 4 {
		return false
	}
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	return ^touchMap&afterMap == 0
}

// branch2 shadows branch1 where both touch the same cell.
func (branchData BranchData) MergeHexBranches(branchData2 BranchData, newData []byte) (BranchData, error) {
	if branchData2 == nil {
		return branchData, nil
	}
	if branchData == nil {
		return branchData2, nil
	}

	touchMap1 := binary.BigEndian.Uint16(branchData[0:])
	afterMap1 := binary.BigEndian.Uint16(branchData[2:])
	bitmap1 := touchMap1 & afterMap1
	pos1 := 4
	touchMap2 := binary.BigEndian.Uint16(branchData2[0:])
	afterMap2 := binary.BigEndian.Uint16(branchData2[2:])
	bitmap2 := touchMap2 & afterMap2
	pos2 := 4
	var bitmapBuf [4]byte
	binary.BigEndian.PutUint16(bitmapBuf[0:], touchMap1|touchMap2)
	binary.BigEndian.PutUint16(bitmapBuf[2:], afterMap2)
	newData = append(newData[:0], bitmapBuf[:]...)
	for bitset, j := bitmap1|bitmap2, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		if bitmap2&bit != 0 {
			fields := cellFields(branchData2[pos2])
			newData = append(newData, byte(fields))
			pos2++
			for i := 0; i < bits.OnesCount8(byte(fields)); i++ {
				l, n := binary.Uvarint(branchData2[pos2:])
				if n == 0 {
					return nil, errors.New("MergeHexBranches buffer2 too small for field")
				} else if n < 0 {
					return nil, errors.New("MergeHexBranches value2 overflow for field")
				}
				newData = append(newData, branchData2[pos2:pos2+n]...)
				pos2 += n
				if len(branchData2) < pos2+int(l) {
					return nil, fmt.Errorf("MergeHexBranches buffer2 too small for %s : expected %d got %d", fields&cellFields(1<<i), pos2+int(l), len(branchData2))
				}
				if l > 0 {
					newData = append(newData, branchData2[pos2:pos2+int(l)]...)
					pos2 += int(l)
				}
			}
		}
		if bitmap1&bit != 0 {
			add := (touchMap2&bit == 0) && (afterMap2&bit != 0)
			fields := cellFields(branchData[pos1])
			if add {
				newData = append(newData, byte(fields))
			}
			pos1++
			for i := 0; i < bits.OnesCount8(byte(fields)); i++ {
				l, n := binary.Uvarint(branchData[pos1:])
				if n == 0 {
					return nil, errors.New("MergeHexBranches buffer1 too small for field")
				} else if n < 0 {
					return nil, errors.New("MergeHexBranches value1 overflow for field")
				}
				if add {
					newData = append(newData, branchData[pos1:pos1+n]...)
				}
				pos1 += n
				if len(branchData) < pos1+int(l) {
					return nil, fmt.Errorf("MergeHexBranches buffer1 too small for %s : expected %d got %d", fields&cellFields(1<<i), pos1+int(l), len(branchData))
				}
				if l > 0 {
					if add {
						newData = append(newData, branchData[pos1:pos1+int(l)]...)
					}
					pos1 += int(l)
				}
			}
		}
		bitset ^= bit
	}
	return newData, nil
}

func (branchData BranchData) decodeCells() (touchMap, afterMap uint16, row [16]*cell, err error) {
	touchMap = binary.BigEndian.Uint16(branchData[0:])
	afterMap = binary.BigEndian.Uint16(branchData[2:])
	pos := 4
	for bitset, j := touchMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		if afterMap&bit != 0 {
			fields := cellFields(branchData[pos])
			pos++
			row[nibble] = new(cell)
			if pos, err = row[nibble].fillFromFields(branchData, pos, fields); err != nil {
				err = fmt.Errorf("failed to fill cell at nibble %x: %w", nibble, err)
				return
			}
		}
		bitset ^= bit
	}
	return
}

func (branchData BranchData) Validate(branchKey []byte) error {
	if branchData.IsTombstone() {
		return nil
	}
	_, afterMap, row, err := branchData.decodeCells()
	if err != nil {
		return err
	}
	if err := validateAfterMap(afterMap, row); err != nil {
		return err
	}
	if err := validatePlainKeys(branchKey, row, keccak.NewFastKeccak()); err != nil {
		return err
	}
	return nil
}

func validateAfterMap(afterMap uint16, row [16]*cell) error {
	cellsInAfterMap := bits.OnesCount16(afterMap)
	var decodedCellsCount int
	for _, c := range row {
		if c != nil {
			decodedCellsCount++
		}
	}
	if cellsInAfterMap != decodedCellsCount {
		return fmt.Errorf("cells in after map does not match branch data: %d vs %d", cellsInAfterMap, decodedCellsCount)
	}
	return nil
}

func validatePlainKeys(branchKey []byte, row [16]*cell, keccak keccak.KeccakState) error {
	uncompactedBranchKey := nibbles.CompactToHex(branchKey)
	if nibbles.HasTerm(uncompactedBranchKey) {
		uncompactedBranchKey = uncompactedBranchKey[:len(uncompactedBranchKey)-1]
	}
	if len(uncompactedBranchKey) > 128 {
		return fmt.Errorf("branch key too long: %d", len(branchKey))
	}
	var hashBuf common.Hash
	depth := int16(len(uncompactedBranchKey))
	for _, c := range row {
		if c == nil {
			continue
		}
		if c.accountAddrLen == 0 && c.storageAddrLen == 0 {
			continue
		}
		err := c.deriveHashedKeys(depth, keccak, length.Addr, hashBuf[:])
		if err != nil {
			return err
		}
		hashedExtLen := c.hashedExtLen
		hashedExt := c.hashedExtension[:hashedExtLen]
		if c.extLen > 0 && hashedExtLen >= c.extLen {
			hashedExtLen -= c.extLen
			hashedExt = hashedExt[:hashedExtLen]
		}
		branchKeyAndExtNibbles := make([]byte, len(uncompactedBranchKey)+int(hashedExtLen))
		copy(branchKeyAndExtNibbles, uncompactedBranchKey)
		copy(branchKeyAndExtNibbles[len(uncompactedBranchKey):], hashedExt)
		var plainKeyNibbles []byte
		if c.accountAddrLen > 0 {
			plainKeyNibbles = KeyToHexNibbleHash(c.accountAddr[:])
		}
		if c.storageAddrLen > 0 {
			plainKeyNibbles = KeyToHexNibbleHash(c.storageAddr[:])
			if c.accountAddrLen > 0 {
				if !bytes.Equal(c.accountAddr[:], c.storageAddr[:length.Addr]) {
					return fmt.Errorf("accountAddr mismatch with storageAddr: %s != %x", common.BytesToAddress(c.accountAddr[:]), common.BytesToHash(c.storageAddr[:length.Addr]))
				}
			}
		}
		if !bytes.Equal(plainKeyNibbles, branchKeyAndExtNibbles) {
			return fmt.Errorf("branch and hashed extension nibbles dont match plainKey nibbles: %x vs %x", plainKeyNibbles, branchKeyAndExtNibbles)
		}
	}
	return nil
}

type BranchMerger struct {
	buf []byte
	num [4]byte
}

func NewHexBranchMerger(capacity uint64) *BranchMerger {
	return &BranchMerger{buf: make([]byte, capacity)}
}

// branch2 shadows branch1 where both touch the same cell.
func (m *BranchMerger) Merge(branch1 BranchData, branch2 BranchData) (BranchData, error) {
	if len(branch2) == 0 {
		return branch1, nil
	}
	if len(branch1) == 0 {
		return branch2, nil
	}

	touchMap1 := binary.BigEndian.Uint16(branch1[0:])
	afterMap1 := binary.BigEndian.Uint16(branch1[2:])
	bitmap1 := touchMap1 & afterMap1
	pos1 := 4

	touchMap2 := binary.BigEndian.Uint16(branch2[0:])
	afterMap2 := binary.BigEndian.Uint16(branch2[2:])
	bitmap2 := touchMap2 & afterMap2
	pos2 := 4

	binary.BigEndian.PutUint16(m.num[0:], touchMap1|touchMap2)
	binary.BigEndian.PutUint16(m.num[2:], afterMap2)
	dataPos := 4

	m.buf = append(m.buf[:0], m.num[:]...)

	for bitset, j := bitmap1|bitmap2, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		if bitmap2&bit != 0 {
			fields := cellFields(branch2[pos2])
			m.buf = append(m.buf, byte(fields))
			pos2++

			for i := 0; i < bits.OnesCount8(byte(fields)); i++ {
				l, n := binary.Uvarint(branch2[pos2:])
				if n == 0 {
					return nil, errors.New("MergeHexBranches branch2 is too small: expected node info size")
				} else if n < 0 {
					return nil, errors.New("MergeHexBranches branch2: size overflow for length")
				}

				m.buf = append(m.buf, branch2[pos2:pos2+n]...)
				pos2 += n
				dataPos += n
				if len(branch2) < pos2+int(l) {
					return nil, fmt.Errorf("MergeHexBranches branch2 is too small: expected at least %d got %d bytes", pos2+int(l), len(branch2))
				}
				if l > 0 {
					m.buf = append(m.buf, branch2[pos2:pos2+int(l)]...)
					pos2 += int(l)
					dataPos += int(l)
				}
			}
		}
		if bitmap1&bit != 0 {
			add := (touchMap2&bit == 0) && (afterMap2&bit != 0)
			fields := cellFields(branch1[pos1])
			if add {
				m.buf = append(m.buf, byte(fields))
			}
			pos1++
			for i := 0; i < bits.OnesCount8(byte(fields)); i++ {
				l, n := binary.Uvarint(branch1[pos1:])
				if n == 0 {
					return nil, errors.New("MergeHexBranches branch1 is too small: expected node info size")
				} else if n < 0 {
					return nil, errors.New("MergeHexBranches branch1: size overflow for length")
				}

				if add {
					m.buf = append(m.buf, branch1[pos1:pos1+n]...)
				}
				pos1 += n
				if len(branch1) < pos1+int(l) {
					return nil, fmt.Errorf("MergeHexBranches branch1 is too small: expected at least %d got %d bytes", pos1+int(l), len(branch1))
				}
				if l > 0 {
					if add {
						m.buf = append(m.buf, branch1[pos1:pos1+int(l)]...)
					}
					pos1 += int(l)
				}
			}
		}
		bitset ^= bit
	}
	return m.buf, nil
}

func ParseTrieVariant(s string) TrieVariant {
	var trieVariant TrieVariant
	switch s {
	case "parallel":
		trieVariant = VariantParallelHexPatricia
	case "hex":
		fallthrough
	default:
		trieVariant = VariantHexPatriciaTrie
	}
	return trieVariant
}

type BranchStat struct {
	KeySize       uint64
	ValSize       uint64
	MinCellSize   uint64
	MaxCellSize   uint64
	CellCount     uint64
	APKSize       uint64
	SPKSize       uint64
	ExtSize       uint64
	HashSize      uint64
	APKCount      uint64
	SPKCount      uint64
	HashCount     uint64
	ExtCount      uint64
	TAMapsSize    uint64
	LeafHashSize  uint64
	LeafHashCount uint64
	MedianAPK     uint64
	MedianSPK     uint64
	MedianHash    uint64
	MedianExt     uint64
	MedianLH      uint64
	IsRoot        bool
}

func (bs *BranchStat) Collect(other *BranchStat) {
	if other == nil {
		return
	}

	bs.KeySize += other.KeySize
	bs.ValSize += other.ValSize
	bs.MinCellSize = min(bs.MinCellSize, other.MinCellSize)
	bs.MaxCellSize = max(bs.MaxCellSize, other.MaxCellSize)
	bs.CellCount += other.CellCount
	bs.APKSize += other.APKSize
	bs.SPKSize += other.SPKSize
	bs.ExtSize += other.ExtSize
	bs.HashSize += other.HashSize
	bs.APKCount += other.APKCount
	bs.SPKCount += other.SPKCount
	bs.HashCount += other.HashCount
	bs.ExtCount += other.ExtCount

	setMedian := func(median *uint64, otherMedian uint64) {
		if *median == 0 {
			*median = otherMedian
		} else {
			*median = (*median + otherMedian) / 2
		}
	}
	setMedian(&bs.MedianExt, other.MedianExt)
	setMedian(&bs.MedianAPK, other.MedianAPK)
	setMedian(&bs.MedianSPK, other.MedianSPK)
	setMedian(&bs.MedianHash, other.MedianHash)
	setMedian(&bs.MedianLH, other.MedianLH)
	bs.MedianHash = (bs.MedianHash + other.MedianHash) / 2
	bs.MedianAPK = (bs.MedianAPK + other.MedianAPK) / 2
	bs.MedianSPK = (bs.MedianSPK + other.MedianSPK) / 2
	bs.MedianLH = (bs.MedianLH + other.MedianLH) / 2
	bs.TAMapsSize += other.TAMapsSize
	bs.LeafHashSize += other.LeafHashSize
	bs.LeafHashCount += other.LeafHashCount
}

func DecodeBranchAndCollectStat(key, branch []byte, tv TrieVariant) *BranchStat {
	stat := &BranchStat{}
	if len(key) == 0 {
		return nil
	}

	stat.KeySize = uint64(len(key))
	stat.ValSize = uint64(len(branch))
	stat.IsRoot = true

	if !bytes.Equal(key, []byte("state")) {
		stat.IsRoot = false

		tm, am, cells, err := BranchData(branch).decodeCells()
		if err != nil {
			return nil
		}
		stat.TAMapsSize = uint64(2 + 2)
		stat.CellCount = uint64(bits.OnesCount16(tm & am))

		medians := make(map[string][]int16)
		for _, c := range cells {
			if c == nil {
				continue
			}
			enc := uint64(len(c.Encode()))
			stat.MinCellSize = min(stat.MinCellSize, enc)
			stat.MaxCellSize = max(stat.MaxCellSize, enc)
			switch {
			case c.accountAddrLen > 0:
				stat.APKSize += uint64(c.accountAddrLen)
				stat.APKCount++
				medians["apk"] = append(medians["apk"], c.accountAddrLen)
			case c.storageAddrLen > 0:
				stat.SPKSize += uint64(c.storageAddrLen)
				stat.SPKCount++
				medians["spk"] = append(medians["spk"], c.storageAddrLen)
			case c.hashLen > 0:
				stat.HashSize += uint64(c.hashLen)
				stat.HashCount++
				medians["hash"] = append(medians["hash"], c.hashLen)
			case c.stateHashLen > 0:
				stat.LeafHashSize += uint64(c.stateHashLen)
				stat.LeafHashCount++
				medians["lh"] = append(medians["lh"], c.stateHashLen)
			case c.extLen > 0:
				stat.ExtSize += uint64(c.extLen)
				stat.ExtCount++
				medians["ext"] = append(medians["ext"], c.extLen)
			default:
				panic("unexpected cell " + c.FullString())
			}
			if c.extLen > 0 {
				if tv == VariantHexPatriciaTrie {
					stat.ExtSize += uint64(c.extLen)
				}
				stat.ExtCount++
			}
		}

		for k, v := range medians {
			slices.Sort(v)
			switch k {
			case "apk":
				stat.MedianAPK = uint64(v[len(v)/2])
			case "spk":
				stat.MedianSPK = uint64(v[len(v)/2])
			case "hash":
				stat.MedianHash = uint64(v[len(v)/2])
			case "ext":
				stat.MedianExt = uint64(v[len(v)/2])
			case "lh":
				stat.MedianLH = uint64(v[len(v)/2])
			}
		}
	}
	return stat
}

type Mode uint

const (
	ModeDisabled Mode = 0
	ModeDirect   Mode = 1
	ModeUpdate   Mode = 2
	ModeParallel Mode = 3
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
	parallel *parallelUpdate

	direct         []KeyUpdate
	directBytes    int
	directMemLimit int

	batchSlab []KeyUpdate

	arenas   [arenaRingSize][]byte
	curArena int
	gen      uint64

	addrCache      addrHashCache
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

func (t *Updates) IsConcurrentCommitment() bool {
	return t.mode == ModeParallel
}

type keyHasher func(key []byte) []byte

func keyHasherNoop(key []byte) []byte { return key }

func hasherReusesAddrPrefix(h keyHasher) bool {
	return reflect.ValueOf(h).Pointer() == reflect.ValueOf(KeyToHexNibbleHash).Pointer()
}

func (t *Updates) hashKey(key []byte) []byte {
	if t.addrCacheReuse {
		return keyToHexNibbleHashCached(key, &t.addrCache)
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
		t.parallel = newParallelUpdate()
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
		if t.parallel == nil {
			t.parallel = newParallelUpdate()
		}
	}
	t.Reset()
}

func (t *Updates) initCollector() {
	if t.etl != nil {
		t.etl.Close()
		t.etl = nil
	}
	t.etl = etl.NewCollectorWithAllocator("commitment", t.tmpdir, etl.SmallSortableBuffers, log.Root().New("update-tree")).LogLvl(log.LvlDebug)
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
	t.direct = append(t.direct, KeyUpdate{hashedKey: hashedKey, plainKey: plainKey})
	t.directBytes += len(hashedKey) + len(plainKey) + directEntryOverhead
	if t.directBytes >= t.directMemLimit {
		t.spillDirect()
	}
}

func (t *Updates) spillDirect() {
	t.initCollector()
	for i := range t.direct {
		if err := t.etl.Collect(t.direct[i].hashedKey, plainKeyBytes(t.direct[i].plainKey)); err != nil {
			log.Warn("failed to collect updated key", "key", fmt.Sprintf("%x", t.direct[i].plainKey), "err", err)
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
				plainKey:  key,
				hashedKey: t.hashKey(common.ToBytesZeroCopy(key)),
				update:    new(Update),
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
		keyBytes := common.ToBytesZeroCopy(key)
		hashedKey := t.hashKey(keyBytes)
		ik := keyBytes
		if _, ok := t.keys[key]; !ok {
			ik = t.parallel.internKey(keyBytes)
			t.keys[key] = struct{}{}
		}
		t.parallel.Insert(hashedKey, ik, nil)
	default:
	}
}

func (t *Updates) TouchPlainKeyDirect(key string, update *Update) {
	if dbg.TraceTouchKey {
		fmt.Printf("TOUCHDIRECT key=%x flags=%v balance=%s nonce=%d codeHash=%x\n",
			key, update.Flags, update.Balance.String(), update.Nonce, update.CodeHash)
	}
	switch t.mode {
	case ModeUpdate:
		if existing, ok := t.treeIdx[key]; ok {
			if update.Flags&DeleteUpdate != 0 {
				existing.update.Flags = DeleteUpdate
				existing.update.CodeHash = empty.CodeHash
			} else {
				existing.update.Flags &^= DeleteUpdate
				if update.Flags&BalanceUpdate != 0 {
					existing.update.Balance.Set(&update.Balance)
					existing.update.Flags |= BalanceUpdate
				}
				if update.Flags&NonceUpdate != 0 {
					existing.update.Nonce = update.Nonce
					existing.update.Flags |= NonceUpdate
				}
				if update.Flags&CodeUpdate != 0 {
					existing.update.CodeHash = update.CodeHash
					existing.update.Flags |= CodeUpdate
				}
				if update.Flags&StorageUpdate != 0 {
					existing.update.Storage = update.Storage
					existing.update.StorageLen = update.StorageLen
					existing.update.Flags |= StorageUpdate
				}
			}
		} else {
			pivot := &KeyUpdate{
				plainKey:  key,
				hashedKey: t.hashKey(common.ToBytesZeroCopy(key)),
				update:    new(Update),
			}
			*pivot.update = *update
			t.tree.ReplaceOrInsert(pivot)
			t.treeIdx[key] = pivot
		}
	case ModeDirect:
		if _, ok := t.keys[key]; !ok {
			t.collectDirect(t.hashKey(common.ToBytesZeroCopy(key)), key)
			t.keys[key] = struct{}{}
		}
	case ModeParallel:
		keyBytes := common.ToBytesZeroCopy(key)
		hashedKey := t.hashKey(keyBytes)
		u := new(Update)
		*u = *update
		ik := keyBytes
		if _, ok := t.keys[key]; !ok {
			ik = t.parallel.internKey(keyBytes)
			t.keys[key] = struct{}{}
		}
		t.parallel.Insert(hashedKey, ik, u)
	default:
	}
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
			t.parallel.Insert(hashedKey, nil, nil)
			t.keys[dedupKey] = struct{}{}
		}
	case ModeUpdate:
		pivot := &KeyUpdate{hashedKey: bytes.Clone(hashedKey), update: new(Update)}
		t.tree.ReplaceOrInsert(pivot)
	default:
	}
}

func (t *Updates) TouchAccount(c *KeyUpdate, val []byte) {
	if len(val) == 0 {
		c.update.Flags = DeleteUpdate
		return
	}
	if c.update.Flags&DeleteUpdate != 0 {
		c.update.Flags = 0
	}

	acc := accounts.Account{}
	err := accounts.DeserialiseV3(&acc, val)
	if err != nil {
		panic(err)
	}
	if c.update.Nonce != acc.Nonce {
		c.update.Nonce = acc.Nonce
		c.update.Flags |= NonceUpdate
	}
	if !c.update.Balance.Eq(&acc.Balance) {
		c.update.Balance.Set(&acc.Balance)
		c.update.Flags |= BalanceUpdate
	}
	if acc.CodeHash.Value() != c.update.CodeHash {
		if acc.CodeHash.IsEmpty() {
			c.update.CodeHash = empty.CodeHash
		} else {
			c.update.Flags |= CodeUpdate
			c.update.CodeHash = acc.CodeHash.Value()
		}
	}
}

func (t *Updates) TouchStorage(c *KeyUpdate, val []byte) {
	c.update.StorageLen = int8(len(val))
	if len(val) == 0 {
		c.update.Flags = DeleteUpdate
	} else {
		c.update.Flags &^= DeleteUpdate
		c.update.Flags |= StorageUpdate
		copy(c.update.Storage[:], val)
	}
}

func (t *Updates) TouchCode(c *KeyUpdate, code []byte) {
	c.update.Flags |= CodeUpdate
	if len(code) == 0 {
		if c.update.Flags == 0 {
			c.update.Flags = DeleteUpdate
		}
		c.update.CodeHash = empty.CodeHash
		return
	}
	c.update.CodeHash = crypto.Keccak256Hash(code)
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
	if t.parallel != nil {
		t.parallel.Close()
		t.parallel = nil
	}
}

const hashSortBatchSize = 10_000

func (t *Updates) hashSortDirectInMem(ctx context.Context, warmuper *Warmuper, fn func(hk, pk []byte, update *Update) error) error {
	slices.SortStableFunc(t.direct, func(a, b KeyUpdate) int {
		return bytes.Compare(a.hashedKey, b.hashedKey)
	})

	var prevKey []byte
	for start := 0; start < len(t.direct); start += hashSortBatchSize {
		batch := t.direct[start:min(start+hashSortBatchSize, len(t.direct))]
		if warmuper != nil {
			for i := range batch {
				hk := batch[i].hashedKey
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
			if err := fn(batch[i].hashedKey, plainKeyBytes(batch[i].plainKey), nil); err != nil {
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
			t.batchSlab = append(t.batchSlab, KeyUpdate{hashedKey: hk, plainKey: unsafe.String(unsafe.SliceData(pk), len(pk))})

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
					if err := fn(t.batchSlab[i].hashedKey, common.ToBytesZeroCopy(t.batchSlab[i].plainKey), nil); err != nil {
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
			if err := fn(t.batchSlab[i].hashedKey, common.ToBytesZeroCopy(t.batchSlab[i].plainKey), nil); err != nil {
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

			hk := t.arenaAlloc(item.hashedKey)
			t.batchSlab = append(t.batchSlab, KeyUpdate{hashedKey: hk, plainKey: item.plainKey, update: item.update})

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
					if err := fn(t.batchSlab[i].hashedKey, common.ToBytesZeroCopy(t.batchSlab[i].plainKey), t.batchSlab[i].update); err != nil {
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
			if err := fn(t.batchSlab[i].hashedKey, common.ToBytesZeroCopy(t.batchSlab[i].plainKey), t.batchSlab[i].update); err != nil {
				return err
			}
		}
		t.tree.Clear(true)

	default:
		return nil
	}
	return nil
}

func (t *Updates) consumeParallel() {
	if t.mode != ModeParallel {
		return
	}
	clear(t.keys)
	if t.parallel != nil {
		t.parallel.Reset()
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
		if t.parallel != nil {
			t.parallel.Reset()
		}
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
	plainKey  string
	hashedKey []byte
	update    *Update
}

// keyUpdateLessFn orders by hashedKey first: Process requires hashedKey-sorted order for
// fold/unfold to walk adjacent trie paths, else the root hash diverges. plainKey is only
// a tiebreaker, e.g. for TouchHashedKey entries sharing a hashedKey with a "real" entry.
func keyUpdateLessFn(i, j *KeyUpdate) bool {
	if c := bytes.Compare(i.hashedKey, j.hashedKey); c != 0 {
		return c < 0
	}
	return i.plainKey < j.plainKey
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
}

func (u *Update) Reset() {
	u.Flags = 0
	u.Balance.Clear()
	u.Nonce = 0
	u.StorageLen = 0
	u.CodeHash = empty.CodeHash
}

func (u *Update) Copy() *Update {
	if u == nil {
		return nil
	}
	c := &Update{
		CodeHash:   u.CodeHash,
		Storage:    u.Storage,
		StorageLen: u.StorageLen,
		Flags:      u.Flags,
		Nonce:      u.Nonce,
	}
	c.Balance.Set(&u.Balance)
	return c
}

func (u *Update) Merge(b *Update) {
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
