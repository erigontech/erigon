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
	"math/bits"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/btree"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/crypto/keccak"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/metrics"
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

// Trie represents commitment variant.
type Trie interface {
	// RootHash produces root hash of the trie
	RootHash() (hash []byte, err error)

	// Makes trie more verbose
	SetTrace(bool)
	// Trace domain writes only (no filding etc)
	SetTraceDomain(bool)
	SetCapture(capture []string)
	GetCapture(truncate bool) []string
	EnableCsvMetrics(filePathPrefix string)
	// EnableWarmupCache enables/disables warmup cache during Process (false by default)
	EnableWarmupCache(bool)

	// Variant returns commitment trie variant
	Variant() TrieVariant

	// Reset Drops everything from the trie
	Reset()

	// Set context for state IO
	ResetContext(ctx PatriciaContext)

	// Process updates. If warmup.Enabled is true, pre-warms MDBX page cache in parallel.
	Process(ctx context.Context, updates *Updates, logPrefix string, progress chan *CommitProgress, warmup WarmupConfig) (rootHash []byte, err error)
}

type CommitProgress struct {
	KeyIndex    uint64
	UpdateCount uint64
	Metrics     MetricValues
}

type PatriciaContext interface {
	// GetBranch load branch node and fill up the cells
	// For each cell, it sets the cell type, clears the modified flag, fills the hash,
	// and for the extension, account, and leaf type, the `l` and `k`
	Branch(prefix []byte) ([]byte, kv.Step, error)
	// store branch data
	PutBranch(prefix []byte, data []byte, prevData []byte, prevStep kv.Step) error
	// fetch account with given plain key
	Account(plainKey []byte) (*Update, error)
	// fetch storage with given plain key
	Storage(plainKey []byte) (*Update, error)
	// TxNum returns the current transaction number
	TxNum() uint64
}

type TrieVariant string

const (
	// VariantHexPatriciaTrie used as default commitment approach
	VariantHexPatriciaTrie TrieVariant = "hex-patricia-hashed"
	// VariantBinPatriciaTrie - Experimental mode with binary key representation
	VariantBinPatriciaTrie       TrieVariant = "bin-patricia-hashed"
	VariantConcurrentHexPatricia TrieVariant = "hex-concurrent-patricia-hashed"
)

func InitializeTrieAndUpdates(tv TrieVariant, mode Mode, tmpdir string) (Trie, *Updates) {
	switch tv {
	case VariantConcurrentHexPatricia:
		root := NewHexPatriciaHashed(length.Addr, nil)
		trie := NewConcurrentPatriciaHashed(root, nil)
		tree := NewUpdates(mode, tmpdir, KeyToHexNibbleHash)
		// tree.SetConcurrentCommitment(true) // first run always sequential
		return trie, tree
	case VariantBinPatriciaTrie:
		//trie := NewBinPatriciaHashed(length.Addr, nil, tmpdir)
		//fn := func(key []byte) []byte { return hexToBin(key) }
		//tree := NewUpdateTree(mode, tmpdir, fn)
		//return trie, tree
		panic("VariantBinPatriciaTrie not supported")
	case VariantHexPatriciaTrie:
		fallthrough
	default:

		trie := NewHexPatriciaHashed(length.Addr, nil)
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

// cellEncodeData contains only the fields needed for EncodeBranch.
// This is much smaller than cell (which has hashedExtension[128], Update, etc.)
type cellEncodeData struct {
	extension   [64]byte
	accountAddr [20]byte                        // common.Address
	storageAddr [length.Addr + length.Hash]byte // 20 + 32 = 52 bytes
	hash        [32]byte                        // common.Hash
	stateHash   [32]byte                        // common.Hash

	extLen         int16
	accountAddrLen int16
	storageAddrLen int16
	hashLen        int16
	stateHashLen   int16
}

// DeferredBranchUpdate holds the data needed to perform a branch update later.
// This allows collecting updates during the fold phase and running computeCellHash + EncodeBranch in parallel.
type DeferredBranchUpdate struct {
	prefix   []byte
	bitmap   uint16
	touchMap uint16
	afterMap uint16

	// Cells needed for EncodeBranch - only the fields required for encoding
	cells [16]cellEncodeData
	depth int16

	// Previous data from ctx.Branch (for merging)
	prev     []byte
	prevStep kv.Step

	// Result after encoding (filled by parallel workers)
	encoded BranchData
}

// Global pool for deferred branch updates.
var deferredUpdatePool = sync.Pool{
	New: func() any {
		return &DeferredBranchUpdate{}
	},
}

// Metrics for getDeferredUpdate - use atomics for thread safety
var getDeferredUpdateCount atomic.Int64

// ResetDeferredUpdateMetrics resets the getDeferredUpdate metrics.
func ResetDeferredUpdateMetrics() {
	getDeferredUpdateCount.Store(0)
}

// GetDeferredUpdateMetrics returns the count for getDeferredUpdate calls.
func GetDeferredUpdateMetrics() int64 {
	return getDeferredUpdateCount.Load()
}

// cellArraysSize is the size of the contiguous arrays in cell (extension through stateHash).
// extension[64] + accountAddr[20] + storageAddr[52] + hash[32] + stateHash[32] = 200 bytes
const cellArraysSize = 64 + 20 + 52 + 32 + 32

// getDeferredUpdate gets a DeferredBranchUpdate from the global pool
// and copies only the fields needed for encoding.
func getDeferredUpdate(
	prefix []byte,
	bitmap, touchMap, afterMap uint16,
	cells *[16]cell,
	depth int16,
	prev []byte,
	prevStep kv.Step,
) *DeferredBranchUpdate {
	getDeferredUpdateCount.Add(1)
	upd := deferredUpdatePool.Get().(*DeferredBranchUpdate)

	upd.prefix = prefix
	upd.bitmap = bitmap
	upd.touchMap = touchMap
	upd.afterMap = afterMap
	upd.depth = depth

	// Only copy cells that are in bitmap (touchMap & afterMap) - those are the only ones accessed during encoding
	for bitset := bitmap; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		src := &cells[nibble]
		dst := &upd.cells[nibble]

		// Copy length fields (10 bytes total, but not contiguous with arrays in src)
		dst.extLen = src.extLen
		dst.accountAddrLen = src.accountAddrLen
		dst.storageAddrLen = src.storageAddrLen
		dst.hashLen = src.hashLen
		dst.stateHashLen = src.stateHashLen

		// Single memcpy for all arrays (200 bytes) - contiguous in both src and dst
		copy(
			unsafe.Slice((*byte)(unsafe.Pointer(&dst.extension[0])), cellArraysSize),
			unsafe.Slice((*byte)(unsafe.Pointer(&src.extension[0])), cellArraysSize),
		)

		bitset ^= bit
	}

	upd.prev = prev
	upd.prevStep = prevStep
	upd.encoded = nil

	return upd
}

// putDeferredUpdate returns a DeferredBranchUpdate to the global pool.
func putDeferredUpdate(upd *DeferredBranchUpdate) {
	if upd != nil {
		deferredUpdatePool.Put(upd)
	}
}

// PendingCommitmentUpdate stores deferred branch updates for a specific block.
// Used by the commitment context to defer branch update application until a later flush.
type PendingCommitmentUpdate struct {
	BlockNum uint64
	TxNum    uint64
	Deferred []*DeferredBranchUpdate
}

// Clear returns all deferred updates to the pool and nils the slice.
func (p *PendingCommitmentUpdate) Clear() {
	for _, upd := range p.Deferred {
		putDeferredUpdate(upd)
	}
	p.Deferred = nil
}

type BranchEncoder struct {
	buf       *bytes.Buffer
	bitmapBuf [binary.MaxVarintLen64]byte
	merger    *BranchMerger
	metrics   *Metrics

	// Deferred updates support
	deferUpdates    bool
	deferred        []*DeferredBranchUpdate
	pendingPrefixes *maphash.NonConcurrentMap[struct{}] // tracks pending prefixes to detect duplicates
	cache           *WarmupCache
}

func NewBranchEncoder(sz uint64) *BranchEncoder {
	return &BranchEncoder{
		buf:    bytes.NewBuffer(make([]byte, sz)),
		merger: NewHexBranchMerger(sz / 2),
	}
}

// SetDeferUpdates enables or disables deferred update collection.
// When enabled, CollectUpdate will store updates in a cache instead of applying them immediately.
func (be *BranchEncoder) SetDeferUpdates(defer_ bool) {
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

// DeferUpdatesEnabled returns whether deferred update collection is enabled.
func (be *BranchEncoder) DeferUpdatesEnabled() bool {
	return be.deferUpdates
}

// HasPendingPrefix returns true if the given prefix has a pending deferred update.
func (be *BranchEncoder) HasPendingPrefix(prefix []byte) bool {
	if be.pendingPrefixes == nil {
		return false
	}
	_, found := be.pendingPrefixes.Get(prefix)
	return found
}

// ClearDeferred clears the deferred updates list and returns all objects to the pool.
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

// encodeDeferredUpdate encodes a branch update using the provided encoder and merger.
// Cell hashes are already computed during fold() before cells were copied.
func encodeDeferredUpdate(
	upd *DeferredBranchUpdate,
	encoder *BranchEncoder,
	merger *BranchMerger,
) error {
	update, err := encoder.encodeBranchFromCellData(upd.bitmap, upd.touchMap, upd.afterMap, &upd.cells)
	if err != nil {
		return err
	}

	if len(upd.prev) > 0 {
		if bytes.Equal(upd.prev, update) {
			upd.encoded = nil // skip unchanged
			return nil
		}
		update, err = merger.Merge(upd.prev, update)
		if err != nil {
			return err
		}
	}

	upd.encoded = common.Copy(update)
	return nil
}

// ApplyDeferredUpdates encodes branch updates concurrently and writes them.
func (be *BranchEncoder) ApplyDeferredUpdates(
	numWorkers int,
	putBranch func(prefix []byte, data []byte, prevData []byte, prevStep kv.Step) error,
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

// ApplyDeferredBranchUpdates encodes deferred branch updates concurrently and writes them.
// Returns the number of updates successfully written.
func ApplyDeferredBranchUpdates(
	deferred []*DeferredBranchUpdate,
	numWorkers int,
	putBranch func(prefix []byte, data []byte, prevData []byte, prevStep kv.Step) error,
) (int, error) {
	start := time.Now()
	defer func() {
		log.Debug("ApplyDeferredBranchUpdates completed", "updates", len(deferred), "took", time.Since(start))
	}()
	if len(deferred) == 0 {
		return 0, nil
	}
	if numWorkers <= 1 {
		numWorkers = 1
	}

	// Pipeline: workers encode in parallel, results sent to channel, main goroutine writes sequentially
	type result struct {
		upd *DeferredBranchUpdate
		err error
	}
	resultCh := make(chan result, maxDeferredUpdates)
	workCh := make(chan *DeferredBranchUpdate, maxDeferredUpdates)

	// Start workers - each with its own encoder and merger
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			encoder := NewBranchEncoder(1024)
			merger := NewHexBranchMerger(512)

			for upd := range workCh {
				err := encodeDeferredUpdate(upd, encoder, merger)
				resultCh <- result{upd: upd, err: err}
			}
		}()
	}

	// Close resultCh when all workers are done
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Send work in background
	go func() {
		for _, upd := range deferred {
			workCh <- upd
		}
		close(workCh)
	}()

	// Process results as they come in - write to storage immediately
	var firstErr error
	var written int
	for res := range resultCh {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
			}
			continue
		}
		if res.upd.encoded == nil {
			continue // skip unchanged
		}
		if firstErr != nil {
			continue // drain channel but don't write after error
		}
		if err := putBranch(res.upd.prefix, res.upd.encoded, res.upd.prev, res.upd.prevStep); err != nil {
			firstErr = err
			continue
		}
		mxTrieBranchesUpdated.Inc()
		written++
	}

	return written, firstErr
}

func (be *BranchEncoder) setMetrics(metrics *Metrics) {
	be.metrics = metrics
}

func (be *BranchEncoder) SetCache(cache *WarmupCache) {
	be.cache = cache
}

func (be *BranchEncoder) CollectUpdate(
	ctx PatriciaContext,
	prefix []byte,
	bitmap, touchMap, afterMap uint16,
	readCell func(nibble int, skip bool) (*cell, error),
) (lastNibble int, err error) {
	var prev []byte
	var prevStep kv.Step
	var foundInCache bool

	if be.cache != nil {
		prev, prevStep, foundInCache = be.cache.GetAndEvictBranch(prefix)
	}
	if !foundInCache {
		prev, prevStep, err = ctx.Branch(prefix)
		if err != nil {
			return 0, err
		}
	}

	update, lastNibble, err := be.EncodeBranch(bitmap, touchMap, afterMap, readCell)
	if err != nil {
		return 0, err
	}

	if len(prev) > 0 {
		if bytes.Equal(prev, update) {
			//fmt.Printf("skip collectBranchUpdate [%x]\n", prefix)
			return lastNibble, nil // do not write the same data for prefix
		}
		update, err = be.merger.Merge(prev, update)
		if err != nil {
			return 0, err
		}
	}

	//fmt.Printf("\ncollectBranchUpdate [%x] -> %s\n", prefix, BranchData(update).String())
	// has to copy :(
	prefixCopy := common.Copy(prefix)
	updateCopy := common.Copy(update)
	if err = ctx.PutBranch(prefixCopy, updateCopy, prev, prevStep); err != nil {
		return 0, err
	}
	// Update cache with the new branch data
	if be.cache != nil {
		be.cache.PutBranch(prefixCopy, updateCopy, prevStep)
	}
	if be.metrics != nil {
		be.metrics.updateBranch.Add(1)
	}
	mxTrieBranchesUpdated.Inc()
	return lastNibble, nil
}

const maxDeferredUpdates = 50_000

// CollectDeferredUpdate stores a branch update job for later parallel processing.
// Unlike CollectUpdate, this does NOT call computeCellHash or EncodeBranch - it copies the cells
// and defers encoding for parallel execution later.
// Cell hashes are already computed during fold() before this is called.
// Flushes pending updates if a duplicate prefix is detected or if deferred count exceeds maxDeferredUpdates.
func (be *BranchEncoder) CollectDeferredUpdate(
	ctx PatriciaContext,
	prefix []byte,
	bitmap, touchMap, afterMap uint16,
	cells *[16]cell,
	depth int16,
) error {
	// Flush if duplicate prefix or too many deferred updates
	needsFlush := len(be.deferred) >= maxDeferredUpdates
	if !needsFlush {
		_, needsFlush = be.pendingPrefixes.Get(prefix)
	}

	if needsFlush {
		if err := be.ApplyDeferredUpdates(16, ctx.PutBranch); err != nil {
			return err
		}
		be.ClearDeferred()
	}

	// try to get previous data from cache
	var (
		prev         []byte
		prevStep     kv.Step
		foundInCache bool
		err          error
	)

	if be.cache != nil {
		prev, prevStep, foundInCache = be.cache.GetAndEvictBranch(prefix)
	}
	if !foundInCache {
		prev, prevStep, err = ctx.Branch(prefix)
	}
	if err != nil {
		return err
	}

	// Track this prefix as pending
	be.pendingPrefixes.Set(prefix, struct{}{})

	// Get a pooled DeferredBranchUpdate and copy all fields
	upd := getDeferredUpdate(prefix, bitmap, touchMap, afterMap, cells, depth, prev, prevStep)
	be.deferred = append(be.deferred, upd)
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

// Encoded result should be copied before next call to EncodeBranch, underlying slice is reused
func (be *BranchEncoder) EncodeBranch(bitmap, touchMap, afterMap uint16, readCell func(nibble int, skip bool) (*cell, error)) (BranchData, int, error) {
	be.buf.Reset()

	var encoded [4]byte
	binary.BigEndian.PutUint16(encoded[:], touchMap)
	binary.BigEndian.PutUint16(encoded[2:], afterMap)
	if _, err := be.buf.Write(encoded[:]); err != nil {
		return nil, 0, err
	}

	var lastNibble int
	for bitset, j := afterMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		for i := lastNibble; i < nibble; i++ {
			if _, err := readCell(i, true /* skip */); err != nil {
				return nil, 0, err
			} // only writes 0x80 into hasher
		}
		lastNibble = nibble + 1

		cell, err := readCell(nibble, false)
		if err != nil {
			return nil, 0, err
		}

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
				return nil, 0, err
			}
			if fields&fieldExtension != 0 {
				if err := be.putUvarAndVal(uint64(cell.extLen), cell.extension[:cell.extLen]); err != nil {
					return nil, 0, err
				}
			}
			if fields&fieldAccountAddr != 0 {
				if err := be.putUvarAndVal(uint64(cell.accountAddrLen), cell.accountAddr[:cell.accountAddrLen]); err != nil {
					return nil, 0, err
				}
			}
			if fields&fieldStorageAddr != 0 {
				if err := be.putUvarAndVal(uint64(cell.storageAddrLen), cell.storageAddr[:cell.storageAddrLen]); err != nil {
					return nil, 0, err
				}
			}
			if fields&fieldHash != 0 {
				if err := be.putUvarAndVal(uint64(cell.hashLen), cell.hash[:cell.hashLen]); err != nil {
					return nil, 0, err
				}
			}
			if fields&fieldStateHash != 0 {
				if err := be.putUvarAndVal(uint64(cell.stateHashLen), cell.stateHash[:cell.stateHashLen]); err != nil {
					return nil, 0, err
				}
			}
		}
		bitset ^= bit
	}
	//fmt.Printf("EncodeBranch [%x] size: %d\n", be.buf.Bytes(), be.buf.Len())
	return be.buf.Bytes(), lastNibble, nil
}

// encodeBranchFromCellData encodes branch data from cellEncodeData (used by deferred updates).
// This avoids the overhead of the readCell callback and works directly with the minimal cell data.
func (be *BranchEncoder) encodeBranchFromCellData(bitmap, touchMap, afterMap uint16, cells *[16]cellEncodeData) (BranchData, error) {
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

func RetrieveCellNoop(nibble int, skip bool) (*cell, error) { return nil, nil }

type BranchData []byte

func (branchData BranchData) String() string {
	if len(branchData) == 0 {
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
				// This is used for test output, so ok to panic
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

// if fn returns nil, the original key will be copied from branchData
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
	newData = append(newData[:0], branchData[:4]...)
	for bitset, j := touchMap&afterMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		fields := cellFields(branchData[pos])
		newData = append(newData, byte(fields))
		pos++
		if fields&fieldExtension != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for hashedKey len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for hashedKey len")
			}
			newData = append(newData, branchData[pos:pos+n]...)
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for hashedKey: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				newData = append(newData, branchData[pos:pos+int(l)]...)
				pos += int(l)
			}
		}
		if fields&fieldAccountAddr != 0 {
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
			if newKey == nil {
				newData = append(newData, branchData[pos-int(l)-n:pos]...)
				//if l != length.Addr {
				//	fmt.Printf("COPY %x LEN %d\n", []byte(branchData[pos-int(l):pos]), l)
				//}
			} else {
				//if len(newKey) > 8 && len(newKey) != length.Addr {
				//	fmt.Printf("SHORT %x LEN %d\n", newKey, len(newKey))
				//}

				n = binary.PutUvarint(numBuf[:], uint64(len(newKey)))
				newData = append(newData, numBuf[:n]...)
				newData = append(newData, newKey...)
			}
		}
		if fields&fieldStorageAddr != 0 {
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
			if newKey == nil {
				newData = append(newData, branchData[pos-int(l)-n:pos]...) // -n to include length
				if l != length.Addr+length.Hash {
					fmt.Printf("COPY %x LEN %d\n", []byte(branchData[pos-int(l):pos]), l)
				}
			} else {
				if len(newKey) > 8 && len(newKey) != length.Addr+length.Hash {
					fmt.Printf("SHORT %x LEN %d\n", newKey, len(newKey))
				}

				n = binary.PutUvarint(numBuf[:], uint64(len(newKey)))
				newData = append(newData, numBuf[:n]...)
				newData = append(newData, newKey...)
			}
		}
		if fields&fieldHash != 0 {
			l, n := binary.Uvarint(branchData[pos:])
			if n == 0 {
				return nil, errors.New("replacePlainKeys buffer too small for hash len")
			} else if n < 0 {
				return nil, errors.New("replacePlainKeys value overflow for hash len")
			}
			newData = append(newData, branchData[pos:pos+n]...)
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for hash: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				newData = append(newData, branchData[pos:pos+int(l)]...)
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
			newData = append(newData, branchData[pos:pos+n]...)
			pos += n
			if len(branchData) < pos+int(l) {
				return nil, fmt.Errorf("replacePlainKeys buffer too small for LeafHash: expected %d got %d", pos+int(l), len(branchData))
			}
			if l > 0 {
				newData = append(newData, branchData[pos:pos+int(l)]...)
				pos += int(l)
			}
		}

		bitset ^= bit
	}

	return newData, nil
}

// IsComplete determines whether given branch data is complete, meaning that all information about all the children is present
// Each of 16 children of a branch node have two attributes
// touch - whether this child has been modified or deleted in this branchData (corresponding bit in touchMap is set)
// after - whether after this branchData application, the child is present in the tree or not (corresponding bit in afterMap is set)
func (branchData BranchData) IsComplete() bool {
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	return ^touchMap&afterMap == 0
}

// MergeHexBranches combines two branchData, number 2 coming after (and potentially shadowing) number 1
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
			// Add fields from branchData2
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
			add := (touchMap2&bit == 0) && (afterMap2&bit != 0) // Add fields from branchData1
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
	if len(branchData) == 0 {
		return nil
	}
	_, afterMap, row, err := branchData.decodeCells()
	if err != nil {
		return err
	}
	if err = validateAfterMap(afterMap, row); err != nil {
		return err
	}
	if err = validatePlainKeys(branchKey, row, keccak.NewFastKeccak()); err != nil {
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

func validatePlainKeys(branchKey []byte, row [16]*cell, keccak keccakState) error {
	uncompactedBranchKey := uncompactNibbles(branchKey)
	if HasTerm(uncompactedBranchKey) {
		uncompactedBranchKey = uncompactedBranchKey[:len(uncompactedBranchKey)-1]
	}
	if len(uncompactedBranchKey) > 128 {
		return fmt.Errorf("branch key too long: %d", len(branchKey))
	}
	depth := int16(len(uncompactedBranchKey))
	for _, c := range row {
		if c == nil {
			continue
		}
		if c.accountAddrLen == 0 && c.storageAddrLen == 0 {
			continue
		}
		err := c.deriveHashedKeys(depth, keccak, length.Addr)
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
				//fmt.Printf("--- debug --- cell with accountAddrLen>0 and storageAddrLen>0: branchKey=%x, branchKeyLen=%d, uncompactedBranchKey=%x, uncompactedBranchKeyLen=%d, plainKeyNibbles=%x, branchKeyAndExtNibbles=%x, cell=%s\n", branchKey, len(branchKey), uncompactedBranchKey, len(uncompactedBranchKey), plainKeyNibbles, branchKeyAndExtNibbles, c)
				if !bytes.Equal(c.accountAddr[:], c.storageAddr[:length.Addr]) {
					return fmt.Errorf("accountAddr mismatch with storageAddr: %s != %x", common.BytesToAddress(c.accountAddr[:]), common.BytesToHash(c.storageAddr[:length.Addr]))
				}
			} else {
				//nolint:staticcheck
				//fmt.Printf("--- debug --- cell with accountAddrLen=0 and storageAddrLen>0: branchKey=%x, branchKeyLen=%d, uncompactedBranchKey=%x, uncompactedBranchKeyLen=%d, plainKeyNibbles=%x, branchKeyAndExtNibbles=%x, cell=%s\n", branchKey, len(branchKey), uncompactedBranchKey, len(uncompactedBranchKey), plainKeyNibbles, branchKeyAndExtNibbles, c)
			}
		}
		//if c.extLen > 0 {
		//	fmt.Printf("--- debug --- cell with plainKey and extLen>0: branchKey=%x, branchKeyLen=%d, uncompactedBranchKey=%x, uncompactedBranchKeyLen=%d, plainKeyNibbles=%x, branchKeyAndExtNibbles=%x, cell=%s\n", branchKey, len(branchKey), uncompactedBranchKey, len(uncompactedBranchKey), plainKeyNibbles, branchKeyAndExtNibbles, c)
		//}
		if !bytes.Equal(plainKeyNibbles, branchKeyAndExtNibbles) {
			//fmt.Printf("--- debug --- branchKey=%x, branchKeyLen=%d, uncompactedBranchKey=%x, uncompactedBranchKeyLen=%d, plainKeyNibbles=%x, branchKeyAndExtNibbles=%x, cell=%s\n", branchKey, len(branchKey), uncompactedBranchKey, len(uncompactedBranchKey), plainKeyNibbles, branchKeyAndExtNibbles, c)
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

// MergeHexBranches combines two branchData, number 2 coming after (and potentially shadowing) number 1
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
			// Add fields from branch2
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
			add := (touchMap2&bit == 0) && (afterMap2&bit != 0) // Add fields from branchData1
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
					fmt.Printf("b1: %x %v\n", branch1, branch1)
					fmt.Printf("b2: %x\n", branch2)
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
	case "bin":
		trieVariant = VariantBinPatriciaTrie
	case "hex-parallel":
		trieVariant = VariantConcurrentHexPatricia
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

// do not add stat of root node to other branch stat
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

	// if key is not "state" then we are interested in the branch data
	if !bytes.Equal(key, []byte("state")) {
		stat.IsRoot = false

		tm, am, cells, err := BranchData(branch).decodeCells()
		if err != nil {
			return nil
		}
		stat.TAMapsSize = uint64(2 + 2) // touchMap + afterMap
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
				switch tv {
				case VariantBinPatriciaTrie:
					stat.ExtSize += uint64(c.extLen)
				case VariantHexPatriciaTrie, VariantConcurrentHexPatricia:
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

// Defines how to evaluate commitments
type Mode uint

const (
	ModeDisabled Mode = 0
	ModeDirect   Mode = 1
	ModeUpdate   Mode = 2
)

func (m Mode) String() string {
	switch m {
	case ModeDisabled:
		return "disabled"
	case ModeDirect:
		return "direct"
	case ModeUpdate:
		return "update"
	default:
		return "unknown"
	}
}

type Updates struct {
	hasher keyHasher
	keys   map[string]struct{}       // plain keys to keep only unique keys in etl
	etl    *etl.Collector            // all-in-one collector
	tree   *btree.BTreeG[*KeyUpdate] // TODO since it's thread safe to read, maybe instead of all collectors we can use one tree
	mode   Mode
	tmpdir string

	sortPerNibble bool // if true, use nibbles collectors instead of etl (all-in-one)
	nibbles       [16]*etl.Collector
}

// Should be called right after updates initialisation. Otherwise could lost some data
func (t *Updates) SetConcurrentCommitment(b bool) {
	t.sortPerNibble = b
	t.initCollector()
}

// SetConcurrentCommitment returns true if updates are sorted per nibble
func (t *Updates) IsConcurrentCommitment() bool {
	return t.sortPerNibble
}

type keyHasher func(key []byte) []byte

func keyHasherNoop(key []byte) []byte { return key }

func NewUpdates(m Mode, tmpdir string, hasher keyHasher) *Updates {
	t := &Updates{
		hasher: hasher,
		tmpdir: tmpdir,
		mode:   m,
	}
	if t.mode == ModeDirect {
		t.keys = make(map[string]struct{})
		t.initCollector()
	} else if t.mode == ModeUpdate {
		t.tree = btree.NewG(64, keyUpdateLessFn)
	}
	return t
}

func (t *Updates) SetMode(m Mode) {
	t.mode = m
	if t.mode == ModeDirect && t.keys == nil {
		t.keys = make(map[string]struct{})
		t.initCollector()
	} else if t.mode == ModeUpdate && t.tree == nil {
		t.tree = btree.NewG(64, keyUpdateLessFn)
	}
	t.Reset()
}

func (t *Updates) initCollector() {
	if t.sortPerNibble {
		for i := 0; i < len(t.nibbles); i++ {
			if t.nibbles[i] != nil {
				t.nibbles[i].Close()
				t.nibbles[i] = nil
			}

			t.nibbles[i] = etl.NewCollectorWithAllocator("commitment.nibble."+strconv.Itoa(i), t.tmpdir, etl.SmallSortableBuffers, log.Root().New("update-tree")).LogLvl(log.LvlDebug)
			t.nibbles[i].SortAndFlushInBackground(true)
		}
		if t.etl != nil {
			t.etl.Close()
			t.etl = nil
		}
		return
	}

	if t.etl != nil {
		t.etl.Close()
		t.etl = nil
	}
	t.etl = etl.NewCollectorWithAllocator("commitment", t.tmpdir, etl.SmallSortableBuffers, log.Root().New("update-tree")).LogLvl(log.LvlDebug)
	t.etl.SortAndFlushInBackground(true)
}

func (t *Updates) Mode() Mode { return t.mode }

func (t *Updates) Size() (updates uint64) {
	switch t.mode {
	case ModeDirect:
		return uint64(len(t.keys))
	case ModeUpdate:
		return uint64(t.tree.Len())
	default:
		return 0
	}
}

// TouchPlainKey marks plainKey as updated and applies different fn for different key types
// (different behaviour for Code, Account and Storage key modifications).
func (t *Updates) TouchPlainKey(key string, val []byte, fn func(c *KeyUpdate, val []byte)) {
	switch t.mode {
	case ModeUpdate:
		pivot, updated := &KeyUpdate{plainKey: key, update: new(Update)}, false

		t.tree.DescendLessOrEqual(pivot, func(item *KeyUpdate) bool {
			if item.plainKey == pivot.plainKey {
				fn(item, val)
				updated = true
			}
			return false
		})
		if !updated {
			pivot.hashedKey = t.hasher(toBytesZeroCopy(pivot.plainKey))
			fn(pivot, val)
			t.tree.ReplaceOrInsert(pivot)
		}
	case ModeDirect:
		if _, ok := t.keys[key]; !ok {
			keyBytes := toBytesZeroCopy(key)
			hashedKey := t.hasher(keyBytes)

			var err error
			if !t.sortPerNibble {
				err = t.etl.Collect(hashedKey, keyBytes)
			} else {
				err = t.nibbles[hashedKey[0]].Collect(hashedKey, keyBytes)
			}
			if err != nil {
				log.Warn("failed to collect updated key", "key", key, "err", err)
			}
			t.keys[key] = struct{}{}
		}
	default:
	}
}

func (t *Updates) TouchAccount(c *KeyUpdate, val []byte) {
	if len(val) == 0 {
		c.update.Flags = DeleteUpdate
		return
	}
	if c.update.Flags&DeleteUpdate != 0 {
		c.update.Flags = 0 // also could invert with ^ but 0 is just a reset
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
	copy(c.update.CodeHash[:], crypto.Keccak256(code))
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
	}
	if t.sortPerNibble {
		for i := 0; i < len(t.nibbles); i++ {
			if t.nibbles[i] != nil {
				t.nibbles[i].Close()
			}
		}
	}
}

const hashSortBatchSize = 10_000

// HashSort sorts and applies fn to each key-value pair in the order of hashed keys.
// Keys are processed in batches of 10k to control memory usage.
// If warmuper is non-nil, keys are submitted for parallel warming before processing.
// Caller is responsible for calling warmuper.Wait() after processing completes.
func (t *Updates) HashSort(ctx context.Context, warmuper *Warmuper, fn func(hk, pk []byte, update *Update) error) error {
	switch t.mode {
	case ModeDirect:
		clear(t.keys)

		batch := make([]*KeyUpdate, 0, hashSortBatchSize)
		var prevKey []byte

		err := t.etl.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			if warmuper != nil && warmuper.Cache() != nil {
				warmuper.Cache().EvictPlainKey(v)
			}
			// Make copies since ETL may reuse buffers
			hk := common.Copy(k)
			pk := common.Copy(v)
			batch = append(batch, &KeyUpdate{hashedKey: hk, plainKey: string(pk)})

			// Submit to warmuper with start depth based on divergence from previous key
			if warmuper != nil {
				startDepth := 0
				if prevKey != nil {
					// Find common prefix length
					minLen := min(len(prevKey), len(hk))
					for startDepth < minLen && prevKey[startDepth] == hk[startDepth] {
						startDepth++
					}
				}
				warmuper.WarmKey(hk, startDepth)
				prevKey = hk
			}

			// Process batch when full
			if len(batch) >= hashSortBatchSize {
				for _, p := range batch {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
					if err := fn(p.hashedKey, toBytesZeroCopy(p.plainKey), nil); err != nil {
						return err
					}
				}
				if warmuper != nil {
					warmuper.DrainPending()
				}
				batch = batch[:0]
			}
			return nil
		}, etl.TransformArgs{Quit: ctx.Done()})
		if err != nil {
			return err
		}

		// Process remaining keys in final batch
		for _, p := range batch {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := fn(p.hashedKey, toBytesZeroCopy(p.plainKey), nil); err != nil {
				return err
			}
		}

		t.initCollector()

	case ModeUpdate:
		batch := make([]*KeyUpdate, 0, hashSortBatchSize)
		var prevKey []byte
		var processErr error

		t.tree.Ascend(func(item *KeyUpdate) bool {
			select {
			case <-ctx.Done():
				processErr = ctx.Err()
				return false
			default:
			}

			// Make copies
			hk := make([]byte, len(item.hashedKey))
			copy(hk, item.hashedKey)
			batch = append(batch, &KeyUpdate{hashedKey: hk, plainKey: item.plainKey, update: item.update})

			// Submit to warmuper with start depth based on divergence from previous key
			if warmuper != nil {
				startDepth := 0
				if prevKey != nil {
					// Find common prefix length
					minLen := min(len(prevKey), len(hk))
					for startDepth < minLen && prevKey[startDepth] == hk[startDepth] {
						startDepth++
					}
				}
				warmuper.WarmKey(hk, startDepth)
				prevKey = hk
			}

			// Process batch when full
			if len(batch) >= hashSortBatchSize {
				for _, p := range batch {
					select {
					case <-ctx.Done():
						processErr = ctx.Err()
						return false
					default:
					}
					if err := fn(p.hashedKey, toBytesZeroCopy(p.plainKey), p.update); err != nil {
						processErr = err
						return false
					}
				}
				if warmuper != nil {
					warmuper.DrainPending()
				}
				batch = batch[:0]
			}
			return true
		})

		if processErr != nil {
			return processErr
		}

		// Process remaining keys in final batch
		for _, p := range batch {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := fn(p.hashedKey, toBytesZeroCopy(p.plainKey), p.update); err != nil {
				return err
			}
		}
		t.tree.Clear(true)

	default:
		return nil
	}
	return nil
}

// Reset clears all updates
func (t *Updates) Reset() {
	switch t.mode {
	case ModeDirect:
		t.keys = nil
		t.keys = make(map[string]struct{})
		t.initCollector()
	case ModeUpdate:
		t.tree.Clear(true)
	default:
	}
}

type KeyUpdate struct {
	plainKey  string
	hashedKey []byte
	update    *Update
}

func keyUpdateLessFn(i, j *KeyUpdate) bool {
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

// Copy creates a deep copy of the Update.
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

func toStringZeroCopy(v []byte) string { return unsafe.String(&v[0], len(v)) } //nolint
func toBytesZeroCopy(s string) []byte  { return unsafe.Slice(unsafe.StringData(s), len(s)) }
