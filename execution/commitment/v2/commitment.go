// Copyright 2026 The Erigon Authors
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

package v2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"slices"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

var (
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

// BranchCache is populated by SharedDomains.Commit, not by this encoder.
type BranchEncoder struct {
	buf       *bytes.Buffer
	bitmapBuf [binary.MaxVarintLen64]byte
	merger    *commitment.BranchMerger
	metrics   *commitment.Metrics

	deferUpdates       bool
	callerOwnsDeferred bool
	maxDeferredUpdates int
	deferred           []*commitment.DeferredBranchUpdate
}

func NewBranchEncoder(sz uint64) *BranchEncoder {
	return &BranchEncoder{
		buf:    bytes.NewBuffer(make([]byte, sz)),
		merger: commitment.NewHexBranchMerger(sz / 2),
	}
}

func (be *BranchEncoder) setDeferUpdates(defer_ bool) {
	be.deferUpdates = defer_
	if defer_ && be.deferred == nil {
		be.deferred = make([]*commitment.DeferredBranchUpdate, 0, 64)
	}
}

func (be *BranchEncoder) DeferUpdatesEnabled() bool {
	return be.deferUpdates
}

func (be *BranchEncoder) ClearDeferred() {
	for _, upd := range be.deferred {
		commitment.PutDeferredUpdate(upd)
	}
	// Delete, not reslice: this encoder sits inside a pooled trie.
	be.deferred = slices.Delete(be.deferred, 0, len(be.deferred))
}

// putBranch is bound by the same no-retain rule as PatriciaContext.PutBranch.
func (be *BranchEncoder) ApplyDeferredUpdates(
	numWorkers int,
	putBranch func(prefix []byte, data []byte, prevData []byte) error,
) error {
	if _, err := commitment.ApplyDeferredBranchUpdates(be.deferred, numWorkers, putBranch, be.metrics); err != nil {
		return err
	}
	return nil
}

func (be *BranchEncoder) setMetrics(metrics *commitment.Metrics) {
	be.metrics = metrics
}

// prev is the branch read during unfold, empty when the branch is new.
// A nil prev is passed to PutBranch as a non-nil empty slice to avoid a storage read.
func (be *BranchEncoder) CollectUpdate(
	ctx commitment.PatriciaContext,
	prefix []byte,
	bitmap, touchMap, afterMap uint16,
	cells *[16]cellEncodeData,
	prev []byte,
) error {
	if be.deferUpdates {
		return be.CollectDeferredUpdate(ctx, prefix, bitmap, touchMap, afterMap, cells, prev)
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
	commitment.PublishBranchWrites(1, len(updateCopy), be.metrics)
	return nil
}

// See CollectUpdate for the meaning of prev.
func (be *BranchEncoder) CollectDeferredUpdate(
	ctx commitment.PatriciaContext,
	prefix []byte,
	bitmap, touchMap, afterMap uint16,
	cells *[16]cellEncodeData,
	prev []byte,
) error {
	limit := be.maxDeferredUpdates
	if limit == 0 {
		limit = DefaultMaxDeferredUpdates
	}
	if !be.callerOwnsDeferred && len(be.deferred) >= limit {
		if err := be.ApplyDeferredUpdates(16, ctx.PutBranch); err != nil {
			return err
		}
		be.ClearDeferred()
	}
	if prev == nil {
		prev = []byte{}
	}

	raw, err := be.EncodeBranch(bitmap, touchMap, afterMap, cells)
	if err != nil {
		return err
	}
	be.deferred = append(be.deferred, commitment.GetDeferredUpdate(prefix, raw, prev))
	return nil
}

// CollectTombstone writes an empty-branch tombstone at prefix (touchMap present,
// afterMap zero, no cells) without merging it against prev. The tombstone carries
// no cells, and a real stored branch can be shorter than its bitmap implies (state
// hashes, extensions, inlined leaves), so Merge would overrun it. prev is still
// forwarded to PutBranch so the domain diff records the real previous branch and an
// unwind across the destruct restores it rather than an empty tombstone.
func (be *BranchEncoder) CollectTombstone(ctx commitment.PatriciaContext, prefix []byte, touchMap uint16, prev []byte) error {
	if be.deferUpdates {
		if err := be.CollectDeferredUpdate(ctx, prefix, 0, touchMap, 0, nil, prev); err != nil {
			return err
		}
		be.deferred[len(be.deferred)-1].RawWrite = true
		return nil
	}
	raw, err := be.EncodeBranch(0, touchMap, 0, nil)
	if err != nil {
		return err
	}
	prefixCopy := bytes.Clone(prefix)
	updateCopy := bytes.Clone(raw)
	if err := ctx.PutBranch(prefixCopy, updateCopy, prev); err != nil {
		return err
	}
	commitment.PublishBranchWrites(1, len(updateCopy), be.metrics)
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

func (be *BranchEncoder) EncodeBranch(bitmap, touchMap, afterMap uint16, cells *[16]cellEncodeData) (commitment.BranchData, error) {
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
			var fields commitment.CellFields
			if cell.extLen > 0 && cell.storageAddrLen == 0 {
				fields |= commitment.FieldExtension
			}
			if cell.accountAddrLen > 0 {
				fields |= commitment.FieldAccountAddr
			}
			if cell.storageAddrLen > 0 {
				fields |= commitment.FieldStorageAddr
			}
			if cell.hashLen > 0 {
				fields |= commitment.FieldHash
			}
			if cell.stateHashLen == 32 && (cell.accountAddrLen > 0 || cell.storageAddrLen > 0) {
				fields |= commitment.FieldStateHash
			}
			if err := be.buf.WriteByte(byte(fields)); err != nil {
				return nil, err
			}
			if fields&commitment.FieldExtension != 0 {
				if err := be.putUvarAndVal(uint64(cell.extLen), cell.extension[:cell.extLen]); err != nil {
					return nil, err
				}
			}
			if fields&commitment.FieldAccountAddr != 0 {
				if err := be.putUvarAndVal(uint64(cell.accountAddrLen), cell.accountAddr[:cell.accountAddrLen]); err != nil {
					return nil, err
				}
			}
			if fields&commitment.FieldStorageAddr != 0 {
				if err := be.putUvarAndVal(uint64(cell.storageAddrLen), cell.storageAddr[:cell.storageAddrLen]); err != nil {
					return nil, err
				}
			}
			if fields&commitment.FieldHash != 0 {
				if err := be.putUvarAndVal(uint64(cell.hashLen), cell.hash[:cell.hashLen]); err != nil {
					return nil, err
				}
			}
			if fields&commitment.FieldStateHash != 0 {
				if err := be.putUvarAndVal(uint64(cell.stateHashLen), cell.stateHash[:cell.stateHashLen]); err != nil {
					return nil, err
				}
			}
		}
		bitset ^= bit
	}
	return be.buf.Bytes(), nil
}

func BranchDataString(branchData commitment.BranchData) string {
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
			fields := commitment.CellFields(branchData[pos])
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

func decodeBranchCells(branchData commitment.BranchData) (touchMap, afterMap uint16, row [16]*cell, err error) {
	touchMap = binary.BigEndian.Uint16(branchData[0:])
	afterMap = binary.BigEndian.Uint16(branchData[2:])
	pos := 4
	for bitset, j := touchMap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		if afterMap&bit != 0 {
			fields := commitment.CellFields(branchData[pos])
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

func ValidateBranchData(branchData commitment.BranchData, branchKey []byte) error {
	if branchData.IsTombstone() {
		return nil
	}
	_, afterMap, row, err := decodeBranchCells(branchData)
	if err != nil {
		return err
	}
	if err := validateAfterMap(afterMap, row); err != nil {
		return err
	}
	if err := validatePlainKeys(branchKey, row); err != nil {
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

func validatePlainKeys(branchKey []byte, row [16]*cell) error {
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
		err := c.deriveHashedKeys(depth, length.Addr, hashBuf[:])
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
			plainKeyNibbles = commitment.KeyToHexNibbleHash(c.accountAddr[:])
		}
		if c.storageAddrLen > 0 {
			plainKeyNibbles = commitment.KeyToHexNibbleHash(c.storageAddr[:])
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

func DecodeBranchAndCollectStat(key, branch []byte, tv commitment.TrieVariant) *commitment.BranchStat {
	stat := &commitment.BranchStat{}
	if len(key) == 0 {
		return nil
	}

	stat.KeySize = uint64(len(key))
	stat.ValSize = uint64(len(branch))
	stat.IsRoot = true

	if !bytes.Equal(key, []byte("state")) {
		stat.IsRoot = false

		tm, am, cells, err := decodeBranchCells(commitment.BranchData(branch))
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
				if tv == commitment.VariantHexPatriciaTrie {
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
