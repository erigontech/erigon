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

// PBinPatriciaHashed — commitment over EIP-8297's partitioned binary tree.
//
// The EIP leaves its hash function open and names Keccak-256 as a candidate;
// this engine uses Keccak-256 both for node hashing and for tree-key
// derivation, behind pbinHasher so the suite can be swapped.
//
// M0 scope: in-memory Process over the account and storage zones, ModeDirect
// only. Code chunking and parallel mounting are out — BASIC_DATA carries
// code_size 0. EIP-8297 has no removal: a zeroed storage slot keeps its leaf at
// 32 zero bytes, while an account removal is refused rather than guessed at.

package commitment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// PBinPatriciaHashed computes commitment over EIP-8297's partitioned binary
// tree. It borrows the hex engine's grid, unfold and fold shape and none of its
// node model: arity is 2, there is no extension node and no storage root, and a
// leaf commits its complete tree key.
type PBinPatriciaHashed struct {
	grid          pbinGrid
	currentKey    pbinBitpath // path from the root to the deepest active row, one bit per level
	ctx           PatriciaContext
	hasher        pbinHasher
	branchEncoder pbinBranchEncoder
	counters      pbinCounters

	siblingKey [pbinAccountKeyLength]byte // scratch for the CODE_HASH key of the account being visited

	traceW io.Writer // nil = disabled

	rootChecked bool // whether the root record is known to be absent
	rootTouched bool
	rootPresent bool
	rootPrev    []byte // root record as last read or written; nil = never read
}

// pbinCounters measures what keeping a single hash per branch cell costs. A
// probe diverging inside a stored prefix invalidates that hash, and rebuilding
// it needs a branch read the descent itself would not have made. Storing both
// child hashes per cell instead is a wire-format change, so it waits on these
// numbers.
type pbinCounters struct {
	splitsInsidePrefix uint64
	materializeReads   uint64
}

// pbinPool recycles engines: the grid is the better part of a megabyte, and
// Release leaves a pooled engine in the state a fresh one starts in.
var pbinPool sync.Pool

func NewPBinPatriciaHashed(ctx PatriciaContext) *PBinPatriciaHashed {
	pph, ok := pbinPool.Get().(*PBinPatriciaHashed)
	if !ok {
		pph = &PBinPatriciaHashed{}
	}
	pph.ctx = ctx
	return pph
}

func (pph *PBinPatriciaHashed) Variant() TrieVariant { return VariantBinPatriciaTrie }

func (pph *PBinPatriciaHashed) ResetContext(ctx PatriciaContext) { pph.ctx = ctx }

// SetTraceWriter enables tracing. M0 traces one line per run: the counters the
// split-rehash decision is waiting on.
func (pph *PBinPatriciaHashed) SetTraceWriter(w io.Writer) { pph.traceW = w }

// EnableCsvMetrics is a no-op: the binary engine collects no metrics in M0.
func (pph *PBinPatriciaHashed) EnableCsvMetrics(string) {}

// Reset drops the in-memory tree, keeping the context. The next run rebuilds
// what it descends into from stored records, starting at the root cell record.
func (pph *PBinPatriciaHashed) Reset() {
	pph.grid.resetForReuse()
	pph.currentKey = pbinBitpath{}
	pph.rootChecked, pph.rootTouched, pph.rootPresent = false, false, false
	pph.rootPrev = nil
}

// setHashSuite swaps H on both seams at once — node hashing on this engine and
// the returned key-derivation hasher — so neither can be configured without the
// other. Production never calls it: the nil default is Keccak-256 on both.
func (pph *PBinPatriciaHashed) setHashSuite(sum pbinHashFn) keyHasher {
	pph.hasher.sum = sum
	return pbinKeyHasherWith(sum)
}

// Release returns the engine to the pool. The caller must not use it afterwards.
func (pph *PBinPatriciaHashed) Release() {
	pph.Reset()
	pph.ctx = nil
	pph.traceW = nil
	pph.hasher.sum = nil
	pph.counters = pbinCounters{}
	pph.branchEncoder.buf = pph.branchEncoder.buf[:0]
	pbinPool.Put(pph)
}

var (
	errPBinMissingBranch     = errors.New("pbin: branch record missing")
	errPBinDeleteUnsupported = errors.New("pbin: EIP-8297 defines no deletion")
)

// ErrPBinUnsupported marks a code path only the hex trie implements. Callers
// wrap it with the path name so the bin variant refuses instead of no-opping.
var ErrPBinUnsupported = errors.New("pbin: unsupported under the bin commitment variant")

// pbinRootKey names the record holding the root cell — the one node no descent
// can name: every other node is found by the path that reaches it, while the
// root's own prefix is stored nowhere else. The sentinel cannot collide with a
// node record: every pbinAppendBitPath key ends in a trailing bit-count byte
// ≤ 7. The empty key would not do — domain iteration reads a zero-length key as
// end-of-stream, and the empty key sorts first, truncating the whole table.
var pbinRootKey = []byte{0x08}

// Process folds the update stream into the tree and returns the new root.
// HashSort hands keys over in tree-key order, which is descent order, so the
// grid only ever walks the path between two consecutive keys.
//
// M0 ignores warmup: the engine runs against an in-memory context, so there is
// no page cache to pre-warm.
func (pph *PBinPatriciaHashed) Process(ctx context.Context, updates *Updates, logPrefix string, onProgress func(*CommitProgress), warmup WarmupConfig) ([]byte, error) {
	var processed uint64
	err := updates.HashSort(ctx, nil, func(treeKey, plainKey []byte, stateUpdate *Update) error {
		if err := pph.processKey(treeKey, plainKey, stateUpdate); err != nil {
			return err
		}
		processed++
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("pbin: process %s: %w", logPrefix, err)
	}
	for pph.grid.activeRows > 0 {
		if err = pph.fold(); err != nil {
			return nil, fmt.Errorf("pbin: final fold: %w", err)
		}
	}
	if err = pph.storeRoot(); err != nil {
		return nil, err
	}
	if onProgress != nil {
		onProgress(&CommitProgress{KeyIndex: processed, UpdateCount: processed})
	}
	if pph.traceW != nil {
		fmt.Fprintf(pph.traceW, "pbin: keys=%d splitsInsidePrefix=%d materializeReads=%d\n",
			processed, pph.counters.splitsInsidePrefix, pph.counters.materializeReads)
	}
	return pph.RootHash()
}

// processKey routes one update into the tree. An account fans out to two leaves
// visited back to back — BASIC_DATA and the CODE_HASH sibling at the next
// sub-index — which is what lets the shared keyHasher stay a one-key function.
func (pph *PBinPatriciaHashed) processKey(treeKey, plainKey []byte, stateUpdate *Update) error {
	if stateUpdate != nil && stateUpdate.Deleted() {
		return fmt.Errorf("%w: update for %x", errPBinDeleteUnsupported, plainKey)
	}
	update := stateUpdate
	if update == nil {
		var err error
		if update, err = pph.stateOf(plainKey); err != nil {
			return err
		}
	}
	if err := pph.followAndUpdate(treeKey, plainKey, update); err != nil {
		return err
	}
	if len(plainKey) != length.Addr {
		return nil
	}
	codeKey, err := pph.codeHashKey(treeKey)
	if err != nil {
		return err
	}
	return pph.followAndUpdate(codeKey, plainKey, update)
}

func (pph *PBinPatriciaHashed) stateOf(plainKey []byte) (*Update, error) {
	if len(plainKey) == length.Addr {
		update, err := pph.ctx.Account(plainKey)
		if err != nil {
			return nil, fmt.Errorf("pbin: read account %x: %w", plainKey, err)
		}
		return update, nil
	}
	update, err := pph.ctx.Storage(plainKey)
	if err != nil {
		return nil, fmt.Errorf("pbin: read storage %x: %w", plainKey, err)
	}
	return update, nil
}

// codeHashKey is the CODE_HASH leaf beside a BASIC_DATA key: same stem, next
// sub-index (eip:311-320). The two sit adjacent in key order, so visiting them
// together never walks the descent backwards.
func (pph *PBinPatriciaHashed) codeHashKey(basicDataKey []byte) ([]byte, error) {
	if len(basicDataKey) != pbinAccountKeyLength || basicDataKey[pbinAccountKeyLength-1] != pbinBasicDataLeafKey {
		return nil, fmt.Errorf("pbin: %x is not a BASIC_DATA key", basicDataKey)
	}
	copy(pph.siblingKey[:], basicDataKey)
	pph.siblingKey[pbinAccountKeyLength-1] = pbinCodeHashLeafKey
	return pph.siblingKey[:], nil
}

// followAndUpdate moves the grid onto treeKey and writes the update into the
// cell that lands there.
func (pph *PBinPatriciaHashed) followAndUpdate(treeKey, plainKey []byte, update *Update) error {
	probe := pbinPathFromBytes(treeKey)
	for !probe.hasPrefix(&pph.currentKey) {
		if err := pph.fold(); err != nil {
			return err
		}
	}
	for u := pph.needUnfolding(&probe); u.action != pbinUnfoldNone; u = pph.needUnfolding(&probe) {
		if err := pph.unfold(&probe, u); err != nil {
			return err
		}
	}
	return pph.updateCell(plainKey, &probe, update)
}

// updateCell writes one leaf into the deepest open row. Unfolding has already
// made the target either empty — a new leaf, whose prefix is the rest of the
// key — or the same leaf touched again.
func (pph *PBinPatriciaHashed) updateCell(plainKey []byte, probe *pbinBitpath, update *Update) error {
	g := &pph.grid
	var c *pbinCell
	var depth int16
	var row int
	var bit uint64
	if g.activeRows == 0 {
		c = &g.root
	} else {
		row = g.activeRows - 1
		depth = g.depths[row]
		if probe.bitLen < depth {
			return fmt.Errorf("pbin: a %d-bit key cannot be updated in a row at depth %d", probe.bitLen, depth)
		}
		bit = probe.bit(depth - 1)
		c = &g.rows[row][bit]
	}

	// A key with no state reads back as a delete. Landing on an empty slot means
	// there simply is no leaf here; landing on one means the leaf keeps its place
	// at a zero value, or the removal is one EIP-8297 does not define.
	if update.Deleted() {
		if c.kind != pbinNodeLeaf {
			return nil
		}
		zeroed, err := pbinZeroedLeafUpdate(plainKey)
		if err != nil {
			return err
		}
		update = &zeroed
	}

	if g.activeRows == 0 {
		pph.rootTouched, pph.rootPresent = true, true
	} else {
		g.touchMap[row] |= uint16(1) << bit
		g.afterMap[row] |= uint16(1) << bit
	}

	switch c.kind {
	case pbinNodeEmpty:
		c.kind = pbinNodeLeaf
		c.prefix = probe.slice(depth, probe.bitLen)
	case pbinNodeLeaf:
	default:
		return fmt.Errorf("pbin: update for a %d-bit key lands on a branch cell", probe.bitLen)
	}

	switch len(plainKey) {
	case length.Addr:
		c.accountAddrLen = int16(len(plainKey))
		copy(c.accountAddr[:], plainKey)
		c.loaded = c.loaded.addFlag(cellLoadAccount)
	case length.Addr + length.Hash:
		c.storageAddrLen = int16(len(plainKey))
		copy(c.storageAddr[:], plainKey)
		c.loaded = c.loaded.addFlag(cellLoadStorage)
	default:
		return fmt.Errorf("pbin: plain key of %d bytes is neither an account nor a storage key", len(plainKey))
	}
	c.setFromUpdate(update)
	return nil
}

// pbinZeroedLeafUpdate reinterprets an absent read over a live leaf. The domain
// encodes zero and absent the same way, while EIP-8297 has no removal and holds
// a zero value as a present leaf, so a zeroed storage slot keeps its leaf at 32
// zero bytes. An absent account is a removal the EIP does not describe — its
// encoding is unverified against the reference and would silently change the
// root — so it stays refused.
func pbinZeroedLeafUpdate(plainKey []byte) (Update, error) {
	if len(plainKey) != length.Addr+length.Hash {
		return Update{}, fmt.Errorf("%w: %x", errPBinDeleteUnsupported, plainKey)
	}
	return Update{Flags: StorageUpdate}, nil
}

// RootHash hashes whatever the root cell holds. A one-key tree's root is the
// leaf itself (eip:133-135) and an empty tree is 32 zero bytes (eip:208), both
// of which fall out of hashing the cell rather than special-casing the shape.
func (pph *PBinPatriciaHashed) RootHash() ([]byte, error) {
	if pph.grid.activeRows != 0 {
		return nil, fmt.Errorf("pbin: root hash requested with %d rows still open", pph.grid.activeRows)
	}
	// A run that touches no key never descends, so nothing has pulled the stored
	// root in yet and an untouched grid would report the empty tree.
	if !pph.rootChecked {
		if err := pph.loadRoot(); err != nil {
			return nil, err
		}
	}
	var path pbinBitpath
	hash, err := pph.cellHash(&pph.grid.root, &path)
	if err != nil {
		return nil, err
	}
	return hash[:], nil
}

// storeRoot persists the root cell so a later engine can find the tree. Without
// it a root sitting under a non-empty prefix — every tree confined to one zone —
// is unreachable, and a run that finds nothing rebuilds from the touched keys
// alone.
func (pph *PBinPatriciaHashed) storeRoot() error {
	if !pph.rootTouched {
		return nil
	}
	// An emptied tree deletes the record: zero-length is the deletion encoding,
	// and the domain refuses a nil value outright.
	record := []byte{}
	if pph.grid.root.kind != pbinNodeEmpty {
		var err error
		if record, err = pbinAppendCell(nil, &pph.grid.root); err != nil {
			return err
		}
	}
	if err := pph.ctx.PutBranch(pbinRootKey, record, pph.rootPrev); err != nil {
		return fmt.Errorf("pbin: write root cell: %w", err)
	}
	pph.rootPrev = record
	return nil
}

// loadRoot reads the stored root cell into the grid. An absent record means the
// tree is empty; anything below the root is reached from the root's own prefix.
func (pph *PBinPatriciaHashed) loadRoot() error {
	pph.rootChecked = true
	data, _, err := pph.ctx.Branch(pbinRootKey)
	if err != nil {
		return fmt.Errorf("pbin: read root cell: %w", err)
	}
	if len(data) == 0 {
		pph.rootPrev = []byte{}
		return nil
	}
	pph.rootPrev = data
	pph.grid.root.reset()
	pos, err := pbinDecodeCell(data, 0, &pph.grid.root)
	if err != nil {
		return fmt.Errorf("pbin: decode root cell: %w", err)
	}
	if pos != len(data) {
		return fmt.Errorf("%w: %d trailing bytes after the root cell", errPBinMalformedBranch, len(data)-pos)
	}
	// Present but untouched: unfold reads these to decide whether the row it opens
	// survives, and a loaded root that reads absent takes the tree with it.
	pph.rootPresent = true
	return nil
}

// pbinUnfoldAction is what needUnfolding tells unfold to do about one cell.
type pbinUnfoldAction uint8

const (
	// pbinUnfoldNone means the probe key's slot is already in the grid.
	pbinUnfoldNone pbinUnfoldAction = iota
	// pbinUnfoldRoot means the grid has no root cell yet: read the root record.
	pbinUnfoldRoot
	// pbinUnfoldRecord means the cell points straight at a stored node: read it.
	pbinUnfoldRecord
	// pbinUnfoldDescend means the probe key agrees with the cell's whole prefix,
	// so the descent runs through it and nothing below moves.
	pbinUnfoldDescend
	// pbinUnfoldSplit means the probe key leaves the cell's prefix partway, so the
	// node below drops a level and its prefix shrinks.
	pbinUnfoldSplit
)

// pbinUnfolding is needUnfolding's answer. Descend and Split are separate
// answers rather than one bit count because only Split shortens a stored node's
// prefix, and the prefix is inside that node's hash.
type pbinUnfolding struct {
	action pbinUnfoldAction
	// matched counts the cell prefix bits the probe key agrees with: the whole
	// prefix for Descend, short of it for Split.
	matched int16
}

// needUnfolding reports what the grid still needs before probe's slot is in it.
// Unlike the hex engine there is no terminator to discount and no account
// boundary to clamp to — one key space, one bit per level.
func (pph *PBinPatriciaHashed) needUnfolding(probe *pbinBitpath) pbinUnfolding {
	var cell *pbinCell
	var depth int16

	if pph.grid.activeRows == 0 {
		if pph.grid.root.kind == pbinNodeEmpty {
			if pph.rootChecked {
				return pbinUnfolding{}
			}
			return pbinUnfolding{action: pbinUnfoldRoot}
		}
		cell = &pph.grid.root
	} else {
		row := pph.grid.activeRows - 1
		depth = pph.grid.depths[row]
		if probe.bitLen <= depth {
			return pbinUnfolding{}
		}
		cell = &pph.grid.rows[row][probe.bit(depth-1)]
	}

	if cell.kind == pbinNodeEmpty {
		return pbinUnfolding{}
	}
	if cell.prefix.bitLen == 0 {
		if cell.kind == pbinNodeBranch {
			return pbinUnfolding{action: pbinUnfoldRecord}
		}
		return pbinUnfolding{}
	}

	matched := pbinCommonPrefixBitsAt(probe, depth, &cell.prefix)
	if matched < cell.prefix.bitLen {
		return pbinUnfolding{action: pbinUnfoldSplit, matched: matched}
	}
	if cell.kind == pbinNodeLeaf {
		return pbinUnfolding{} // keys are prefix-free, so probe is this leaf's key
	}
	return pbinUnfolding{action: pbinUnfoldDescend, matched: matched}
}

// unfold opens one more level of the grid along probe, per the plan needUnfolding
// produced.
func (pph *PBinPatriciaHashed) unfold(probe *pbinBitpath, u pbinUnfolding) error {
	if u.action == pbinUnfoldNone {
		return nil
	}
	if u.action == pbinUnfoldRoot {
		return pph.loadRoot()
	}
	g := &pph.grid

	var upCell *pbinCell
	var touched, present bool
	var upDepth int16

	if g.activeRows == 0 {
		upCell = &g.root
		touched, present = pph.rootTouched, pph.rootPresent
	} else {
		upRow := g.activeRows - 1
		upDepth = g.depths[upRow]
		upBit := probe.bit(upDepth - 1)
		upCell = &g.rows[upRow][upBit]
		touched = g.touchMap[upRow]&(uint16(1)<<upBit) != 0
		present = g.afterMap[upRow]&(uint16(1)<<upBit) != 0
		pph.currentKey.appendBit(upBit)
	}

	row := g.activeRows
	g.rows[row][0].reset()
	g.rows[row][1].reset()
	g.touchMap[row], g.afterMap[row], g.branchBefore[row] = 0, 0, false
	g.prevRecord[row] = nil

	if u.action == pbinUnfoldRecord {
		return pph.unfoldBranchNode(row, upDepth+1, touched && !present)
	}

	consumed, err := pbinUnfoldConsumed(u, &upCell.prefix)
	if err != nil {
		return err
	}
	if u.action == pbinUnfoldSplit {
		pph.counters.splitsInsidePrefix++
	}
	bit := upCell.prefix.bit(consumed - 1)
	if touched {
		g.touchMap[row] = uint16(1) << bit
	}
	if present {
		g.afterMap[row] = uint16(1) << bit
	}
	g.rows[row][bit].fillFromUpperCell(upCell, consumed)
	pph.rehashAfterPrefixChange(&g.rows[row][bit])

	if consumed > 1 {
		head := upCell.prefix.slice(0, consumed-1)
		pph.currentKey.append(&head)
	}
	g.depths[row] = upDepth + consumed
	g.activeRows++
	return nil
}

// pbinUnfoldConsumed is how many of the cell's prefix bits this unfold takes:
// all of them when the probe key matched, and one past the divergence when it
// did not — that extra bit is what the new row branches on.
func pbinUnfoldConsumed(u pbinUnfolding, prefix *pbinBitpath) (int16, error) {
	switch u.action {
	case pbinUnfoldDescend:
		return prefix.bitLen, nil
	case pbinUnfoldSplit:
		if u.matched >= prefix.bitLen {
			return 0, fmt.Errorf("pbin: %d matched bits of a %d-bit prefix is not a split", u.matched, prefix.bitLen)
		}
		return u.matched + 1, nil
	default:
		return 0, fmt.Errorf("pbin: unfold action %d consumes no prefix bits", u.action)
	}
}

// unfoldBranchNode loads the record at the current descent key into a row. The
// key is reconstructed from the parent cell's stored prefix, which is the only
// place the bits between the two nodes exist.
func (pph *PBinPatriciaHashed) unfoldBranchNode(row int, depth int16, deleted bool) error {
	g := &pph.grid
	key := pbinEncodeBitPath(&pph.currentKey)

	data, _, err := pph.ctx.Branch(key)
	if err != nil {
		return fmt.Errorf("pbin: read branch at %x: %w", key, err)
	}
	if len(data) == 0 {
		return fmt.Errorf("%w at %x (%d bits)", errPBinMissingBranch, key, pph.currentKey.bitLen)
	}

	_, afterMap, err := pbinDecodeBranch(data, &g.rows[row])
	if err != nil {
		return fmt.Errorf("pbin: decode branch at %x: %w", key, err)
	}
	g.prevRecord[row] = data
	// The record's own touch map is write-time bookkeeping; nothing in this run
	// has touched the row yet. A parent cell that is touched but gone takes the
	// whole subtree with it.
	if deleted {
		g.touchMap[row], g.afterMap[row] = afterMap, 0
	} else {
		g.touchMap[row], g.afterMap[row] = 0, afterMap
	}
	g.branchBefore[row] = true
	g.depths[row] = depth
	g.activeRows++
	return nil
}

// fillFromUpperCell moves a cell one level down, dropping the prefix bits the
// descent has taken over. skip counts those bits and includes the one the new
// row branches on. It re-cuts the prefix, so the caller owes the cell a
// rehashAfterPrefixChange.
func (c *pbinCell) fillFromUpperCell(up *pbinCell, skip int16) {
	c.reset()
	if skip < up.prefix.bitLen {
		c.prefix = up.prefix.slice(skip, up.prefix.bitLen)
	}
	c.kind = up.kind
	c.accountAddrLen = up.accountAddrLen
	if up.accountAddrLen > 0 {
		c.accountAddr = up.accountAddr
	}
	c.storageAddrLen = up.storageAddrLen
	if up.storageAddrLen > 0 {
		c.storageAddr = up.storageAddr
	}
	c.hashLen = up.hashLen
	if up.hashLen > 0 {
		c.hash = up.hash
	}
	c.children, c.childrenSet = up.children, up.childrenSet
	c.loaded = up.loaded
	c.Update = up.Update
}

// fillFromLowerCell moves the sole survivor of a collapsed row into the cell
// above, prepending the bits the row consumed: the ones the parent already
// descended plus the one the row branched on.
func (c *pbinCell) fillFromLowerCell(low *pbinCell, head *pbinBitpath, bit uint64) {
	prefix := *head
	prefix.appendBit(bit)
	prefix.append(&low.prefix)
	*c = *low
	c.prefix = prefix
}

// rehashAfterPrefixChange restores the invariant that a set hashLen means the
// hash covers the prefix the cell holds now. A cell that knows its children
// re-derives; one that does not is marked stale for materializeBranch.
func (pph *PBinPatriciaHashed) rehashAfterPrefixChange(c *pbinCell) {
	if c.kind != pbinNodeBranch {
		return
	}
	if c.childrenSet {
		c.hash = pph.hasher.branchHash(&c.prefix, &c.children[0], &c.children[1])
		c.hashLen = length.Hash
		return
	}
	c.hash, c.hashLen = common.Hash{}, 0
}

// fold reduces currentKey by one row: it hashes what the row holds into the cell
// above and, when the row stays a branch, writes the row's record.
func (pph *PBinPatriciaHashed) fold() error {
	g := &pph.grid
	if g.activeRows == 0 {
		return errors.New("pbin: cannot fold with no active rows")
	}
	row := g.activeRows - 1
	if err := pbinCheckCellMaps(g.touchMap[row], g.afterMap[row]); err != nil {
		return err
	}
	depth := g.depths[row]
	if pph.currentKey.bitLen != depth-1 {
		return fmt.Errorf("pbin: row %d at depth %d folds under a %d-bit key", row, depth, pph.currentKey.bitLen)
	}

	var upCell *pbinCell
	var bit uint64
	var upDepth int16
	if row == 0 {
		upCell = &g.root
	} else {
		upDepth = g.depths[row-1]
		bit = pph.currentKey.bit(upDepth - 1)
		upCell = &g.rows[row-1][bit]
	}

	var err error
	switch kind, _ := afterMapUpdateKind(g.afterMap[row]); kind {
	case updateKindDelete:
		err = pph.foldDelete(row, bit, upCell)
	case updateKindPropagate:
		err = pph.foldPropagate(row, bit, upDepth, depth, upCell)
	case updateKindBranch:
		err = pph.foldBranch(row, bit, upDepth, depth, upCell)
	}
	if err != nil {
		return err
	}
	g.activeRows--
	pph.currentKey.truncate(max(upDepth-1, 0))
	return nil
}

// foldBranch hashes a row that keeps both cells and stores it as one record,
// keyed by the bit path down to the branch bit.
func (pph *PBinPatriciaHashed) foldBranch(row int, bit uint64, upDepth, depth int16, upCell *pbinCell) error {
	g := &pph.grid
	if n := bits.OnesCount16(g.afterMap[row]); n != 2 {
		return fmt.Errorf("pbin: branch fold at row %d keeps %d cells, want 2", row, n)
	}
	pph.propagateTouch(row, bit)

	childPath := pph.currentKey
	childPath.appendBit(0)
	left, err := pph.hashRowCell(&g.rows[row][0], &childPath)
	if err != nil {
		return err
	}
	childPath.setBitAt(depth-1, 1)
	right, err := pph.hashRowCell(&g.rows[row][1], &childPath)
	if err != nil {
		return err
	}

	key := pbinEncodeBitPath(&pph.currentKey)
	record, err := pph.branchEncoder.encode(g.touchMap[row], g.afterMap[row], &g.rows[row])
	if err != nil {
		return err
	}
	if err = pph.ctx.PutBranch(key, bytes.Clone(record), g.prevRecordFor(row)); err != nil {
		return fmt.Errorf("pbin: write branch at %x: %w", key, err)
	}

	prefix := pph.currentKey.slice(upDepth, depth-1)
	upCell.reset()
	upCell.kind = pbinNodeBranch
	upCell.prefix = prefix
	upCell.children, upCell.childrenSet = [2]common.Hash{left, right}, true
	upCell.hash = pph.hasher.branchHash(&prefix, &left, &right)
	upCell.hashLen = length.Hash
	return nil
}

// foldPropagate collapses a row down to its sole survivor. The node moves up
// rather than being rewritten, so no record is written and the bits the row
// consumed are prepended to the survivor's own prefix.
func (pph *PBinPatriciaHashed) foldPropagate(row int, bit uint64, upDepth, depth int16, upCell *pbinCell) error {
	g := &pph.grid
	pph.propagateTouch(row, bit)

	childBit := bits.TrailingZeros16(g.afterMap[row])
	child := &g.rows[row][childBit]

	head := pph.currentKey.slice(upDepth, depth-1)
	upCell.fillFromLowerCell(child, &head, uint64(childBit))
	// The row's own bit is part of what moves up: dropping it still hashes, and
	// still gives the wrong root.
	if want := depth - upDepth + child.prefix.bitLen; upCell.prefix.bitLen != want {
		return fmt.Errorf("pbin: propagate at row %d formed a %d-bit prefix, want %d", row, upCell.prefix.bitLen, want)
	}
	pph.rehashAfterPrefixChange(upCell)
	return nil
}

// foldDelete drops a row that kept nothing, taking the record it came from with
// it.
func (pph *PBinPatriciaHashed) foldDelete(row int, bit uint64, upCell *pbinCell) error {
	g := &pph.grid
	if g.touchMap[row] != 0 {
		if row == 0 {
			pph.rootTouched, pph.rootPresent = true, false
		} else {
			g.touchMap[row-1] |= uint16(1) << bit
			g.afterMap[row-1] &^= uint16(1) << bit
		}
	}
	upCell.reset()
	if !g.branchBefore[row] {
		return nil
	}
	key := pbinEncodeBitPath(&pph.currentKey)
	if err := pph.ctx.PutBranch(key, []byte{}, g.prevRecordFor(row)); err != nil {
		return fmt.Errorf("pbin: delete branch at %x: %w", key, err)
	}
	return nil
}

// propagateTouch carries a modification to the row above. A fold that leaves a
// node behind also marks the root present: without it the next unfold reads
// touched and absent, and drops the whole subtree.
func (pph *PBinPatriciaHashed) propagateTouch(row int, bit uint64) {
	if pph.grid.touchMap[row] == 0 {
		return
	}
	if row == 0 {
		pph.rootTouched, pph.rootPresent = true, true
		return
	}
	pph.grid.touchMap[row-1] |= uint16(1) << bit
}

// hashRowCell hashes one cell of a folding row and writes the result back, so
// the record the row produces carries every child hash a later read needs.
func (pph *PBinPatriciaHashed) hashRowCell(c *pbinCell, path *pbinBitpath) (common.Hash, error) {
	h, err := pph.cellHash(c, path)
	if err != nil {
		return common.Hash{}, err
	}
	if c.kind == pbinNodeBranch {
		c.hash, c.hashLen = h, length.Hash
	}
	return h, nil
}

// cellHash resolves whatever a cell is missing — a leaf's state, a branch's
// stale hash — and hands it to the one hasher.
func (pph *PBinPatriciaHashed) cellHash(c *pbinCell, path *pbinBitpath) (common.Hash, error) {
	switch c.kind {
	case pbinNodeLeaf:
		if err := pph.loadCellState(c); err != nil {
			return common.Hash{}, err
		}
	case pbinNodeBranch:
		if !c.childrenSet && c.hashLen == 0 {
			if err := pph.materializeBranch(c, path); err != nil {
				return common.Hash{}, err
			}
		}
	}
	return pph.hasher.cellHash(c, path)
}

// loadCellState fills a leaf cell whose plain key arrived from a record and
// whose value therefore did not. The leaf is already in the tree, so an absent
// read is pbinZeroedLeafUpdate's case: a zero value for storage, a refusal for
// an account.
func (pph *PBinPatriciaHashed) loadCellState(c *pbinCell) error {
	if c.accountAddrLen > 0 && !c.loaded.account() {
		plainKey := c.accountAddr[:c.accountAddrLen]
		update, err := pph.ctx.Account(plainKey)
		if err != nil {
			return fmt.Errorf("pbin: read account %x: %w", plainKey, err)
		}
		if update.Deleted() {
			return fmt.Errorf("%w: %x", errPBinDeleteUnsupported, plainKey)
		}
		c.setFromUpdate(update)
		c.loaded = c.loaded.addFlag(cellLoadAccount)
	}
	if c.storageAddrLen > 0 && !c.loaded.storage() {
		plainKey := c.storageAddr[:c.storageAddrLen]
		update, err := pph.ctx.Storage(plainKey)
		if err != nil {
			return fmt.Errorf("pbin: read storage %x: %w", plainKey, err)
		}
		if update.Deleted() {
			zeroed, err := pbinZeroedLeafUpdate(plainKey)
			if err != nil {
				return err
			}
			update = &zeroed
		}
		c.setFromUpdate(update)
		c.loaded = c.loaded.addFlag(cellLoadStorage)
	}
	return nil
}

// materializeBranch rebuilds a branch cell's hash under the prefix it holds now
// by reading its own record. A split shortens a survivor's prefix without moving
// its record, so the key is the cell's path followed by that prefix.
func (pph *PBinPatriciaHashed) materializeBranch(c *pbinCell, path *pbinBitpath) error {
	nodeKey := *path
	nodeKey.append(&c.prefix)
	key := pbinEncodeBitPath(&nodeKey)

	data, _, err := pph.ctx.Branch(key)
	if err != nil {
		return fmt.Errorf("pbin: read branch at %x: %w", key, err)
	}
	if len(data) == 0 {
		return fmt.Errorf("%w at %x (%d bits)", errPBinMissingBranch, key, nodeKey.bitLen)
	}
	pph.counters.materializeReads++

	var cells [2]pbinCell
	if _, _, err = pbinDecodeBranch(data, &cells); err != nil {
		return fmt.Errorf("pbin: decode branch at %x: %w", key, err)
	}
	childPath := nodeKey
	childPath.appendBit(0)
	left, err := pph.cellHash(&cells[0], &childPath)
	if err != nil {
		return err
	}
	childPath.setBitAt(nodeKey.bitLen, 1)
	right, err := pph.cellHash(&cells[1], &childPath)
	if err != nil {
		return err
	}

	c.children, c.childrenSet = [2]common.Hash{left, right}, true
	c.hash = pph.hasher.branchHash(&c.prefix, &left, &right)
	c.hashLen = length.Hash
	return nil
}
