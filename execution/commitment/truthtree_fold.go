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

package commitment

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/rlp"
)

// foldCtx is a per-goroutine scratch bundle threaded through the whole recursion: one
// HexPatriciaHashed for its keccak state and cell-hash helpers, one reused scratch cell, and
// one reused hash-output buffer. No per-node heap cell is allocated — this reuse is the
// buffer-reuse win the proto validated (the naive per-node-cell fold regresses ~10x on alloc).
type foldCtx struct {
	hph      *HexPatriciaHashed
	cell     cell
	buf      []byte
	emit     bool
	deferred []*DeferredBranchUpdate
}

// foldChildKind classifies one present slot of a branch during a fold, selecting how its cell is
// re-derived in the branch RLP pass: a storage or account leaf (hashed transitively by
// computeCellHash from its plain key), a depth-64 account whose storage subtree was folded to bh,
// or a sub-branch already folded to bh.
type foldChildKind uint8

const (
	childStorageLeaf foldChildKind = iota
	childAccountLeaf
	childSeamAccount       // depth-64 account terminator over a storage branch (≥2 nibbles); bh is its storage root
	childSeamAccountInline // depth-64 account terminator over a single storage slot, held inline as storageAddr
	childSeamAccountExt    // depth-64 account terminator over a single-nibble storage sub-branch; bh is the sub-branch hash
	childBranch
)

// foldChild records one present slot of a branch during a fold: its kind, the folded node, and,
// for a sub-branch or seam account, the hash (branch hash or storage root) applied to its cell,
// plus its length contribution to the branch RLP list.
type foldChild struct {
	present bool
	node    *prefixNode
	bh      common.Hash
	hlen    int16
	kind    foldChildKind
}

func (fc *foldCtx) setStorageLeaf(node *prefixNode) {
	c := &fc.cell
	c.reset()
	c.storageAddrLen = int16(len(node.plainKey))
	copy(c.storageAddr[:], node.plainKey)
	c.setFromUpdate(node.update)
}

// setAccountLeaf places an account terminator's cell: reset restores empty.CodeHash, so a
// balance-only update (no CodeUpdate flag) keeps it. computeCellHash re-derives the account key
// tail from accountAddr at the slot depth, so no extension is set here.
func (fc *foldCtx) setAccountLeaf(node *prefixNode) {
	c := &fc.cell
	c.reset()
	c.accountAddrLen = int16(len(node.plainKey))
	copy(c.accountAddr[:], node.plainKey)
	c.setFromUpdate(node.update)
}

// setPlaneLeaf places a present leaf slot's cell by plane and returns its foldChild kind. On the
// account plane a leaf must carry an account key; a longer key is an orphan storage slot (a storage
// write with no account, impossible in real state) that would land above the depth-64 seam and hash
// wrong, so it is rejected fail-closed rather than mis-folded.
func (fc *foldCtx) setPlaneLeaf(node *prefixNode, storagePlane bool) (foldChildKind, error) {
	if storagePlane {
		fc.setStorageLeaf(node)
		return childStorageLeaf, nil
	}
	if int16(len(node.plainKey)) != fc.hph.accountKeyLen {
		return 0, fmt.Errorf("truthtree fold: account-plane leaf with non-account key len %d", len(node.plainKey))
	}
	fc.setAccountLeaf(node)
	return childAccountLeaf, nil
}

// setSeamAccount places a depth-64 account over a storage branch (≥2 first nibbles): the cell
// carries the folded storage branch root as its hash, which computeCellHash consumes directly.
func (fc *foldCtx) setSeamAccount(node *prefixNode, sr common.Hash) {
	fc.setAccountLeaf(node)
	fc.cell.hash = sr
	fc.cell.hashLen = length.Hash
}

// setSeamAccountInline places a depth-64 account whose storage is a single slot: the cell carries
// both the account and the storage plain key, matching the sequential engine's representation, and
// computeCellHash derives the singleton storage root from the slot on the fly.
func (fc *foldCtx) setSeamAccountInline(node *prefixNode) {
	slot := node.children[0]
	fc.setAccountLeaf(node)
	c := &fc.cell
	c.storageAddrLen = int16(len(slot.plainKey))
	copy(c.storageAddr[:], slot.plainKey)
	c.setFromUpdate(slot.update)
}

// setSeamAccountExt places a depth-64 account whose storage is a single first nibble over a
// sub-branch: the cell keeps the storage extension (survivor nibble ++ sub-branch ext) and the
// sub-branch hash, so computeCellHash re-derives the storage root by extension-hashing — the
// sequential engine's representation, which stores the pre-extension hash, not the folded root.
func (fc *foldCtx) setSeamAccountExt(node *prefixNode, bh common.Hash) {
	survNib := bits.TrailingZeros16(node.bitmap)
	sub := node.children[0]
	fc.setAccountLeaf(node)
	c := &fc.cell
	c.extLen = int16(len(sub.ext)) + 1
	c.extension[0] = byte(survNib)
	copy(c.extension[1:], sub.ext)
	c.hashLen = length.Hash
	c.hash = bh
}

// seamAccountInline reports whether an account terminator's storage is a lone slot (a single
// storage leaf), which the fold holds inline rather than folding to a separate storage root.
func seamAccountInline(node *prefixNode) bool {
	return bits.OnesCount16(node.bitmap) == 1 && node.children[0].bitmap == 0
}

func (fc *foldCtx) setBranch(node *prefixNode, bh common.Hash) {
	c := &fc.cell
	c.reset()
	if n := int16(len(node.ext)); n > 0 {
		c.extLen = n
		copy(c.extension[:], node.ext)
	}
	c.hashLen = length.Hash
	c.hash = bh
}

// foldNode folds the branch node whose branch row sits at branchDepth into its keccak hash,
// recursing touched child branches and hashing leaves at their parent-slot depth. It is the
// direct-recursion analog of foldMounted for a fresh (no on-disk siblings) subtree in either plane:
// a leaf is placed raw so computeCellHash re-derives its key tail at the slot depth; a sub-branch
// is folded here and applied as an extension/hash cell so the parent applies extension hashing over
// the branch hash. branchDepth >= 64 marks the storage plane (children are storage nibbles);
// below it children are account nibbles, and a child that both terminates an account key and
// branches is the depth-64 storage seam — its storage subtree folds to a root the account leaf
// hashes over. Only the 17-slot branch keccak is hand-rolled; leaf and extension hashing run
// transitively inside computeCellHash. The hash is returned by value into fc's reused scratch —
// the non-escaping shape the proto settled on; escape behavior is a manual `-gcflags=-m` spot-check,
// while the alloc-ceiling bench is the CI gate.
func (fc *foldCtx) foldNode(node *prefixNode, prefix []byte, branchDepth int16) (common.Hash, error) {
	childDepth := branchDepth + 1
	storagePlane := branchDepth >= 64
	var rec [16]foldChild
	childIdx := 0
	totalLen := 0
	for bm := node.bitmap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		child := node.children[childIdx]
		r := &rec[nib]
		r.present, r.node = true, child
		switch {
		case child.bitmap == 0:
			if child.plainKey == nil {
				return common.Hash{}, fmt.Errorf("truthtree fold: leaf without plainKey at slot %x", nib)
			}
			kind, err := fc.setPlaneLeaf(child, storagePlane)
			if err != nil {
				return common.Hash{}, err
			}
			r.kind = kind
			r.hlen = fc.hph.computeCellHashLen(&fc.cell, childDepth)
		case child.plainKey != nil:
			// Depth-64 storage seam: the child terminates an account key and branches into storage.
			switch {
			case seamAccountInline(child):
				r.kind = childSeamAccountInline
				fc.setSeamAccountInline(child)
			case bits.OnesCount16(child.bitmap) == 1:
				r.kind = childSeamAccountExt
				survNib := bits.TrailingZeros16(child.bitmap)
				sub := child.children[0]
				subPrefix := fc.seamSubPrefix(prefix, nib, child.ext, survNib, sub.ext)
				bh, err := fc.foldNode(sub, subPrefix, 64+1+int16(len(sub.ext)))
				if err != nil {
					return common.Hash{}, err
				}
				r.bh = bh
				fc.setSeamAccountExt(child, bh)
			default:
				r.kind = childSeamAccount
				accPrefix := fc.childPrefix(prefix, nib, child.ext)
				sr, err := fc.foldNode(child, accPrefix, 64)
				if err != nil {
					return common.Hash{}, err
				}
				r.bh = sr
				fc.setSeamAccount(child, sr)
			}
			r.hlen = fc.hph.computeCellHashLen(&fc.cell, childDepth)
		default:
			r.kind = childBranch
			childPrefix := fc.childPrefix(prefix, nib, child.ext)
			bh, err := fc.foldNode(child, childPrefix, childDepth+int16(len(child.ext)))
			if err != nil {
				return common.Hash{}, err
			}
			r.bh = bh
			r.hlen = length.Hash + 1
		}
		totalLen += int(r.hlen)
		childIdx++
		bm &^= uint16(1) << nib
	}
	return fc.hashBranchRow(node, prefix, &rec, totalLen, childDepth)
}

// hashBranchRow keccaks the 17-slot branch row from the already-classified child records into the
// branch hash and, when emit is on, stages this branch's DeferredBranchUpdate. rec holds every
// present slot's kind/node/child-hash; totalLen is the RLP payload length summed from the child
// hlens (the empty-slot padding is added here). It is the shared branch-stitch tail of the serial
// foldNode and the fork-join fold, so both produce byte-identical hashes and records.
func (fc *foldCtx) hashBranchRow(node *prefixNode, prefix []byte, rec *[16]foldChild, totalLen int, childDepth int16) (common.Hash, error) {
	totalLen += 17 - bits.OnesCount16(node.bitmap)

	fc.hph.keccak2.Reset()
	var lp [4]byte
	pt := rlp.EncodeListPrefixToBuf(totalLen, lp[:])
	fc.hph.keccak2.Write(lp[:pt])
	var cellData [16]cellEncodeData
	b80 := [1]byte{0x80}
	for s := range 17 {
		if s == 16 || !rec[s].present {
			fc.hph.keccak2.Write(b80[:])
			continue
		}
		r := &rec[s]
		switch r.kind {
		case childStorageLeaf:
			fc.setStorageLeaf(r.node)
		case childAccountLeaf:
			fc.setAccountLeaf(r.node)
		case childSeamAccount:
			fc.setSeamAccount(r.node, r.bh)
		case childSeamAccountInline:
			fc.setSeamAccountInline(r.node)
		case childSeamAccountExt:
			fc.setSeamAccountExt(r.node, r.bh)
		default:
			fc.setBranch(r.node, r.bh)
		}
		hb, err := fc.hph.computeCellHash(&fc.cell, childDepth, fc.buf[:0])
		if err != nil {
			return common.Hash{}, err
		}
		fc.buf = hb
		fc.hph.keccak2.Write(hb)
		if fc.emit {
			cellData[s] = cellEncodeDataFromCell(&fc.cell)
		}
	}
	var h common.Hash
	fc.hph.keccak2.Read(h[:])
	if fc.emit {
		if err := fc.emitBranchUpdate(node.bitmap, prefix, &cellData); err != nil {
			return common.Hash{}, err
		}
	}
	return h, nil
}

// newFoldCtx builds a foldCtx with its own HexPatriciaHashed keccak state; emit toggles
// DeferredBranchUpdate staging. Each fork-join lineage owns one (own scratch cell + output buffer),
// so a forked goroutine never shares mutable state with its parent. The caller releases fc.hph.
func newFoldCtx(emit bool) *foldCtx {
	return &foldCtx{hph: NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig()), emit: emit}
}

// forkFolder folds a provably-fresh subtree — in either plane — with a recursive per-split-point
// fork-join, bounding concurrency by sem and splitting at any branch whose subtreeCount exceeds k —
// the same threshold the fold DAG uses. Below k a subtree folds serially via foldNode's buffer-reuse
// path; above it each child branch forks onto its own lineage, and a depth-64 account seam recurses
// into its storage on a grain sized to that storage subtree's own key count (see storageFold). The
// stitch is foldNode's exact 17-slot branch keccak, so the fork-join root and its branch records are
// byte-identical to the serial fold. numWorkers sizes the storage-plane grain and is 0 for a caller
// that pins k directly (a test forcing a low K); foldK treats 0 workers as 1.
type forkFolder struct {
	sem        *semaphore.Weighted
	k          uint32
	numWorkers int
}

// forkFoldMaxDepth records the deepest branch (its row's nibble depth) at which a fresh fork-join
// spawned child lineages. Tests read it to confirm splits fanned out at deep prefixes (account
// branches and, across the seam, a whale's storage), not only at the top nibble.
var forkFoldMaxDepth atomic.Int64

func recordForkDepth(depth int16) {
	d := int64(depth)
	for {
		cur := forkFoldMaxDepth.Load()
		if d <= cur || forkFoldMaxDepth.CompareAndSwap(cur, d) {
			return
		}
	}
}

// forkJob is one storage sub-branch large enough (subtreeCount > k) to be a split point: either
// forked onto its own lineage or, when foldSem is saturated, folded inline in the parent lineage.
type forkJob struct {
	node   *prefixNode
	prefix []byte
	depth  int16
	nib    int
}

// fold folds the branch pn (whose branch row sits at branchDepth, in either plane) into its hash and
// merges every forked child's deferred updates into fc. A subtree at or below k folds serially in fc
// via foldNode (buffer reuse). Above k, leaves and small child branches fold serially in fc, and each
// child branch larger than k is a split point: it forks onto its own foldCtx when a foldSem slot is
// free, else folds inline in fc. A depth-64 account seam recurses back through fold, so a whale's
// fresh storage plane fans out on the same split threshold as the account plane above it.
//
// TryAcquire (not a blocking Acquire) is load-bearing for deadlock freedom: a forked goroutine holds
// its slot while awaiting its own forked children, so under saturation a blocking acquire would wedge
// every slot on a parent waiting for a child that can never start. Falling back to an inline fold
// always makes progress without another slot. fc is only ever touched by this goroutine — forked
// children own separate foldCtxs and their results merge after the join — so no lock guards it.
func (ff *forkFolder) fold(ctx context.Context, fc *foldCtx, pn *prefixNode, prefix []byte, branchDepth int16) (common.Hash, error) {
	if pn.subtreeCount <= ff.k {
		return fc.foldNode(pn, prefix, branchDepth)
	}
	childDepth := branchDepth + 1
	storagePlane := branchDepth >= 64
	var rec [16]foldChild
	totalLen := 0
	var jobs []forkJob
	childIdx := 0
	for bm := pn.bitmap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		child := pn.children[childIdx]
		r := &rec[nib]
		r.present, r.node = true, child
		switch {
		case child.bitmap == 0:
			if child.plainKey == nil {
				return common.Hash{}, fmt.Errorf("truthtree fork fold: leaf without plainKey at slot %x", nib)
			}
			kind, err := fc.setPlaneLeaf(child, storagePlane)
			if err != nil {
				return common.Hash{}, err
			}
			r.kind = kind
			r.hlen = fc.hph.computeCellHashLen(&fc.cell, childDepth)
		case child.plainKey != nil:
			// Depth-64 account seam: the child terminates an account key and branches into storage,
			// folded back through storageFold so a large storage plane forks on its own grain.
			switch {
			case seamAccountInline(child):
				r.kind = childSeamAccountInline
				fc.setSeamAccountInline(child)
			case bits.OnesCount16(child.bitmap) == 1:
				r.kind = childSeamAccountExt
				survNib := bits.TrailingZeros16(child.bitmap)
				sub := child.children[0]
				subPrefix := fc.seamSubPrefix(prefix, nib, child.ext, survNib, sub.ext)
				bh, err := ff.storageFold(ctx, fc, sub, subPrefix, 64+1+int16(len(sub.ext)))
				if err != nil {
					return common.Hash{}, err
				}
				r.bh = bh
				fc.setSeamAccountExt(child, bh)
			default:
				r.kind = childSeamAccount
				accPrefix := fc.childPrefix(prefix, nib, child.ext)
				sr, err := ff.storageFold(ctx, fc, child, accPrefix, 64)
				if err != nil {
					return common.Hash{}, err
				}
				r.bh = sr
				fc.setSeamAccount(child, sr)
			}
			r.hlen = fc.hph.computeCellHashLen(&fc.cell, childDepth)
		default:
			r.kind = childBranch
			r.hlen = length.Hash + 1
			childBranchDepth := childDepth + int16(len(child.ext))
			childPrefix := fc.childPrefix(prefix, nib, child.ext)
			if child.subtreeCount > ff.k {
				jobs = append(jobs, forkJob{node: child, prefix: childPrefix, depth: childBranchDepth, nib: nib})
			} else {
				bh, err := fc.foldNode(child, childPrefix, childBranchDepth)
				if err != nil {
					return common.Hash{}, err
				}
				r.bh = bh
			}
		}
		totalLen += int(r.hlen)
		childIdx++
		bm &^= uint16(1) << nib
	}

	if len(jobs) > 0 {
		results := make([][]*DeferredBranchUpdate, len(jobs))
		forkedBH := make([]common.Hash, len(jobs))
		forked := make([]bool, len(jobs))
		g, gctx := errgroup.WithContext(ctx)
		var inlineErr error
		for i := range jobs {
			if ff.sem.TryAcquire(1) {
				recordForkDepth(branchDepth)
				forked[i] = true
				g.Go(func() error {
					defer ff.sem.Release(1)
					cfc := newFoldCtx(fc.emit)
					defer cfc.hph.Release()
					bh, err := ff.fold(gctx, cfc, jobs[i].node, jobs[i].prefix, jobs[i].depth)
					if err != nil {
						putDeferredUpdates(cfc.deferred)
						return err
					}
					forkedBH[i] = bh
					results[i] = cfc.deferred
					return nil
				})
				continue
			}
			bh, err := ff.fold(ctx, fc, jobs[i].node, jobs[i].prefix, jobs[i].depth)
			if err != nil {
				inlineErr = err
				break
			}
			rec[jobs[i].nib].bh = bh
		}
		werr := g.Wait()
		if inlineErr != nil || werr != nil {
			for i := range results {
				putDeferredUpdates(results[i])
			}
			if inlineErr != nil {
				return common.Hash{}, inlineErr
			}
			return common.Hash{}, werr
		}
		for i := range jobs {
			if forked[i] {
				rec[jobs[i].nib].bh = forkedBH[i]
				fc.deferred = append(fc.deferred, results[i]...)
			}
		}
	}

	return fc.hashBranchRow(pn, prefix, &rec, totalLen, childDepth)
}

// storageFold recurses into a depth-64 account's fresh storage plane on a grain sized to that storage
// subtree's own key count rather than the enclosing account-plane gate. On a whale-dominated build the
// account-plane k (root subtreeCount / numWorkers) is far coarser than a big storage subtree needs, so
// without a storage-local grain the whale's storage folds serially behind the account plane at
// numWorkers=NumCPU and only splits once oversubscription shrinks the global grain. The grain is capped
// at ff.k so a caller that pins a low K (a test proving deep forking) still forks at least that finely
// below the seam. Only the grain differs — the fold is byte-identical to a same-k ff.fold.
func (ff *forkFolder) storageFold(ctx context.Context, fc *foldCtx, node *prefixNode, prefix []byte, branchDepth int16) (common.Hash, error) {
	storageFF := &forkFolder{sem: ff.sem, numWorkers: ff.numWorkers, k: min(ff.k, foldK(node.subtreeCount, ff.numWorkers))}
	return storageFF.fold(ctx, fc, node, prefix, branchDepth)
}

// childPrefix builds the hex-nibble path to a child branch (prefix ++ slot nibble ++ child ext)
// for deferred-update emission; when emit is off no prefix is needed, so it stays nil (and
// allocation-free — the alloc-ceiling path).
func (fc *foldCtx) childPrefix(prefix []byte, nib int, ext []byte) []byte {
	if !fc.emit {
		return nil
	}
	out := make([]byte, 0, len(prefix)+1+len(ext))
	out = append(out, prefix...)
	out = append(out, byte(nib))
	out = append(out, ext...)
	return out
}

// seamSubPrefix builds the hex-nibble path to a seam account's lone storage sub-branch (account
// path ++ storage survivor nibble ++ sub-branch ext) for deferred emission; nil when emit is off.
func (fc *foldCtx) seamSubPrefix(prefix []byte, nib int, accExt []byte, survNib int, subExt []byte) []byte {
	if !fc.emit {
		return nil
	}
	out := make([]byte, 0, len(prefix)+1+len(accExt)+1+len(subExt))
	out = append(out, prefix...)
	out = append(out, byte(nib))
	out = append(out, accExt...)
	out = append(out, byte(survNib))
	out = append(out, subExt...)
	return out
}

// emitBranchUpdate encodes this fresh branch (every present child both touched and after, no
// predecessor) with the same BranchEncoder the sequential fold uses and stages a DeferredBranchUpdate
// for prefix, so the emitted records are byte-identical to foldMounted's.
func (fc *foldCtx) emitBranchUpdate(bitmap uint16, prefix []byte, cellData *[16]cellEncodeData) error {
	raw, err := fc.hph.branchEncoder.EncodeBranch(bitmap, bitmap, bitmap, cellData)
	if err != nil {
		return err
	}
	fc.deferred = append(fc.deferred, getDeferredUpdate(nibbles.HexToCompact(prefix), raw, nil))
	return nil
}

// resolveSubtreeUpdates materializes every terminator update in the subtree and reports whether any
// terminator is an empty (deleted) value. A ModeParallel touch stages either a nil update
// (TouchPlainKey — re-read from state on demand, as the replay path does) or a carried value
// (TouchPlainKeyDirect — the parallel-exec commitment calculator's entry point, which can carry a
// DeleteUpdate). The direct fold reads node.update straight and hashes every present slot, so a
// delete in either form is a leaf it would wrongly hash instead of drop; reporting it routes the
// caller back to replay, which folds the delete correctly. Fail-closed on the first read error.
func resolveSubtreeUpdates(node *prefixNode, accountKeyLen int16, readAccount, readStorage func([]byte) (*Update, error)) (hasEmpty bool, err error) {
	if node == nil {
		return false, nil
	}
	if node.plainKey != nil {
		if node.update == nil {
			var u *Update
			if int16(len(node.plainKey)) == accountKeyLen {
				u, err = readAccount(node.plainKey)
			} else {
				u, err = readStorage(node.plainKey)
			}
			if err != nil {
				return false, err
			}
			node.update = u
		}
		if node.update != nil && node.update.Deleted() {
			hasEmpty = true
		}
	}
	for _, c := range node.children {
		childEmpty, cerr := resolveSubtreeUpdates(c, accountKeyLen, readAccount, readStorage)
		if cerr != nil {
			return false, cerr
		}
		hasEmpty = hasEmpty || childEmpty
	}
	return hasEmpty, nil
}

// foldFreshStorageRoot folds a fresh account's touched storage subtree (rooted at the depth-64
// account node, whose children are the first storage nibbles) into its storage root, reading no
// on-disk siblings — the fresh-whale case the proto validated. An empty subtree hashes to the
// empty-storage root.
func foldFreshStorageRoot(node *prefixNode) (common.Hash, error) {
	if node == nil || node.bitmap == 0 {
		return empty.RootHash, nil
	}
	fc := &foldCtx{hph: NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())}
	defer fc.hph.Release()
	return fc.foldNode(node, nil, 64)
}

// foldFreshStorageRootDeferred folds a fresh account's touched storage subtree like
// foldFreshStorageRoot, but also emits a DeferredBranchUpdate per storage branch prefix, so the
// caller can persist the trie, not just its root. accPrefix is the 64-nibble account path the
// storage-root branch sits at. Fail-closed: any fold error drops every collected update.
func foldFreshStorageRootDeferred(node *prefixNode, accPrefix []byte) (common.Hash, []*DeferredBranchUpdate, error) {
	if node == nil || node.bitmap == 0 {
		return empty.RootHash, nil, nil
	}
	fc := &foldCtx{hph: NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig()), emit: true}
	defer fc.hph.Release()
	h, err := fc.foldNode(node, accPrefix, 64)
	if err != nil {
		for _, upd := range fc.deferred {
			putDeferredUpdate(upd)
		}
		return common.Hash{}, nil, err
	}
	return h, fc.deferred, nil
}

// foldAccountRoot folds a fresh account trie (rooted at root, whose branch sits at len(root.ext))
// into its state root, crossing every depth-64 storage seam inline. A non-empty root extension is
// hashed over the root branch at depth 0.
func (fc *foldCtx) foldAccountRoot(root *prefixNode) (common.Hash, error) {
	bh, err := fc.foldNode(root, root.ext, int16(len(root.ext)))
	if err != nil {
		return common.Hash{}, err
	}
	if len(root.ext) == 0 {
		return bh, nil
	}
	c := &fc.cell
	c.reset()
	c.extLen = int16(len(root.ext))
	copy(c.extension[:], root.ext)
	c.hashLen = length.Hash
	c.hash = bh
	h, err := fc.hph.computeCellHash(c, 0, fc.buf[:0])
	if err != nil {
		return common.Hash{}, err
	}
	fc.buf = h
	var out common.Hash
	copy(out[:], h[1:])
	return out, nil
}

// foldFreshAccountRoot folds a fresh account trie into its state root, reading no on-disk siblings
// — the account-plane analog of foldFreshStorageRoot. An empty trie hashes to the empty root.
func foldFreshAccountRoot(root *prefixNode) (common.Hash, error) {
	if root == nil || root.bitmap == 0 {
		return empty.RootHash, nil
	}
	fc := &foldCtx{hph: NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())}
	defer fc.hph.Release()
	return fc.foldAccountRoot(root)
}

// foldFreshAccountRootDeferred folds a fresh account trie like foldFreshAccountRoot, but also emits
// a DeferredBranchUpdate per account and storage branch prefix. Fail-closed: any fold error drops
// every collected update.
func foldFreshAccountRootDeferred(root *prefixNode) (common.Hash, []*DeferredBranchUpdate, error) {
	if root == nil || root.bitmap == 0 {
		return empty.RootHash, nil, nil
	}
	fc := &foldCtx{hph: NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig()), emit: true}
	defer fc.hph.Release()
	h, err := fc.foldAccountRoot(root)
	if err != nil {
		for _, upd := range fc.deferred {
			putDeferredUpdate(upd)
		}
		return common.Hash{}, nil, err
	}
	return h, fc.deferred, nil
}

// foldSubtreeCell folds a fresh account-plane branch subtree at slot nib under parentPrefix into
// the mount-wall-relative cell the parent stitches (invariant M): its extension spans the whole
// subtree prefix, which stripCellToMountWall trims of the parent prefix and slot nibble. Depth-64
// seams inside the subtree fold inline. Only a pure branch subtree is a leaf-task root here; a lone
// account or a whale seam at the mount boundary is handled by the account-leaf/whale paths.
func (fc *foldCtx) foldSubtreeCell(node *prefixNode, parentPrefix []byte, nib int) (cell, error) {
	prefix, err := subtreeCellPrefix(node, parentPrefix, nib)
	if err != nil {
		return cell{}, err
	}
	bh, err := fc.foldNode(node, prefix, int16(len(prefix)))
	if err != nil {
		return cell{}, err
	}
	return mountWallCell(prefix, bh, int16(len(parentPrefix))+1), nil
}

// foldSubtreeCellForkJoin is the fork-join analog of foldSubtreeCell: it folds a fresh account-plane
// leaf-task subtree through the recursive per-split-point fork-join instead of the serial foldNode,
// producing the byte-identical mount-wall cell the parent stitches.
func (fc *foldCtx) foldSubtreeCellForkJoin(ctx context.Context, ff *forkFolder, node *prefixNode, parentPrefix []byte, nib int) (cell, error) {
	prefix, err := subtreeCellPrefix(node, parentPrefix, nib)
	if err != nil {
		return cell{}, err
	}
	bh, err := ff.fold(ctx, fc, node, prefix, int16(len(prefix)))
	if err != nil {
		return cell{}, err
	}
	return mountWallCell(prefix, bh, int16(len(parentPrefix))+1), nil
}

// subtreeCellPrefix builds the hex-nibble path (parentPrefix ++ slot nibble ++ node ext) to a fresh
// account-plane leaf-task subtree. Only a pure branch is a leaf-task root; a lone account or a whale
// seam at the mount boundary is handled by the account-leaf/whale paths.
func subtreeCellPrefix(node *prefixNode, parentPrefix []byte, nib int) ([]byte, error) {
	if node.bitmap == 0 || node.plainKey != nil {
		return nil, fmt.Errorf("truthtree fold: subtree cell expects a pure branch at slot %x", nib)
	}
	prefix := make([]byte, 0, len(parentPrefix)+1+len(node.ext))
	prefix = append(prefix, parentPrefix...)
	prefix = append(prefix, byte(nib))
	prefix = append(prefix, node.ext...)
	return prefix, nil
}

// mountWallCell wraps a folded subtree branch hash into the parent-stitchable cell (invariant M): its
// extension spans the whole subtree prefix, which stripCellToMountWall trims of the parent prefix and
// slot nibble.
func mountWallCell(prefix []byte, bh common.Hash, mountWall int16) cell {
	var full cell
	full.extLen = int16(len(prefix))
	copy(full.extension[:], prefix)
	full.hashedExtLen = int16(len(prefix))
	copy(full.hashedExtension[:], prefix)
	full.hashLen = length.Hash
	full.hash = bh
	return stripCellToMountWall(&full, mountWall)
}

// foldFreshAccountSubtreeCellDeferred folds a fresh account-plane leaf-task subtree into its
// parent-stitchable mount-wall cell plus the branch records it emits. Fail-closed on any error.
func foldFreshAccountSubtreeCellDeferred(node *prefixNode, parentPrefix []byte, nib int) (cell, []*DeferredBranchUpdate, error) {
	fc := &foldCtx{hph: NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig()), emit: true}
	defer fc.hph.Release()
	c, err := fc.foldSubtreeCell(node, parentPrefix, nib)
	if err != nil {
		for _, upd := range fc.deferred {
			putDeferredUpdate(upd)
		}
		return cell{}, nil, err
	}
	return c, fc.deferred, nil
}

// foldReconciledStorageRoot folds a touched storage subtree against on-disk state, so untouched
// on-disk siblings inside a touched branch survive into the folded root — the reconciliation the
// fresh hand-rolled fold lacks (it reads no disk). It seeds row 0 from the on-disk branch at
// accPrefix, replays the touched keys so unfold reads each deeper on-disk branch and fold keeps its
// untouched children blinded, then collapses row 0 into the account's single storage-root cell at
// the depth-64 seam. A missing branch at accPrefix means the account is storage-less on disk (the
// fresh case) — an empty wall, no siblings. Returns the storage root and the deferred branch
// updates the fold produced.
func foldReconciledStorageRoot(fp *foldPool, ctx context.Context, node *prefixNode, accPrefix []byte) (common.Hash, []*DeferredBranchUpdate, error) {
	base, release := newDeferredStorageWorker(fp.workerPool, fp.ctxFactory, fp.traceW)
	defer release()

	if err := seedBaseAtPrefix(base, accPrefix); err != nil && !errors.Is(err, errStorageBaseNotBranch) {
		return common.Hash{}, nil, fmt.Errorf("reconciled storage seed: %w", err)
	}

	if err := dfsSubtree(node, append([]byte(nil), accPrefix...), base.followAndUpdate); err != nil {
		return common.Hash{}, nil, err
	}
	for base.activeRows > 1 {
		if err := ctx.Err(); err != nil {
			return common.Hash{}, nil, err
		}
		if err := base.fold(); err != nil {
			return common.Hash{}, nil, fmt.Errorf("reconciled storage fold: %w", err)
		}
	}

	var noChildren [16]cell
	sr, err := aggregateMountedStorageRoot(base, &noChildren, 0)
	if err != nil {
		return common.Hash{}, nil, fmt.Errorf("reconciled storage aggregate: %w", err)
	}
	deferred := base.TakeDeferredUpdates()
	if sr.IsEmpty() {
		return empty.RootHash, deferred, nil
	}
	return sr.hash, deferred, nil
}
