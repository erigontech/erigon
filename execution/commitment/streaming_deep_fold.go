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
	"io"
	"math/bits"
	"runtime"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// hk is copied off the walk path; pk and upd still reference the caller's storage.
type touchedKey struct {
	hk  []byte
	pk  []byte
	upd *Update
}

// maxFoldConcurrency caps storage-leaf fold fan-out at the CPUs the process may run on.
func maxFoldConcurrency() int { return max(1, runtime.GOMAXPROCS(0)) }

func newFoldSem() *semaphore.Weighted { return semaphore.NewWeighted(int64(maxFoldConcurrency())) }

func foldStorageLeaf(ctx context.Context, w *HexPatriciaHashed, base *HexPatriciaHashed, nib int, group []touchedKey) (cell, error) {
	w.mountTo(base, nib)
	for i := range group {
		if err := w.followAndUpdate(group[i].hk, group[i].pk, group[i].upd); err != nil {
			return cell{}, err
		}
	}
	return w.foldMounted(ctx, nib)
}

func isDeepStorageAccount(node *prefixNode, depth int) bool {
	return depth == 64 && node.plainKey != nil &&
		bits.OnesCount16(node.bitmap) >= 2 && node.subtreeCount > deepStorageThreshold
}

func dfsSubtreeDeep(w *HexPatriciaHashed, node *prefixNode, path []byte, storageRoot func(node *prefixNode, path []byte, accountFresh bool) (cell, error)) error {
	if node == nil {
		return nil
	}
	accountFresh := false
	if node.plainKey != nil {
		if err := w.followAndUpdate(path, node.plainKey, node.update); err != nil {
			return err
		}
		accountFresh = w.lastUpdateCellWasEmpty
	} else if node.bitmap == 0 {
		return errors.New("commitment: trie leaf without a plainKey")
	}

	if isDeepStorageAccount(node, len(path)) {
		sr, err := storageRoot(node, path, accountFresh)
		if err == nil {
			setAccountStorageRoot(w, path, sr)
			return nil
		}
		if !errors.Is(err, errSplitNotBranch) {
			return fmt.Errorf("storageRoot: %w", err)
		}
	}

	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := byte(bits.TrailingZeros16(bm))
		child := node.children[childIdx]
		base := len(path)
		path = append(path, nib)
		path = append(path, child.ext...)
		if err := dfsSubtreeDeep(w, child, path, storageRoot); err != nil {
			return err
		}
		path = path[:base]
		childIdx++
		bm &^= uint16(1) << nib
	}
	return nil
}

func foldStorageRoot(ctx context.Context, sem *semaphore.Weighted, newWorker func(context.Context) (*HexPatriciaHashed, func()), pu *parallelUpdate, node *prefixNode, path []byte, accountFresh bool) (cell, error) {
	accPrefix := append([]byte(nil), path...)

	base, releaseBase := newWorker(ctx)
	defer releaseBase()

	var accTag string
	if base.traceW != nil {
		accID := node.plainKey
		if accID == nil {
			accID = accPrefix
		}
		accTag = fmt.Sprintf("[%x] ", accID)
		base.SetTraceWriter(tracePrefix(base.traceW, accTag))
	}
	if err := unfoldSplitBase(base, accPrefix); err != nil {
		if !accountFresh || !errors.Is(err, errSplitNotBranch) {
			return cell{}, fmt.Errorf("unfold storage root: %w", err)
		}
	}

	var children [16]cell
	g, gctx := errgroup.WithContext(ctx)
	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := int(bits.TrailingZeros16(bm))
		child := node.children[childIdx]
		ni, ch := nib, child
		childPrefix := make([]byte, len(accPrefix), len(accPrefix)+1+len(ch.ext))
		copy(childPrefix, accPrefix)
		childPrefix = append(childPrefix, byte(ni))
		childPrefix = append(childPrefix, ch.ext...)
		group := collectSubtreeKeys(ch, childPrefix)
		g.Go(func() error {
			if err := sem.Acquire(gctx, 1); err != nil {
				return err
			}
			defer sem.Release(1)
			w, release := newWorker(gctx)
			if w.traceW != nil {
				w.SetTraceWriter(tracePrefix(w.traceW, accTag))
			}
			c, err := foldStorageLeaf(gctx, w, base, ni, group)
			if err == nil {
				if d := w.TakeDeferredUpdates(); len(d) > 0 {
					pu.appendDeferred(d)
				}
			}
			release()
			if err != nil {
				return fmt.Errorf("storage nibble[%x] fold: %w", ni, err)
			}
			children[ni] = c
			return nil
		})
		childIdx++
		bm &^= uint16(1) << nib
	}
	if err := g.Wait(); err != nil {
		return cell{}, err
	}

	stitchSplitCells(base, &children, node.bitmap)
	sr, err := foldSplitRow(ctx, base, foldToCell)
	if err != nil {
		return cell{}, fmt.Errorf("storage branch fold: %w", err)
	}
	if deferred := base.TakeDeferredUpdates(); len(deferred) > 0 {
		pu.appendDeferred(deferred)
	}
	if sr.IsEmpty() {
		return cell{}, nil
	}
	return sr, nil
}

func newDeferredStorageWorker(ctx context.Context, accountKeyLen int16, cfg TrieConfig, factory TrieContextFactory, traceW io.Writer) (*HexPatriciaHashed, func()) {
	w := NewHexPatriciaHashed(accountKeyLen, nil, cfg)
	wctx, cleanup := factory(ctx)
	w.ResetContext(wctx)
	w.SetTraceWriter(traceW)
	w.branchEncoder.setDeferUpdates(true)
	w.SetLeaveDeferredForCaller(true)
	return w, func() {
		w.Release()
		if cleanup != nil {
			cleanup()
		}
	}
}

// keyArena copies walk-path nibbles into chunked buffers so each collected key gets a stable slice.
type keyArena struct {
	buf       []byte
	remaining int
}

const keyArenaChunk = 64 * 1024

func (a *keyArena) copy(hk []byte) []byte {
	if len(hk) > cap(a.buf)-len(a.buf) {
		want := len(hk) * max(a.remaining, 1)
		a.buf = make([]byte, 0, max(min(want, keyArenaChunk), len(hk)))
	}
	a.remaining--
	start := len(a.buf)
	a.buf = append(a.buf, hk...)
	return a.buf[start:len(a.buf):len(a.buf)]
}

func collectSubtreeKeys(node *prefixNode, path []byte) []touchedKey {
	out := make([]touchedKey, 0, node.subtreeCount)
	arena := keyArena{remaining: int(node.subtreeCount)}
	_ = dfsSubtree(node, path, func(hk, pk []byte, upd *Update) error {
		out = append(out, touchedKey{hk: arena.copy(hk), pk: pk, upd: upd})
		return nil
	})
	return out
}
