package commitment

import (
	"context"
	"errors"
	"fmt"
	"runtime"
)

func parallelMountConcurrency(numWorkers int) int {
	return min(numWorkers, max(1, runtime.GOMAXPROCS(0)))
}

func (hph *HexPatriciaHashed) mountTo(base *HexPatriciaHashed, nibble int) {
	hph.rootTouched = false
	hph.rootChecked = false
	hph.rootPresent = true

	hph.root = base.root

	fork := max(base.activeRows-1, 0)
	hph.activeRows = min(base.activeRows, 1)
	hph.currentKeyLen = base.currentKeyLen
	copy(hph.currentKey[:], base.currentKey[:])
	hph.depths[0] = base.depths[fork]
	hph.branchBefore[0] = base.branchBefore[fork]
	hph.touchMap[0] = base.touchMap[fork]
	hph.afterMap[0] = base.afterMap[fork]
	copy(hph.depthsToTxNum[:], base.depthsToTxNum[:])

	// The clone did not read this row; the base did, so it holds no record for it.
	hph.rowBranch[0] = hph.rowBranch[0][:0]

	hph.mountedNib = nibble
	hph.mounted = true
	hph.mountWall = base.currentKeyLen + 1
	hph.grid[0] = base.grid[fork]
}

func (p *ParallelPatriciaHashed) processMounted(ctx context.Context, updates *Updates) ([]byte, baseSnapshot, error) {
	pu := updates.parallel
	base := p.template
	if base == nil {
		return nil, baseSnapshot{}, errors.New("processMounted: nil template")
	}
	if base.ctx == nil && p.trieCtxFactory != nil {
		bctx, cleanup := p.trieCtxFactory(ctx)
		if cleanup != nil {
			defer cleanup()
		}
		base.ResetContext(bctx)
	}
	base.branchEncoder.setDeferUpdates(true)
	base.SetLeaveDeferredForCaller(true)
	base.resetFoldFrontier()
	if len(base.branchEncoder.deferred) > 0 {
		base.branchEncoder.ClearDeferred()
	}
	base.metrics.Reset()
	saved := snapshotBase(base)

	concurrency := parallelMountConcurrency(p.numWorkers)
	leases := newCtxLeasePool(ctx, p.trieCtxFactory, concurrency)
	defer leases.close()
	fw := &forkWalk{
		leases:        leases,
		accountKeyLen: p.accountKeyLen,
		cfg:           p.cfg,
		pu:            pu,
		metrics:       p.metrics,
		traceW:        base.traceW,
		grain:         p.grainFor(updates.Size(), concurrency),
	}
	bw := &walker{trie: base}
	if err := fw.attach(ctx, bw); err != nil {
		saved.restore(base)
		return nil, saved, err
	}
	root := pu.trie.root
	path := make([]byte, 0, forkPathCap)
	path = append(path, root.ext...)
	walkErr := fw.walk(ctx, bw, root, path)
	fw.detach(bw)
	p.forks.Store(fw.forks.Load())
	if walkErr != nil {
		saved.restore(base)
		return nil, saved, fmt.Errorf("processMounted: %w", walkErr)
	}

	if _, err := foldSplitRow(ctx, base); err != nil {
		saved.restore(base)
		return nil, saved, fmt.Errorf("processMounted: root fold: %w", err)
	}
	p.metrics.Merge(base.metrics)
	pu.appendDeferred(base.TakeDeferredUpdates())
	rh, err := base.RootHash()
	if err != nil {
		saved.restore(base)
		return nil, saved, err
	}
	return rh, saved, nil
}

type baseSnapshot struct {
	root        cell
	rootTouched bool
	rootChecked bool
	rootPresent bool
}

func snapshotBase(base *HexPatriciaHashed) baseSnapshot {
	return baseSnapshot{
		root:        base.root,
		rootTouched: base.rootTouched,
		rootChecked: base.rootChecked,
		rootPresent: base.rootPresent,
	}
}

func (s baseSnapshot) restore(base *HexPatriciaHashed) {
	base.branchEncoder.ClearDeferred()
	base.root = s.root
	base.rootTouched = s.rootTouched
	base.rootChecked = s.rootChecked
	base.rootPresent = s.rootPresent
	base.activeRows = 0
	base.currentKeyLen = 0
}
