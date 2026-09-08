// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package commitment

import (
	"errors"
	"strings"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func fakeReader(tree syntheticTree, notFound map[string]bool, valSz int, failOnPath string) CommitmentReader {
	return func(prefix []byte) ([]byte, uint64, bool, error) {
		path := string(nibbles.CompactToHex(prefix))
		if failOnPath != "" && path == failOnPath {
			return nil, 0, false, errors.New("synthetic reader failure")
		}
		am, ok := tree[path]
		if !ok || notFound[path] {
			return nil, 0, false, nil
		}
		return branchVal(am, valSz), 1, true, nil
	}
}

func TestContractTrunkPreload_ResumeAfterReaderError(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	root := ""
	for p := range tree {
		if root == "" || len(p) < len(root) {
			root = p
		}
	}
	const valSz = 100
	r1 := root + string([]byte{1})
	failingReader := fakeReader(tree, nil, valSz, r1)
	healthyReader := fakeReader(tree, nil, valSz, "")

	c := NewBranchCache(64)
	p, err := NewContractTrunkPreload(hash)
	if err != nil {
		t.Fatal(err)
	}

	n, done, err := p.Run(1<<20, failingReader, c, nil)
	if err == nil {
		t.Fatal("expected reader error")
	}
	if done {
		t.Fatal("Run with a reader error should report done=false")
	}
	if n != 1 {
		t.Fatalf("newlyPinned %d, want 1 (root pinned before R1's failure)", n)
	}
	if p.PinnedTotal() != 1 {
		t.Fatalf("PinnedTotal() %d, want 1: the root's pin must be accounted despite the error", p.PinnedTotal())
	}
	rootCost := estimatedEntryOverheadBytes + len(nibbles.HexToCompact([]byte(root))) + valSz
	if p.UsedBytes() != rootCost {
		t.Fatalf("UsedBytes() %d, want %d", p.UsedBytes(), rootCost)
	}
	if p.QueueRemaining() == 0 {
		t.Fatal("queue drained on error: the failing entry and its subtree were lost")
	}
	if got := string(p.queue[0].path); got != r1 {
		t.Fatalf("queue head = %x, want the failing entry %x (retryable)", got, r1)
	}

	if _, done, err := p.Run(1<<20, healthyReader, c, nil); err != nil {
		t.Fatalf("retry failed: %v", err)
	} else if !done {
		t.Fatalf("expected done after retry; queue=%d", p.QueueRemaining())
	}
	if p.PinnedTotal() != len(tree) {
		t.Fatalf("pinned %d, want the whole tree (%d)", p.PinnedTotal(), len(tree))
	}
}

func TestContractTrunkPreload_BudgetStopPreservesQueueHead(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	const valSz = 100
	order := breadthFirstOrder(tree, nil) // 7 entries, by depth then key
	want := 3
	budget := 0
	for i := range want {
		budget += estimatedEntryOverheadBytes + len(nibbles.HexToCompact([]byte(order[i]))) + valSz
	}
	budget += 10 // slack inside the want-th, below the (want+1)-th

	c := NewBranchCache(64)
	p, err := NewContractTrunkPreload(hash)
	if err != nil {
		t.Fatal(err)
	}
	reader := fakeReader(tree, nil, valSz, "")

	n, done, err := p.Run(budget, reader, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("expected not done: the budget stops short of the whole tree")
	}
	if n != want {
		t.Fatalf("pinned %d, want %d", n, want)
	}
	if p.QueueRemaining() == 0 {
		t.Fatal("queue drained: the unaffordable entry was lost")
	}
	if got := string(p.queue[0].path); got != order[want] {
		t.Fatalf("queue head = %x, want the unaffordable entry %x", got, order[want])
	}

	if _, done, err := p.Run(1<<20, reader, c, nil); err != nil {
		t.Fatal(err)
	} else if !done {
		t.Fatalf("expected done after a large follow-on budget; queue=%d", p.QueueRemaining())
	}
	if p.PinnedTotal() != len(tree) {
		t.Fatalf("pinned %d, want the whole tree (%d)", p.PinnedTotal(), len(tree))
	}
}

func TestPreloadContractTrunk_NilCacheWithLoggerReturnsError(t *testing.T) {
	hash := make([]byte, 32)
	reader := func(prefix []byte) ([]byte, uint64, bool, error) { return nil, 0, false, nil }

	_, err := PreloadContractTrunk(hash, 1<<20, reader, nil, log.Root())
	if err == nil {
		t.Fatal("expected error when cache is nil")
	}
	if !strings.Contains(err.Error(), "cache is nil") {
		t.Fatalf("error %q, want it to name the nil cache", err)
	}
}

func TestContractTrunkPreload_StopsAtMaxDepth(t *testing.T) {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = byte(i)
	}
	endless := func(prefix []byte) ([]byte, uint64, bool, error) {
		v := make([]byte, 8)
		v[3] = 1 // bitmap: child at nibble 0
		return v, 1, true, nil
	}

	c := NewBranchCache(1 << 16)
	p, err := NewContractTrunkPreload(hash)
	if err != nil {
		t.Fatal(err)
	}
	for range 4096 {
		if _, done, err := p.Run(1<<20, endless, c, nil); err != nil {
			t.Fatal(err)
		} else if done {
			break
		}
	}
	if p.MaxDepthReached() > maxStorageTrunkDepth {
		t.Errorf("maxDepthReached=%d, must not exceed maxStorageTrunkDepth=%d", p.MaxDepthReached(), maxStorageTrunkDepth)
	}
	if p.QueueRemaining() != 0 {
		t.Errorf("queue must drain once the ceiling is reached, got %d remaining", p.QueueRemaining())
	}
}
