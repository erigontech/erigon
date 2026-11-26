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

package v3

import (
	"bytes"
	"context"
	"math/bits"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

type collapseTestEvent struct {
	sibling []byte
	prefix  []byte
}

func TestCollapseRuleAbsentDelete(t *testing.T) {
	rng := rand.New(rand.NewSource(19))
	pre := witnessState(rng, witnessShape{accounts: 2, accountPrefixes: [][]byte{{1, 2}, {1, 3}}})
	missing := craftKey(rng, 20, []byte{8}, accountNibbles)
	batch := []commitmenttest.Op{commitmenttest.DeleteAt(missing)}
	for _, route := range []string{"untraced", "Process", "TraceCollapsesRoot"} {
		t.Run(route, func(t *testing.T) {
			bed := newWitnessBed(t, pre)
			tr := bed.openV3(t.Context())
			_, _, err := tr.RestoreState(bed.v3State)
			require.NoError(t, err)
			before, err := tr.RootHash()
			require.NoError(t, err)
			updates := testUpdates(t, commitment.ModeCollect, batch)
			trace := func([]byte, []byte) { t.Error("absent delete reported a collapse") }
			var after []byte
			if route == "TraceCollapsesRoot" {
				after, err = tr.TraceCollapsesRoot(t.Context(), updates, trace)
			} else {
				if route == "Process" {
					tr.SetCollapseTracer(trace)
				}
				after, err = tr.Process(t.Context(), updates, "", nil, commitment.WarmupConfig{})
			}
			require.NoError(t, err, "deleting an absent account outside the root extension")
			require.Equal(t, before, after)
		})
	}
}

func collapseRuleEvents(t *testing.T, pre, batch []commitmenttest.Op, reverse bool) []collapseTestEvent {
	t.Helper()
	b := newWitnessBed(t, pre)
	b.v3Mem.Apply(batch)
	tr := b.openV3(context.Background())
	_, _, err := tr.RestoreState(b.v3State)
	require.NoError(t, err)
	items := collapseTestItems(batch)
	if reverse {
		slices.Reverse(items)
	}
	events := make([]collapseTestEvent, 0)
	err = traceCollapsesDeletes(tr.ctx, items, func(sibling, prefix []byte) {
		events = append(events, collapseTestEvent{sibling: bytes.Clone(sibling), prefix: bytes.Clone(prefix)})
	})
	require.NoError(t, err)
	return events
}

func collapseTestItems(batch []commitmenttest.Op) []feedEntry {
	items := make([]feedEntry, 0, len(batch))
	for _, op := range batch {
		items = append(items, feedEntry{
			plainKey:  string(op.Key),
			hashedKey: commitment.KeyToHexNibbleHash(op.Key),
			update:    runner.Update(op),
		})
	}
	return items
}

func accountCollapseEvent(a, b []byte) collapseTestEvent {
	aPath := accountNibbles(a)
	bPath := accountNibbles(b)
	prefix := commonNibblePrefix(aPath, bPath)
	return collapseTestEvent{sibling: bPath, prefix: prefix}
}

func storageCollapseEvent(addr, slot, other []byte) collapseTestEvent {
	addrPath := accountNibbles(addr)
	slotPath := slotNibbles(addr)
	otherPath := slotPath(other)
	return collapseTestEvent{
		sibling: slices.Concat(addrPath, otherPath),
		prefix:  slices.Concat(addrPath, commonNibblePrefix(slotPath(slot), otherPath)),
	}
}

func commonNibblePrefix(a, b []byte) []byte {
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	return slices.Clone(a[:n])
}

func TestDeletesOnlyCollapseShapes(t *testing.T) {
	t.Run("two leaves", func(t *testing.T) {
		pre := witnessState(rand.New(rand.NewSource(1)), witnessShape{
			accounts:        2,
			accountPrefixes: [][]byte{{1, 2}, {1, 3}},
		})
		want := accountCollapseEvent(pre[0].Key, pre[1].Key)
		batch := []commitmenttest.Op{commitmenttest.DeleteAt(pre[0].Key)}
		got := collapseRuleEvents(t, pre, batch, false)
		require.ElementsMatch(t, []collapseTestEvent{want}, got)
	})

	t.Run("three leaves delete two", func(t *testing.T) {
		pre := witnessState(rand.New(rand.NewSource(2)), witnessShape{
			accounts:        3,
			accountPrefixes: [][]byte{{1, 2, 3}, {1, 2, 4}, {1, 2, 8}},
		})
		want := accountCollapseEvent(pre[0].Key, pre[2].Key)
		batch := []commitmenttest.Op{commitmenttest.DeleteAt(pre[0].Key), commitmenttest.DeleteAt(pre[1].Key)}
		got := collapseRuleEvents(t, pre, batch, false)
		require.ElementsMatch(t, []collapseTestEvent{want}, got)
	})

	t.Run("cascade", func(t *testing.T) {
		pre := witnessState(rand.New(rand.NewSource(3)), witnessShape{
			accounts:        3,
			accountPrefixes: [][]byte{{1, 2, 3}, {1, 2, 4}, {1, 8}},
		})
		want := []collapseTestEvent{accountCollapseEvent(pre[0].Key, pre[1].Key), accountCollapseEvent(pre[0].Key, pre[2].Key)}
		batch := []commitmenttest.Op{commitmenttest.DeleteAt(pre[0].Key), commitmenttest.DeleteAt(pre[1].Key)}
		got := collapseRuleEvents(t, pre, batch, false)
		require.ElementsMatch(t, want, got)
	})

	t.Run("branch keeps none", func(t *testing.T) {
		pre := witnessState(rand.New(rand.NewSource(4)), witnessShape{
			accounts:        2,
			accountPrefixes: [][]byte{{1, 2}, {1, 3}},
		})
		want := accountCollapseEvent(pre[0].Key, pre[1].Key)
		batch := []commitmenttest.Op{commitmenttest.DeleteAt(pre[0].Key), commitmenttest.DeleteAt(pre[1].Key)}
		require.ElementsMatch(t, []collapseTestEvent{want}, collapseRuleEvents(t, pre, batch, false))
		require.ElementsMatch(t, []collapseTestEvent{want}, collapseRuleEvents(t, pre, batch, true))
	})

	t.Run("delete and insert before", func(t *testing.T) {
		testDeletesOnlyInsertOrder(t, false)
	})
	t.Run("delete and insert after", func(t *testing.T) {
		testDeletesOnlyInsertOrder(t, true)
	})

	t.Run("storage plane", func(t *testing.T) {
		pre := witnessState(rand.New(rand.NewSource(5)), witnessShape{
			accounts:     1,
			slots:        2,
			slotPrefixes: [][]byte{{1, 2}, {1, 3}},
		})
		addr := pre[0].Key
		want := storageCollapseEvent(addr, pre[1].Key[length.Addr:], pre[2].Key[length.Addr:])
		batch := []commitmenttest.Op{commitmenttest.DeleteAt(pre[1].Key)}
		got := collapseRuleEvents(t, pre, batch, false)
		require.ElementsMatch(t, []collapseTestEvent{want}, got)
	})

	t.Run("account delete with storage", func(t *testing.T) {
		pre := witnessState(rand.New(rand.NewSource(6)), witnessShape{
			accounts:        2,
			slots:           2,
			accountPrefixes: [][]byte{{1, 2}, {1, 3}},
		})
		want := accountCollapseEvent(pre[0].Key, pre[1].Key)
		batch := []commitmenttest.Op{commitmenttest.DeleteAt(pre[0].Key)}
		got := collapseRuleEvents(t, pre, batch, false)
		require.ElementsMatch(t, []collapseTestEvent{want}, got)
	})
}

func testDeletesOnlyInsertOrder(t *testing.T, before bool) {
	t.Helper()
	rng := rand.New(rand.NewSource(7))
	pre := witnessState(rng, witnessShape{
		accounts:        2,
		accountPrefixes: [][]byte{{1, 2}, {1, 3}},
	})
	a, survivor := pre[0].Key, pre[1].Key
	prefix := commonNibblePrefix(accountNibbles(a), accountNibbles(survivor))
	c := relativeAccountKey(rng, prefix, accountNibbles(a), before)
	batch := []commitmenttest.Op{
		commitmenttest.DeleteAt(a),
		{Key: c, Account: randomWitnessAccount(rng)},
	}
	want := []collapseTestEvent{accountCollapseEvent(a, survivor)}
	require.ElementsMatch(t, want, collapseRuleEvents(t, pre, batch, false))
	require.ElementsMatch(t, want, collapseRuleEvents(t, pre, batch, true))
}

func relativeAccountKey(rng *rand.Rand, prefix, pivot []byte, before bool) []byte {
	for {
		key := make([]byte, length.Addr)
		_, _ = rng.Read(key)
		path := accountNibbles(key)
		if !bytes.HasPrefix(path, prefix) {
			continue
		}
		if (bytes.Compare(path, pivot) < 0) == before {
			return key
		}
	}
}

func TestDeletesOnlyCollapseProperty(t *testing.T) {
	for seed := range int64(20) {
		rng := rand.New(rand.NewSource(seed))
		pre := witnessState(rng, witnessShape{
			accounts:        8,
			slots:           5,
			slotPrefixes:    [][]byte{{1}, {2}, {3}, {4}, {5}},
			accountPrefixes: [][]byte{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}},
		})
		var batch []commitmenttest.Op
		for i, op := range pre {
			if i%3 == 0 || rng.Intn(3) == 0 {
				batch = append(batch, commitmenttest.DeleteAt(op.Key))
			}
		}
		got := collapseRuleEvents(t, pre, batch, false)
		want := referenceCollapseEvents(t, pre, batch)
		require.ElementsMatch(t, want, got, "seed %d", seed)
		require.ElementsMatch(t, got, collapseRuleEvents(t, pre, batch, true), "seed %d feed order", seed)
		require.Len(t, got, len(want), "seed %d duplicate events", seed)
	}
}

func TestCollapseRandomBlocksKeepWitnessNodes(t *testing.T) {
	seedCount := 180
	if testing.Short() {
		seedCount = 24
	}
	var legacyMisses, canonicalDiffs int
	for seed := range int64(seedCount) {
		rng := rand.New(rand.NewSource(seed))
		pre := witnessState(rng, witnessShape{accounts: 16, slots: 4})
		bed := newWitnessBed(t, pre)
		batch := collapseBatch(rng, bed)
		var keys witnessKeys
		byKey := make(map[string]bool, len(pre))
		for _, op := range pre {
			byKey[string(op.Key)] = true
		}
		var valid []commitmenttest.Op
		dead := make(map[string]bool)
		touched := make(map[string]bool)
		for _, op := range batch {
			keys.plain = append(keys.plain, op.Key)
			if op.Delete && !byKey[string(op.Key)] {
				continue
			}
			valid = append(valid, op)
			touched[string(op.Key)] = true
			if op.Delete && len(op.Key) == length.Addr {
				dead[string(op.Key)] = true
			}
		}
		debugPlain := slices.Clone(keys.plain)
		for _, slot := range bed.slots {
			if dead[string(slot[:length.Addr])] && !touched[string(slot)] {
				valid = append(valid, commitmenttest.DeleteAt(slot))
				keys.plain = append(keys.plain, slot)
			}
		}
		batch = valid

		v3Events := collapseRuleEvents(t, pre, batch, false)
		hphEvents, hphChildCount := collapseHphEvents(t, pre, batch)
		post := newWitnessBed(t, pre)
		_, root := post.v3Collapses(t, batch, true)
		_, root2 := post.v3Collapses(t, batch, false)
		require.Equal(t, root, root2)
		tr := post.openV3(t.Context())
		read := func(key []byte) ([]byte, error) {
			data, _, err := post.v3Mem.Branch(key)
			return data, err
		}
		v3ChildCount := func(prefix []byte) int {
			childCount, err := tr.BranchChildCount(read, prefix)
			require.NoError(t, err)
			return childCount
		}
		makeKeys := func(plain [][]byte, events []collapseTestEvent, childCount func([]byte) int) witnessKeys {
			result := witnessKeys{plain: slices.Clone(plain)}
			for _, event := range events {
				if childCount != nil && childCount(event.prefix) >= 2 {
					continue
				}
				result.hashed = append(result.hashed, event.sibling)
			}
			return result
		}

		for shape, plain := range map[string][][]byte{"eth_getWitness": keys.plain, "debug_executionWitness": debugPlain} {
			hphLegacy := bed.hphWitness(t, makeKeys(plain, hphEvents, nil), true)
			v3Legacy := bed.v3NativeWitness(t, makeKeys(plain, v3Events, nil), true)
			if missing := countWitnessNodes(hphLegacy, v3Legacy); missing != 0 {
				legacyMisses++
				if legacyMisses <= 2 {
					t.Logf("legacy %s seed %d HPH %d v3 %d missing %d", shape, seed, len(hphLegacy), len(v3Legacy), missing)
				}
			}
			hphCanonical := bed.hphWitness(t, makeKeys(plain, hphEvents, hphChildCount), false)
			v3Canonical := bed.v3NativeWitness(t, makeKeys(plain, v3Events, v3ChildCount), false)
			if !slices.EqualFunc(hphCanonical, v3Canonical, bytes.Equal) {
				canonicalDiffs++
				if canonicalDiffs <= 2 {
					t.Logf("canonical %s seed %d HPH %d v3 %d", shape, seed, len(hphCanonical), len(v3Canonical))
				}
			}
		}
	}
	t.Logf("%d seeds legacy misses=%d canonical HPH diffs=%d", seedCount, legacyMisses, canonicalDiffs)
	require.Zero(t, legacyMisses)
	require.Zero(t, canonicalDiffs)
}

func TestCollapseDeletedSiblingsKeepHphStorageNodes(t *testing.T) {
	pre := witnessState(rand.New(rand.NewSource(8)), witnessShape{
		accounts:        3,
		slots:           1,
		accountPrefixes: [][]byte{{1, 2}, {1, 3}, {8}},
		slotPrefixes:    [][]byte{{1}},
	})
	a, b := pre[0].Key, pre[1].Key
	batch := []commitmenttest.Op{commitmenttest.DeleteAt(a), commitmenttest.DeleteAt(b)}
	for _, op := range pre[3:] {
		if bytes.HasPrefix(op.Key, a) || bytes.HasPrefix(op.Key, b) {
			batch = append(batch, commitmenttest.DeleteAt(op.Key))
		}
	}
	require.Len(t, batch, 4)
	keys := func(events []collapseTestEvent) witnessKeys {
		result := witnessKeys{plain: [][]byte{a, b}}
		for _, event := range events {
			result.hashed = append(result.hashed, event.sibling)
		}
		return result
	}
	hphEvents, _ := collapseHphEvents(t, pre, batch)
	v3Events := collapseRuleEvents(t, pre, batch, false)
	bed := newWitnessBed(t, pre)
	hphLegacy := bed.hphWitness(t, keys(hphEvents), true)
	v3Legacy := bed.v3NativeWitness(t, keys(v3Events), true)
	require.Zero(t, countWitnessNodes(hphLegacy, v3Legacy), "hph events %x, v3 events %x", hphEvents, v3Events)
}

func TestCollapseDeleteRangeCost(t *testing.T) {
	deletes := make([][]byte, 1024)
	for i := range deletes {
		path := make([]byte, 64)
		value := i
		for pos := len(path) - 1; value != 0; pos-- {
			path[pos] = byte(value & 0x0f)
			value >>= 4
		}
		deletes[i] = path
	}
	comparisons := 0
	for _, path := range deletes {
		require.NotEmpty(t, deleteRange(deletes, path, &comparisons))
	}
	t.Logf("1024 deletes comparisons=%d", comparisons)
	require.LessOrEqual(t, comparisons, 32*len(deletes))
}

func collapseHphEvents(t *testing.T, pre, batch []commitmenttest.Op) ([]collapseTestEvent, func([]byte) int) {
	t.Helper()
	bed := newWitnessBed(t, pre)
	bed.hphMem.Apply(batch)
	reader, _ := bed.hphMem.Open(t.Context())
	hph := commitment.NewHexPatriciaHashed(length.Addr, reader, commitment.DefaultTrieConfig())
	require.NoError(t, hph.SetState(bed.hphState))
	events := make([]collapseTestEvent, 0)
	hph.SetCollapseTracer(func(sibling, prefix []byte) {
		events = append(events, collapseTestEvent{sibling: bytes.Clone(sibling), prefix: bytes.Clone(prefix)})
	})
	_, err := hph.Process(t.Context(), testUpdates(t, commitment.ModeUpdate, batch), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	childCount := func(prefix []byte) int {
		data, _, err := bed.hphMem.Branch(nibbles.HexToCompact(prefix))
		require.NoError(t, err)
		return commitment.BranchData(data).ChildCount()
	}
	return events, childCount
}

func countWitnessNodes(want, got [][]byte) int {
	gotSet := make(map[string]struct{}, len(got))
	for _, node := range got {
		gotSet[string(node)] = struct{}{}
	}
	missing := 0
	for _, node := range want {
		if _, ok := gotSet[string(node)]; !ok {
			missing++
		}
	}
	return missing
}

func referenceCollapseEvents(t *testing.T, pre, batch []commitmenttest.Op) []collapseTestEvent {
	t.Helper()
	b := newWitnessBed(t, pre)
	ctx := b.openV3(context.Background()).ctx
	live := make(map[byte]map[string]struct{})
	deletes := make(map[byte]map[string]struct{})
	for _, op := range pre {
		plane := planeAccount
		if len(op.Key) > length.Addr {
			plane = planeStorage
		}
		if live[plane] == nil {
			live[plane] = make(map[string]struct{})
		}
		live[plane][string(commitment.KeyToHexNibbleHash(op.Key))] = struct{}{}
	}
	for _, op := range batch {
		if len(op.Key) == length.Addr {
			if deletes[planeAccount] == nil {
				deletes[planeAccount] = make(map[string]struct{})
			}
			deletes[planeAccount][string(commitment.KeyToHexNibbleHash(op.Key))] = struct{}{}
			for _, preOp := range pre {
				if len(preOp.Key) > length.Addr && bytes.Equal(preOp.Key[:length.Addr], op.Key) {
					delete(live[planeStorage], string(commitment.KeyToHexNibbleHash(preOp.Key)))
				}
			}
			continue
		}
		if deletes[planeStorage] == nil {
			deletes[planeStorage] = make(map[string]struct{})
		}
		deletes[planeStorage][string(commitment.KeyToHexNibbleHash(op.Key))] = struct{}{}
		delete(live[planeStorage], string(commitment.KeyToHexNibbleHash(op.Key)))
	}
	for key := range deletes[planeAccount] {
		delete(live[planeAccount], key)
	}
	groups := make(map[string][][]byte)
	for key := range deletes[planeAccount] {
		groups[""] = append(groups[""], []byte(key))
	}
	for key := range deletes[planeStorage] {
		path := []byte(key)
		if _, deleted := deletes[planeAccount][string(path[:64])]; deleted {
			continue
		}
		groups[string(path[:64])] = append(groups[string(path[:64])], path[64:])
	}
	var result []collapseTestEvent
	for ownerKey, dels := range groups {
		plane := planeAccount
		owner := []byte(nil)
		ownerPath := []byte(nil)
		if ownerKey != "" {
			plane = planeStorage
			ownerPath = []byte(ownerKey)
			packed := hashAddressPath(ownerPath)
			owner = packed[:]
		}
		g := graph{plane: plane, addrHash: owner}
		root, err := g.loadRoot(ctx)
		require.NoError(t, err)
		if root == nil {
			continue
		}
		require.NoError(t, loadAllCollapseNodes(ctx, &g, root))
		planeLive := live[plane]
		if plane == planeStorage {
			planeLive = make(map[string]struct{})
			for key := range live[plane] {
				if bytes.HasPrefix([]byte(key), ownerPath) {
					planeLive[key[len(ownerPath):]] = struct{}{}
				}
			}
		}
		result = append(result, referenceCollapseNodes(root, planeLive, dels, ownerPath)...)
	}
	return result
}

func loadAllCollapseNodes(ctx commitment.PatriciaContext, g *graph, n *node) error {
	for mask := n.childMask &^ n.leafMask; mask != 0; mask &= mask - 1 {
		nib := bits.TrailingZeros16(mask)
		child := n.child(nib)
		if child == nil {
			path := n.childPath(nib, nil)
			var err error
			child, err = g.unfoldChild(ctx, path)
			if err != nil {
				return err
			}
			n.setChild(nib, child)
		}
		if err := loadAllCollapseNodes(ctx, g, child); err != nil {
			return err
		}
	}
	return nil
}

func referenceCollapseNodes(n *node, live map[string]struct{}, deletes [][]byte, ownerPath []byte) []collapseTestEvent {
	if n == nil || n.childMask == 0 {
		return nil
	}
	var result []collapseTestEvent
	for mask := n.childMask &^ n.leafMask; mask != 0; mask &= mask - 1 {
		nib := bits.TrailingZeros16(mask)
		result = append(result, referenceCollapseNodes(n.child(nib), live, deletes, ownerPath)...)
	}
	if bits.OnesCount16(n.childMask) < 2 || !hasDeletedUnder(n.path, deletes) {
		return result
	}
	survivors := make([][]byte, 0, bits.OnesCount16(n.childMask))
	var last []byte
	for mask := n.childMask; mask != 0; mask &= mask - 1 {
		nib := bits.TrailingZeros16(mask)
		childPath := collapseNodeChildPath(n, nib)
		last = childPath
		for key := range live {
			if bytes.HasPrefix([]byte(key), childPath) {
				survivors = append(survivors, childPath)
				break
			}
		}
	}
	if len(survivors) == 0 {
		survivors = append(survivors, last)
	}
	if len(survivors) == 1 {
		result = append(result, collapseTestEvent{sibling: slices.Concat(ownerPath, survivors[0]), prefix: slices.Concat(ownerPath, n.path)})
	}
	return result
}

func hasDeletedUnder(prefix []byte, deletes [][]byte) bool {
	for _, key := range deletes {
		if bytes.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func collapseNodeChildPath(n *node, nib int) []byte {
	if n.leafMask&(uint16(1)<<nib) != 0 {
		suffix, _ := n.leafAt(nib)
		path := append(slices.Clone(n.path), byte(nib))
		return append(path, unpackPath(suffix, 63-len(n.path), nil)...)
	}
	return n.childPath(nib, nil)
}
