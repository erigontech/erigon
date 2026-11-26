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
	"fmt"
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

func collapseEventKey(event collapseTestEvent) string {
	return fmt.Sprintf("%x/%x", event.sibling, event.prefix)
}

func collapseEventSet(events []collapseTestEvent) map[string]struct{} {
	set := make(map[string]struct{}, len(events))
	for _, event := range events {
		set[collapseEventKey(event)] = struct{}{}
	}
	return set
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
		require.Equal(t, map[string]struct{}{collapseEventKey(want): {}}, collapseEventSet(got))
	})

	t.Run("three leaves delete two", func(t *testing.T) {
		pre := witnessState(rand.New(rand.NewSource(2)), witnessShape{
			accounts:        3,
			accountPrefixes: [][]byte{{1, 2, 3}, {1, 2, 4}, {1, 2, 8}},
		})
		want := accountCollapseEvent(pre[0].Key, pre[2].Key)
		batch := []commitmenttest.Op{commitmenttest.DeleteAt(pre[0].Key), commitmenttest.DeleteAt(pre[1].Key)}
		got := collapseRuleEvents(t, pre, batch, false)
		require.Equal(t, map[string]struct{}{collapseEventKey(want): {}}, collapseEventSet(got))
	})

	t.Run("cascade", func(t *testing.T) {
		pre := witnessState(rand.New(rand.NewSource(3)), witnessShape{
			accounts:        3,
			accountPrefixes: [][]byte{{1, 2, 3}, {1, 2, 4}, {1, 8}},
		})
		want := accountCollapseEvent(pre[0].Key, pre[2].Key)
		batch := []commitmenttest.Op{commitmenttest.DeleteAt(pre[0].Key), commitmenttest.DeleteAt(pre[1].Key)}
		got := collapseRuleEvents(t, pre, batch, false)
		require.Equal(t, map[string]struct{}{collapseEventKey(want): {}}, collapseEventSet(got))
	})

	t.Run("branch keeps none", func(t *testing.T) {
		pre := witnessState(rand.New(rand.NewSource(4)), witnessShape{
			accounts:        2,
			accountPrefixes: [][]byte{{1, 2}, {1, 3}},
		})
		batch := []commitmenttest.Op{commitmenttest.DeleteAt(pre[0].Key), commitmenttest.DeleteAt(pre[1].Key)}
		require.Empty(t, collapseRuleEvents(t, pre, batch, false))
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
		require.Equal(t, map[string]struct{}{collapseEventKey(want): {}}, collapseEventSet(got))
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
		require.Equal(t, map[string]struct{}{collapseEventKey(want): {}}, collapseEventSet(got))
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
		{Key: c, Account: func() *commitmenttest.AccountValue {
			account := commitmenttest.RandomAccount(rng)
			return &account
		}()},
	}
	want := map[string]struct{}{collapseEventKey(accountCollapseEvent(a, survivor)): {}}
	require.Equal(t, want, collapseEventSet(collapseRuleEvents(t, pre, batch, false)))
	require.Equal(t, want, collapseEventSet(collapseRuleEvents(t, pre, batch, true)))
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
		if len(batch) == 0 {
			batch = append(batch, commitmenttest.DeleteAt(pre[0].Key))
		}
		got := collapseRuleEvents(t, pre, batch, false)
		want := referenceCollapseEvents(t, pre, batch)
		gotSet, wantSet := collapseEventSet(got), collapseEventSet(want)
		require.Equal(t, wantSet, gotSet, "seed %d", seed)
		require.Equal(t, gotSet, collapseEventSet(collapseRuleEvents(t, pre, batch, true)), "seed %d feed order", seed)
		require.Len(t, gotSet, len(got), "seed %d duplicate events", seed)
	}
}

func TestDeletesOnlyCollapseComparison(t *testing.T) {
	var equal, superset, differ int
	var first string
	for seed := range int64(20) {
		rng := rand.New(rand.NewSource(seed + 100))
		pre := witnessState(rng, witnessShape{
			accounts:     8,
			slots:        4,
			slotPrefixes: [][]byte{{1}, {2}, {3}, {4}},
		})
		batch := make([]commitmenttest.Op, 0)
		for i, op := range pre {
			if i%2 == 0 || rng.Intn(3) == 0 {
				batch = append(batch, commitmenttest.DeleteAt(op.Key))
			}
		}
		b := newWitnessBed(t, pre)
		b.v3Mem.Apply(batch)
		tr := b.openV3(context.Background())
		_, _, err := tr.RestoreState(b.v3State)
		require.NoError(t, err)
		items := collapseTestItems(batch)
		var newEvents, oldEvents []collapseTestEvent
		err = traceCollapsesDeletes(tr.ctx, items, func(sibling, prefix []byte) {
			newEvents = append(newEvents, collapseTestEvent{bytes.Clone(sibling), bytes.Clone(prefix)})
		})
		require.NoError(t, err)
		err = traceCollapses(tr.ctx, items, func(sibling, prefix []byte) {
			oldEvents = append(oldEvents, collapseTestEvent{bytes.Clone(sibling), bytes.Clone(prefix)})
		})
		require.NoError(t, err)
		newSet, oldSet := collapseEventSet(newEvents), collapseEventSet(oldEvents)
		switch {
		case equalEventSets(newSet, oldSet):
			equal++
		case isEventSuperset(newSet, oldSet):
			superset++
		default:
			differ++
			if first == "" {
				first = fmt.Sprintf("seed=%d batch=%d new=%d old=%d", seed, len(batch), len(newSet), len(oldSet))
			}
		}
	}
	t.Logf("deletes-only comparison: equal=%d superset=%d differ=%d", equal, superset, differ)
	if first != "" {
		t.Logf("first differ otherwise: %s", first)
	}
}

func equalEventSets(a, b map[string]struct{}) bool {
	return isEventSuperset(a, b) && isEventSuperset(b, a)
}

func isEventSuperset(a, b map[string]struct{}) bool {
	for event := range b {
		if _, ok := a[event]; !ok {
			return false
		}
	}
	return true
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
	var result []collapseTestEvent
	for plane, keys := range deletes {
		if len(keys) == 0 {
			continue
		}
		owners := map[string][]byte{"": nil}
		if plane == planeStorage {
			owners = make(map[string][]byte)
			for key := range keys {
				path := []byte(key)
				owner := hashAddressPath(path[:64])
				owners[string(owner[:])] = owner[:]
			}
		}
		for ownerKey, owner := range owners {
			g := graph{plane: plane, addrHash: owner}
			root, err := g.loadRoot(ctx)
			require.NoError(t, err)
			if root == nil {
				continue
			}
			require.NoError(t, loadAllCollapseNodes(ctx, &g, root))
			dels := make([][]byte, 0)
			for key := range keys {
				path := []byte(key)
				if plane == planeStorage {
					pathOwner := hashAddressPath(path[:64])
					if string(pathOwner[:]) != ownerKey {
						continue
					}
					path = path[64:]
				}
				dels = append(dels, path)
			}
			ownerPath := []byte(nil)
			if plane == planeStorage {
				ownerPath = make([]byte, 64)
				nibbles.Expand(owner, ownerPath)
			}
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
	for mask := n.childMask; mask != 0; mask &= mask - 1 {
		nib := bits.TrailingZeros16(mask)
		childPath := collapseNodeChildPath(n, nib)
		for key := range live {
			if bytes.HasPrefix([]byte(key), childPath) {
				survivors = append(survivors, childPath)
				break
			}
		}
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
