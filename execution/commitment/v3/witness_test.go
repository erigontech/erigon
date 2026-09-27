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
	"encoding/hex"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

type witnessBed struct {
	hphMem   *runner.Memory
	hphState []byte
	v3Mem    *runner.Memory
	v3State  []byte
	accounts [][]byte
	slots    [][]byte
}

type witnessKeys struct {
	plain  [][]byte
	hashed [][]byte
}

func (k witnessKeys) touch(u *commitment.Updates) {
	for _, key := range k.plain {
		u.TouchPlainKey(string(key), nil, func(*commitment.KeyUpdate, []byte) {})
	}
	for _, key := range k.hashed {
		u.TouchHashedKey(key)
	}
}

func newWitnessBed(t testing.TB, ops []commitmenttest.Op) *witnessBed {
	t.Helper()
	ctx := context.Background()
	b := &witnessBed{hphMem: runner.NewMemory(runner.ContextSpec{}), v3Mem: runner.NewMemory(runner.ContextSpec{})}
	b.hphMem.Apply(ops)
	b.v3Mem.Apply(ops)
	for _, op := range ops {
		if len(op.Key) == length.Addr {
			b.accounts = append(b.accounts, op.Key)
		} else {
			b.slots = append(b.slots, op.Key)
		}
	}

	reader, _ := b.hphMem.Open(ctx)
	hph := commitment.NewHexPatriciaHashed(length.Addr, reader, commitment.DefaultTrieConfig())
	hu := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
	defer hu.Close()
	for _, op := range ops {
		hu.TouchPlainKeyDirect(string(op.Key), runner.Update(op))
	}
	hphRoot, err := hph.Process(ctx, hu, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	b.hphState, err = hph.EncodeCurrentState(nil)
	require.NoError(t, err)

	tr := b.openV3(ctx)
	vu := commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash)
	defer vu.Close()
	for _, op := range ops {
		vu.TouchPlainKeyDirect(string(op.Key), runner.Update(op))
	}
	v3Root, err := tr.Process(ctx, vu, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Equal(t, hphRoot, v3Root)
	b.v3State, err = tr.EncodeState(1, 1, nil)
	require.NoError(t, err)
	return b
}

func (b *witnessBed) openV3(ctx context.Context) *Trie {
	tr := &Trie{scheduleWorkers: 1}
	reader, _ := b.v3Mem.Open(ctx)
	tr.ResetContext(reader)
	tr.SetTrieContextFactory(b.v3Mem.Open)
	return tr
}

func (b *witnessBed) hphWitness(t testing.TB, keys witnessKeys, exclusion bool) [][]byte {
	t.Helper()
	ctx := context.Background()
	reader, _ := b.hphMem.Open(ctx)
	hph := commitment.NewHexPatriciaHashed(length.Addr, reader, commitment.DefaultTrieConfig())
	require.NoError(t, hph.SetState(b.hphState))
	u := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
	defer u.Close()
	keys.touch(u)
	byHash, proved, root, err := hph.WitnessesByHash(ctx, u, exclusion)
	require.NoError(t, err)
	nodes, err := trie.WitnessNodesForKeysByHash(byHash, root, proved)
	require.NoError(t, err)
	return nodes
}

func (b *witnessBed) v3Witness(t testing.TB, keys witnessKeys, exclusion bool) [][]byte {
	t.Helper()
	ctx := context.Background()
	tr := b.openV3(ctx)
	_, _, err := tr.RestoreState(b.v3State)
	require.NoError(t, err)
	u := commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash)
	defer u.Close()
	keys.touch(u)
	byHash, proved, root, err := tr.WitnessesByHash(ctx, u, exclusion)
	require.NoError(t, err)
	nodes, err := trie.WitnessNodesForKeysByHash(byHash, root, proved)
	require.NoError(t, err)
	return nodes
}

func requireSameNodes(t *testing.T, want, got [][]byte, label string) {
	t.Helper()
	wantSet, gotSet := make(map[string]struct{}, len(want)), make(map[string]struct{}, len(got))
	for _, n := range want {
		wantSet[string(n)] = struct{}{}
	}
	for _, n := range got {
		gotSet[string(n)] = struct{}{}
	}
	var missing, extra []string
	for n := range wantSet {
		if _, ok := gotSet[n]; !ok {
			missing = append(missing, fmt.Sprintf("%d:%x", len(n), []byte(n)))
		}
	}
	for n := range gotSet {
		if _, ok := wantSet[n]; !ok {
			extra = append(extra, fmt.Sprintf("%d:%x", len(n), []byte(n)))
		}
	}
	slices.Sort(missing)
	slices.Sort(extra)
	if len(missing) != 0 || len(extra) != 0 {
		t.Fatalf("%s: hph %d nodes, v3 %d nodes\nmissing from v3: %v\nextra in v3: %v", label, len(want), len(got), missing, extra)
	}
	if len(want) != 0 {
		require.Equal(t, want[0], got[0], "%s: root node first", label)
	}
}

func randomWitnessAccount(rng *rand.Rand) *commitmenttest.AccountValue {
	value := commitmenttest.Account(commitmenttest.AccountSpec{Nonce: uint64(rng.Intn(1 << 20)), Balance: rng.Uint64()})
	value.CodeHash = empty.CodeHash
	if rng.Intn(3) == 0 {
		value.CodeHash = common.BigToHash(uint256.NewInt(rng.Uint64()).ToBig())
	}
	return &value
}

func randomWitnessValue(rng *rand.Rand) []byte {
	value := make([]byte, 1+rng.Intn(32))
	_, _ = rng.Read(value)
	value[0] |= 1
	return value
}

func craftKey(rng *rand.Rand, size int, prefix []byte, hashed func([]byte) []byte) []byte {
	for {
		key := make([]byte, size)
		_, _ = rng.Read(key)
		if bytes.HasPrefix(hashed(key), prefix) {
			return key
		}
	}
}

func accountNibbles(addr []byte) []byte { return commitment.KeyToHexNibbleHash(addr) }

func slotNibbles(addr []byte) func([]byte) []byte {
	return func(slot []byte) []byte {
		return commitment.KeyToHexNibbleHash(append(bytes.Clone(addr), slot...))[64:]
	}
}

type witnessShape struct {
	name            string
	accounts        int
	slots           int
	accountPrefixes [][]byte
	slotPrefixes    [][]byte
	batch           func(*rand.Rand, *witnessBed) []commitmenttest.Op
}

func witnessState(rng *rand.Rand, shape witnessShape) []commitmenttest.Op {
	var ops []commitmenttest.Op
	var addrs [][]byte
	for i := range shape.accounts {
		var prefix []byte
		if len(shape.accountPrefixes) != 0 {
			prefix = shape.accountPrefixes[i%len(shape.accountPrefixes)]
		}
		addr := craftKey(rng, length.Addr, prefix, accountNibbles)
		addrs = append(addrs, addr)
		ops = append(ops, commitmenttest.Op{Key: addr, Account: randomWitnessAccount(rng)})
	}
	for _, addr := range addrs {
		count := shape.slots
		if count > 0 && len(shape.slotPrefixes) == 0 {
			count = rng.Intn(count + 1)
		}
		for j := range count {
			var prefix []byte
			if len(shape.slotPrefixes) != 0 {
				prefix = shape.slotPrefixes[j%len(shape.slotPrefixes)]
			}
			slot := craftKey(rng, length.Hash, prefix, slotNibbles(addr))
			ops = append(ops, commitmenttest.Op{Key: append(bytes.Clone(addr), slot...), Storage: randomWitnessValue(rng)})
		}
	}
	return ops
}

func witnessKeySet(rng *rand.Rand, b *witnessBed) witnessKeys {
	var keys witnessKeys
	for _, addr := range b.accounts {
		if rng.Intn(2) == 0 {
			keys.plain = append(keys.plain, addr)
		}
	}
	for _, slot := range b.slots {
		if rng.Intn(2) == 0 {
			keys.plain = append(keys.plain, slot)
		}
	}
	for range 1 + rng.Intn(3) {
		keys.plain = append(keys.plain, craftKey(rng, length.Addr, nil, accountNibbles))
	}
	for range rng.Intn(4) {
		if len(b.accounts) == 0 {
			break
		}
		near := accountNibbles(b.accounts[rng.Intn(len(b.accounts))])
		keys.plain = append(keys.plain, craftKey(rng, length.Addr, near[:1+rng.Intn(3)], accountNibbles))
	}
	for range rng.Intn(4) {
		if len(b.slots) == 0 {
			break
		}
		slot := b.slots[rng.Intn(len(b.slots))]
		addr := slot[:length.Addr]
		near := slotNibbles(addr)(slot[length.Addr:])
		keys.plain = append(keys.plain, append(bytes.Clone(addr), craftKey(rng, length.Hash, near[:rng.Intn(3)], slotNibbles(addr))...))
	}
	edges := witnessEdgeKeys(rng, b)
	keys.plain = append(keys.plain, edges.plain...)
	keys.hashed = append(keys.hashed, edges.hashed...)
	all := append(slices.Clone(b.accounts), b.slots...)
	for range rng.Intn(4) {
		if len(all) == 0 {
			break
		}
		full := commitment.KeyToHexNibbleHash(all[rng.Intn(len(all))])
		cut := 1 + rng.Intn(len(full)-1)
		partial := bytes.Clone(full[:cut])
		if rng.Intn(2) == 0 {
			partial[cut-1] = byte(rng.Intn(16))
		}
		keys.hashed = append(keys.hashed, partial)
	}
	return keys
}

func witnessEdgeKeys(rng *rand.Rand, b *witnessBed) witnessKeys {
	var keys witnessKeys
	for depth := range 3 {
		if len(b.accounts) == 0 {
			break
		}
		near := accountNibbles(b.accounts[rng.Intn(len(b.accounts))])
		prefix := append(bytes.Clone(near[:depth]), (near[depth]+1+byte(rng.Intn(15)))%16)
		keys.plain = append(keys.plain, craftKey(rng, length.Addr, prefix, accountNibbles))
	}
	for depth := range 3 {
		if len(b.slots) == 0 {
			break
		}
		slot := b.slots[rng.Intn(len(b.slots))]
		addr := slot[:length.Addr]
		near := slotNibbles(addr)(slot[length.Addr:])
		prefix := append(bytes.Clone(near[:depth]), (near[depth]+1+byte(rng.Intn(15)))%16)
		keys.plain = append(keys.plain, append(bytes.Clone(addr), craftKey(rng, length.Hash, prefix, slotNibbles(addr))...))
	}
	all := append(slices.Clone(b.accounts), b.slots...)
	if len(all) != 0 {
		full := commitment.KeyToHexNibbleHash(all[rng.Intn(len(all))])
		for cut := 1; cut <= 8; cut++ {
			keys.hashed = append(keys.hashed, bytes.Clone(full[:cut]))
		}
		if len(full) > 64 {
			for cut := 65; cut <= 72; cut++ {
				keys.hashed = append(keys.hashed, bytes.Clone(full[:cut]))
			}
		}
	}
	return keys
}

func TestWitnessMatchesHPH(t *testing.T) {
	shapes := []witnessShape{
		{name: "single-account", accounts: 1},
		{name: "single-slot", accounts: 1, slots: 1, slotPrefixes: [][]byte{nil}},
		{name: "random", accounts: 24, slots: 4},
		{name: "root-extension", accounts: 3, accountPrefixes: [][]byte{{0x7, 0x2}}},
		{name: "inner-extension", accounts: 6, accountPrefixes: [][]byte{{0x1, 0x2, 0x3}, {0x1, 0x2, 0x3}, {0x4}, {0x9}}},
		{name: "storage-root-extension", accounts: 2, slots: 3, slotPrefixes: [][]byte{{0x5, 0x5}}},
		{name: "storage-inner-extension", accounts: 1, slots: 5, slotPrefixes: [][]byte{{0x3, 0xa, 0x1}, {0x3, 0xa, 0x1}, {0xc}}},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			for seed := range int64(25) {
				rng := rand.New(rand.NewSource(seed))
				b := newWitnessBed(t, witnessState(rng, shape))
				for round := range 4 {
					keys := witnessKeySet(rng, b)
					edges := witnessEdgeKeys(rng, b)
					for _, exclusion := range []bool{false, true} {
						label := fmt.Sprintf("seed=%d round=%d exclusion=%t", seed, round, exclusion)
						requireSameNodes(t, b.hphWitness(t, keys, exclusion), b.v3Witness(t, keys, exclusion), label)
						for _, key := range edges.plain {
							single := witnessKeys{plain: [][]byte{key}}
							requireSameNodes(t, b.hphWitness(t, single, exclusion), b.v3Witness(t, single, exclusion), fmt.Sprintf("%s plain=%x", label, key))
						}
						for _, key := range edges.hashed {
							single := witnessKeys{hashed: [][]byte{key}}
							requireSameNodes(t, b.hphWitness(t, single, exclusion), b.v3Witness(t, single, exclusion), fmt.Sprintf("%s hashed=%x", label, key))
						}
					}
				}
			}
		})
	}
}

func collapseBatch(rng *rand.Rand, b *witnessBed) []commitmenttest.Op {
	var ops []commitmenttest.Op
	deleted := make(map[string]bool)
	for _, addr := range b.accounts {
		switch rng.Intn(10) {
		case 0, 1, 2:
			deleted[string(addr)] = true
			ops = append(ops, commitmenttest.Op{Key: addr, Delete: true})
		case 3, 4:
			ops = append(ops, commitmenttest.Op{Key: addr, Account: randomWitnessAccount(rng)})
		}
	}
	for _, slot := range b.slots {
		dead := deleted[string(slot[:length.Addr])]
		switch n := rng.Intn(10); {
		case n < 4:
			ops = append(ops, commitmenttest.Op{Key: slot, Delete: true})
		case n < 6 && !dead:
			ops = append(ops, commitmenttest.Op{Key: slot, Storage: randomWitnessValue(rng)})
		}
	}
	for range rng.Intn(4) {
		var prefix []byte
		if len(b.accounts) != 0 && rng.Intn(2) == 0 {
			near := accountNibbles(b.accounts[rng.Intn(len(b.accounts))])
			prefix = near[:1+rng.Intn(2)]
		}
		ops = append(ops, commitmenttest.Op{Key: craftKey(rng, length.Addr, prefix, accountNibbles), Account: randomWitnessAccount(rng)})
	}
	for range rng.Intn(3) {
		var prefix []byte
		if len(b.accounts) != 0 && rng.Intn(2) == 0 {
			near := accountNibbles(b.accounts[rng.Intn(len(b.accounts))])
			prefix = near[:1+rng.Intn(3)]
		}
		ops = append(ops, commitmenttest.Op{Key: craftKey(rng, length.Addr, prefix, accountNibbles), Delete: true})
	}
	for range rng.Intn(4) {
		if len(b.accounts) == 0 {
			break
		}
		addr := b.accounts[rng.Intn(len(b.accounts))]
		if deleted[string(addr)] {
			continue
		}
		var prefix []byte
		if len(b.slots) != 0 && rng.Intn(2) == 0 {
			slot := b.slots[rng.Intn(len(b.slots))]
			if bytes.Equal(slot[:length.Addr], addr) {
				prefix = slotNibbles(addr)(slot[length.Addr:])[:1+rng.Intn(2)]
			}
		}
		ops = append(ops, commitmenttest.Op{Key: append(bytes.Clone(addr), craftKey(rng, length.Hash, prefix, slotNibbles(addr))...), Storage: randomWitnessValue(rng)})
	}
	return ops
}

func (b *witnessBed) hphCollapses(t *testing.T, batch []commitmenttest.Op) ([]string, []byte) {
	t.Helper()
	ctx := context.Background()
	b.hphMem.Apply(batch)
	reader, _ := b.hphMem.Open(ctx)
	hph := commitment.NewHexPatriciaHashed(length.Addr, reader, commitment.DefaultTrieConfig())
	require.NoError(t, hph.SetState(b.hphState))
	var events []string
	hph.SetCollapseTracer(func(sibling, prefix []byte) { events = append(events, fmt.Sprintf("%x/%x", sibling, prefix)) })
	u := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
	defer u.Close()
	for _, op := range batch {
		u.TouchPlainKeyDirect(string(op.Key), runner.Update(op))
	}
	root, err := hph.Process(ctx, u, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	return events, root
}

func (b *witnessBed) v3Collapses(t *testing.T, batch []commitmenttest.Op) ([]string, []byte) {
	t.Helper()
	ctx := context.Background()
	b.v3Mem.Apply(batch)
	tr := b.openV3(ctx)
	_, _, err := tr.RestoreState(b.v3State)
	require.NoError(t, err)
	var events []string
	tr.SetCollapseTracer(func(sibling, prefix []byte) { events = append(events, fmt.Sprintf("%x/%x", sibling, prefix)) })
	u := commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash)
	defer u.Close()
	for _, op := range batch {
		u.TouchPlainKeyDirect(string(op.Key), runner.Update(op))
	}
	root, err := tr.Process(ctx, u, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	return events, root
}

func TestCollapseTracerMatchesHPH(t *testing.T) {
	shapes := []witnessShape{
		{name: "single-account", accounts: 1},
		{name: "single-slot", accounts: 1, slots: 1, slotPrefixes: [][]byte{nil}},
		{name: "random", accounts: 24, slots: 4},
		{name: "root-extension", accounts: 3, accountPrefixes: [][]byte{{0x7, 0x2}}},
		{name: "inner-extension", accounts: 6, accountPrefixes: [][]byte{{0x1, 0x2, 0x3}, {0x1, 0x2, 0x3}, {0x4}, {0x9}}},
		{name: "storage-root-extension", accounts: 2, slots: 3, slotPrefixes: [][]byte{{0x5, 0x5}}},
		{name: "storage-inner-extension", accounts: 1, slots: 5, slotPrefixes: [][]byte{{0x3, 0xa, 0x1}, {0x3, 0xa, 0x1}, {0xc}}},
		{name: "propagate", accounts: 3, accountPrefixes: [][]byte{{0x2, 0x1}, {0x2, 0x9}, {0x8}}},
		{name: "propagate-deep", accounts: 5, accountPrefixes: [][]byte{{0x2, 0x1}, {0x2, 0x9}, {0x8, 0x1}, {0x8, 0x5}, {0xc}}},
		{name: "storage-propagate", accounts: 1, slots: 3, slotPrefixes: [][]byte{{0x2, 0x1}, {0x2, 0x9}, {0x8}}},
		{
			name: "storage-delete-without-storage", accounts: 3, accountPrefixes: [][]byte{{0x6, 0x1}, {0x6, 0x9}, {0xb}},
			batch: func(rng *rand.Rand, b *witnessBed) []commitmenttest.Op {
				addr := b.accounts[0]
				return []commitmenttest.Op{{Key: append(bytes.Clone(addr), craftKey(rng, length.Hash, nil, slotNibbles(addr))...), Delete: true}}
			},
		},
		{
			name: "split-then-propagate", accounts: 4, accountPrefixes: [][]byte{{0x1, 0x2, 0x1}, {0x1, 0x8}, {0x1, 0x9}, {0x5}},
			batch: func(rng *rand.Rand, b *witnessBed) []commitmenttest.Op {
				return []commitmenttest.Op{
					{Key: craftKey(rng, length.Addr, []byte{0x1, 0x2, 0x5}, accountNibbles), Account: randomWitnessAccount(rng)},
					{Key: b.accounts[1], Delete: true},
					{Key: b.accounts[2], Delete: true},
					{Key: b.accounts[3], Delete: true},
				}
			},
		},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			for seed := range int64(40) {
				rng := rand.New(rand.NewSource(seed))
				pre := witnessState(rng, shape)
				batch := collapseBatch(rng, newWitnessBed(t, pre))
				if shape.batch != nil {
					batch = shape.batch(rng, newWitnessBed(t, pre))
				}
				b := newWitnessBed(t, pre)
				wantEvents, wantRoot := b.hphCollapses(t, batch)
				gotEvents, gotRoot := b.v3Collapses(t, batch)
				label := fmt.Sprintf("seed=%d batch=%d ops", seed, len(batch))
				require.Equal(t, wantRoot, gotRoot, label)
				require.Equal(t, wantEvents, gotEvents, label)
				requireSameChildCounts(t, b, pre, batch, wantEvents, label)
			}
		})
	}
}

func requireSameChildCounts(t *testing.T, b *witnessBed, pre, batch []commitmenttest.Op, events []string, label string) {
	t.Helper()
	hphRecords, v3Records := b.hphMem.Records(), b.v3Mem.Records()
	read := func(key []byte) ([]byte, error) { return v3Records[string(key)], nil }
	tr := &Trie{}
	check := func(prefix []byte) {
		want := commitment.BranchData(hphRecords[string(nibbles.HexToCompact(prefix))]).ChildCount()
		got, err := tr.BranchChildCount(read, prefix)
		require.NoError(t, err)
		require.Equal(t, want, got, "%s prefix=%x", label, prefix)
	}
	for _, event := range events {
		prefix, err := hex.DecodeString(event[strings.IndexByte(event, '/')+1:])
		require.NoError(t, err)
		check(prefix)
	}
	state := make(commitmenttest.State)
	state.Apply(pre)
	state.Apply(batch)
	for _, op := range state.Ops() {
		full := commitment.KeyToHexNibbleHash(op.Key)
		for cut := 0; cut <= len(full); cut++ {
			check(full[:cut])
		}
	}
}
