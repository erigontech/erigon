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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

func parityAccount(key []byte, i int) commitmenttest.Op {
	return commitmenttest.AccountAt(key, commitmenttest.AccountSpec{Kind: "parity", Number: i})
}

func parityAddress(i int) []byte {
	return commitmenttest.Key(commitmenttest.KeySpec{Kind: "wrapping-address", Size: 20}, i)
}

func paritySlot(i int) []byte {
	return commitmenttest.Key(commitmenttest.KeySpec{Kind: "wrapping-slot", Size: 32}, i)
}

func incrAddress(i int) []byte {
	return commitmenttest.Key(commitmenttest.KeySpec{Kind: "integer", Size: 20}, i)
}

func incrSlot(addr []byte, j int) []byte {
	return commitmenttest.SlotKey(addr, commitmenttest.Key(commitmenttest.KeySpec{Kind: "integer", Size: 32}, j))
}

var (
	parityEngines = []runner.RunSpec{{Name: "v3", Mode: commitment.ModeCollect}, {Name: "hph", Mode: commitment.ModeUpdate}, {Name: "parallel", Mode: commitment.ModeParallel}}
	serialEngines = []runner.RunSpec{{Name: "v3", Mode: commitment.ModeCollect, Workers: 1}, {Name: "hph", Mode: commitment.ModeUpdate}}
)

func differential(t *testing.T, c commitmenttest.Case, specs []runner.RunSpec) ([][]byte, map[string][]byte, []int) {
	t.Helper()
	c.Assertions = commitmenttest.Assertions{TolerateHPHDrift: c.Assertions.TolerateHPHDrift, ZeroAccountReads: true, ZeroStorageReads: true, StateReadEngines: []string{"v3", "v3-reload", "fresh-v3"}}
	v3 := specs[0]
	v3.Context.CheckPrevious = true
	modes := []runner.RunSpec{v3, {Name: "fresh-v3", Mode: commitment.ModeCollect, Workers: 1, Fresh: true, Context: runner.ContextSpec{CheckPrevious: true}}, {Name: "fresh-hph", Mode: commitment.ModeUpdate, Fresh: true}}
	got := runner.Compare(t, c, append(modes, specs[1:]...), openTestTrie)
	roots := make([][]byte, len(c.Rounds))
	state := make(commitmenttest.State)
	for i := range got[0].Rounds {
		round := &got[0].Rounds[i]
		roots[i] = round.Root
		state.Apply(c.Rounds[i])
		label := fmt.Sprintf("case=%s seed=%+v round=%d", c.ID, c.Seed, i)
		checkDifferentialRecords(t, state, round.Records, label)
		requireLiveRecords(t, got[1].Rounds[i].Records, round.Records, label)
	}
	var hphDrift []int
	for _, run := range got {
		hphDrift = append(hphDrift, run.HPHDrift...)
	}
	return roots, got[0].Rounds[len(c.Rounds)-1].Records, hphDrift
}

func withInitial(initial, ops []commitmenttest.Op) [][]commitmenttest.Op {
	if len(initial) == 0 {
		return [][]commitmenttest.Op{ops}
	}
	return [][]commitmenttest.Op{initial, ops}
}

func TestDifferential(t *testing.T) {
	parity := func(i int) commitmenttest.Op {
		return commitmenttest.AccountAt(parityAddress(i), commitmenttest.AccountSpec{Kind: "parity", Number: i})
	}
	t.Run("wide_round_without_factory", func(t *testing.T) {
		var initial, ops []commitmenttest.Op
		for i := range 4096 {
			initial = append(initial, commitmenttest.AccountAt(incrAddress(i), commitmenttest.AccountSpec{Kind: "parity", Number: i}))
			switch {
			case i%7 == 0:
				ops = append(ops, commitmenttest.DeleteAt(incrAddress(i)))
			case i%3 == 0:
				ops = append(ops, commitmenttest.StorageAt(incrSlot(incrAddress(i), i), i+1))
			default:
				ops = append(ops, commitmenttest.AccountAt(incrAddress(i), commitmenttest.AccountSpec{Kind: "parity", Number: i + 8192}))
			}
		}
		for i := 4096; i < 4096+512; i++ {
			ops = append(ops, commitmenttest.AccountAt(incrAddress(i), commitmenttest.AccountSpec{Kind: "parity", Number: i}))
		}
		engines := []runner.RunSpec{{Name: "v3", Mode: commitment.ModeCollect, Workers: 4, NoContextFactory: true}, {Name: "hph", Mode: commitment.ModeUpdate}}
		differential(t, commitmenttest.Case{ID: "wide_round_without_factory", Rounds: [][]commitmenttest.Op{initial, ops}}, engines)
	})

	t.Run("delete_storage_subtree", func(t *testing.T) {
		wiped, kept := parityAddress(1), parityAddress(2)
		initial := []commitmenttest.Op{parity(1), parity(2)}
		for i := range 300 {
			initial = append(initial, commitmenttest.StorageAt(incrSlot(wiped, i), i+1), commitmenttest.StorageAt(incrSlot(kept, i), i+1))
		}
		recreate := func(number int) commitmenttest.Op {
			op := commitmenttest.AccountAt(wiped, commitmenttest.AccountSpec{Kind: "parity", Number: number})
			op.DeleteStorageSubtree = true
			return op
		}
		rounds := [][]commitmenttest.Op{
			initial,
			{recreate(1001), commitmenttest.StorageAt(incrSlot(wiped, 3), 7), commitmenttest.StorageAt(incrSlot(wiped, 500), 8)},
			{recreate(1002)},
			{commitmenttest.StorageAt(incrSlot(wiped, 9), 9)},
			{recreate(1003), commitmenttest.DeleteAt(incrSlot(wiped, 9))},
		}
		want, _, _ := differential(t, commitmenttest.Case{ID: "delete_storage_subtree", Rounds: rounds}, parityEngines)
		for _, deferred := range []bool{false, true} {
			require.Equal(t, want, requireSameRuns(t, nil, v3Config{deferred: deferred}, v3Config{deferred: deferred, feed: true}, rounds...))
		}
	})

	t.Run("bulk", func(t *testing.T) {
		for _, kind := range []string{"accounts", "storage", "mixed"} {
			for _, count := range []int{1, 2, 16, 1000, 100000} {
				var initial, ops []commitmenttest.Op
				for i := range count {
					slot := commitmenttest.StorageAt(commitmenttest.SlotKey(parityAddress(i), paritySlot(i)), i)
					switch kind {
					case "accounts":
						ops = append(ops, parity(i))
					case "storage":
						initial, ops = append(initial, parity(i)), append(ops, slot)
					default:
						ops = append(ops, parity(i), slot)
					}
				}
				differential(t, commitmenttest.Case{ID: fmt.Sprintf("%s/%d", kind, count), Rounds: withInitial(initial, ops)}, parityEngines)
			}
		}
	})

	t.Run("correctness", func(t *testing.T) {
		address := parityAddress(7)
		slotA, slotB := commitmenttest.SlotKey(address, paritySlot(8)), commitmenttest.SlotKey(address, paritySlot(9))
		account := []commitmenttest.Op{parity(7)}
		fields := func(spec commitmenttest.AccountSpec) commitmenttest.Op {
			spec.CodeHash = empty.CodeHash
			return commitmenttest.AccountAt(address, spec)
		}
		for _, tc := range []struct {
			name         string
			initial, ops []commitmenttest.Op
		}{
			{"account", nil, account},
			{"slot_after_account", account, []commitmenttest.Op{commitmenttest.StorageAt(slotA, 8)}},
			{"two_slots_after_account", account, []commitmenttest.Op{commitmenttest.StorageAt(slotA, 8), commitmenttest.StorageAt(slotB, 9)}},
			{"account_with_slot", nil, []commitmenttest.Op{parity(7), commitmenttest.StorageAt(slotA, 8)}},
			{"account_with_other_slot", nil, []commitmenttest.Op{commitmenttest.AccountAt(address, commitmenttest.AccountSpec{Kind: "parity", Number: 12}), commitmenttest.StorageAt(slotB, 13)}},
			{"delete_absent_account", nil, []commitmenttest.Op{commitmenttest.DeleteAt(address)}},
			{"zeroed_account", nil, []commitmenttest.Op{fields(commitmenttest.AccountSpec{})}},
			{"zeroed_account_with_neighbour", nil, []commitmenttest.Op{fields(commitmenttest.AccountSpec{}), parity(9)}},
			{"nonce_only", nil, []commitmenttest.Op{fields(commitmenttest.AccountSpec{Nonce: 1})}},
			{"balance_only", nil, []commitmenttest.Op{fields(commitmenttest.AccountSpec{Balance: 5})}},
		} {
			differential(t, commitmenttest.Case{ID: tc.name, Rounds: withInitial(tc.initial, tc.ops)}, parityEngines)
		}
	})

	t.Run("incremental", func(t *testing.T) {
		c := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "incremental"})
		state := make(commitmenttest.State)
		for _, round := range c.Rounds {
			state.Apply(round)
		}
		final := state.Ops()
		reload := runner.RunSpec{Name: "v3-reload", Mode: commitment.ModeCollect, Workers: 1, Reload: true}
		roots, records, _ := differential(t, c, []runner.RunSpec{serialEngines[0], reload, serialEngines[1]})
		bulkRoots, bulkRecords, _ := differential(t, commitmenttest.Case{ID: "incremental/bulk", Rounds: [][]commitmenttest.Op{final}}, serialEngines[:1])
		require.Equal(t, liveStorageRecords(records), liveStorageRecords(bulkRecords))
		require.Equal(t, roots[len(roots)-1], bulkRoots[0])

		deletes := make([]commitmenttest.Op, len(final))
		for i, op := range final {
			deletes[i] = commitmenttest.DeleteAt(op.Key)
		}
		roots, _, _ = differential(t, commitmenttest.Case{ID: "incremental/unwind", Rounds: [][]commitmenttest.Op{final, deletes, final}}, serialEngines)
		require.Equal(t, empty.RootHash[:], roots[1])
		require.Equal(t, roots[0], roots[2])
	})

	t.Run("feed", func(t *testing.T) {
		storage := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "storage", Count: 300})
		whale := commitmenttest.Generate(commitmenttest.MathRand(424242), commitmenttest.SequenceSpec{Kind: "whale", Count: 3 * defaultStorageFanOutMin})
		seed := storage.Rounds[0]
		seed = append(seed, whale.Rounds[0]...)
		var next []commitmenttest.Op
		for i := range 300 {
			addr, slot := benchAddr(i), commitmenttest.SlotKey(benchAddr(i), benchSlot(i))
			switch i % 5 {
			case 0:
				next = append(next, commitmenttest.AccountAt(addr, commitmenttest.AccountSpec{Kind: "parity", Number: i + 1000}))
			case 1:
				next = append(next, commitmenttest.StorageAt(slot, i+1000))
			case 2:
				next = append(next, commitmenttest.DeleteAt(slot))
			case 3:
				next = append(next, commitmenttest.DeleteAt(addr))
			default:
				next = append(next, commitmenttest.DeleteAt(addr), commitmenttest.DeleteAt(slot))
			}
		}
		for i := 1; i < len(whale.Rounds[0]); i += 3 {
			next = append(next, commitmenttest.DeleteAt(whale.Rounds[0][i].Key))
		}
		want, _, _ := differential(t, commitmenttest.Case{ID: "feed", Rounds: [][]commitmenttest.Op{seed, next}}, serialEngines)
		for _, deferred := range []bool{false, true} {
			roots := requireSameRuns(t, nil, v3Config{deferred: deferred}, v3Config{deferred: deferred, feed: true}, seed, next)
			require.Equal(t, want, roots)
		}
	})

	for _, tt := range []struct {
		name    string
		kind    string
		first   []int
		second  []int
		rewrite []bool
	}{
		{"whale_batches", "whale", []int{2, 3, 5}, []int{1, 2, 3}, []bool{false}},
		{"one_slot_per_account", "one-slot", []int{2, 5, 20}, []int{1, 2, 5}, []bool{false, true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, rewrite := range tt.rewrite {
				for seed := int64(1); seed <= 40; seed++ {
					for _, b1 := range tt.first {
						for _, b2 := range tt.second {
							c := commitmenttest.Generate(commitmenttest.MathRand(seed), commitmenttest.SequenceSpec{Kind: tt.kind, BatchSizes: []int{b1, b2}, Rewrite: rewrite})
							c.ID = fmt.Sprintf("%s/%d+%d", tt.kind, b1, b2)
							if tt.kind == "one-slot" {
								c.ID += fmt.Sprintf("/rewrite=%t", rewrite)
							}
							differential(t, c, serialEngines)
						}
					}
				}
			}
		})
	}

	t.Run("E61/two-slot-seeds", func(t *testing.T) {
		for seed := range int64(200) {
			t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
				c := commitmenttest.Generate(commitmenttest.MathRand(seed), commitmenttest.SequenceSpec{Kind: "whale", Count: 2})
				c.ID = "E61/two-slot-seeds"
				differential(t, c, serialEngines[:1])
			})
		}
	})

	t.Run("slot_threshold", func(t *testing.T) {
		for _, seed := range []int64{424242, 1, 2, 3, 7, 99, 12345, 777, 31337, 5150} {
			for n := 1; n <= 80; n++ {
				c := commitmenttest.Generate(commitmenttest.MathRand(seed), commitmenttest.SequenceSpec{Kind: "whale", Count: n})
				c.ID = fmt.Sprintf("slot-threshold/%d", n)
				differential(t, c, serialEngines[:1])
			}
		}
	})

	t.Run("stress", func(t *testing.T) {
		for _, spec := range []commitmenttest.SequenceSpec{
			{Kind: "stress", Rounds: 120, Accounts: 6, Slots: 4, OpsPerRound: 3},
			{Kind: "stress", Rounds: 120, Accounts: 24, Slots: 10, OpsPerRound: 5},
			{Kind: "stress", Rounds: 60, Accounts: 200, Slots: 40, OpsPerRound: 8},
		} {
			runWorlds(t, spec, 4)
		}
	})

	t.Run("pending_collapse", func(t *testing.T) {
		hashedPrefix := func(prefix ...byte) []byte {
			for i := 0; ; i++ {
				addr := commitmenttest.Key(commitmenttest.KeySpec{Kind: "bench-address", Size: length.Addr}, i)
				if bytes.HasPrefix(commitment.KeyToHexNibbleHash(addr), prefix) {
					return addr
				}
			}
		}
		account := func(key []byte, number int) commitmenttest.Op {
			return commitmenttest.AccountAt(key, commitmenttest.AccountSpec{Kind: "parity", Number: number})
		}
		a, b, c, d, e := hashedPrefix(3, 1), hashedPrefix(3, 2), hashedPrefix(3, 5), hashedPrefix(9, 0), hashedPrefix(9, 1)
		seed := []commitmenttest.Op{account(a, 1), account(b, 2), account(d, 3), account(e, 4)}
		for _, tc := range []struct {
			name string
			next []commitmenttest.Op
		}{
			{"delete", []commitmenttest.Op{commitmenttest.DeleteAt(a), account(d, 5)}},
			{"delete_and_recreate", []commitmenttest.Op{commitmenttest.DeleteAt(a), account(c, 6), account(d, 5)}},
		} {
			differential(t, commitmenttest.Case{ID: "pending_collapse/" + tc.name, Rounds: [][]commitmenttest.Op{seed, tc.next}}, []runner.RunSpec{{Name: "v3", Mode: commitment.ModeCollect, Workers: 4}})
		}
	})

	t.Run("root_collapse", func(t *testing.T) {
		for _, accounts := range []int{2, 3, 5, 8} {
			runWorlds(t, commitmenttest.SequenceSpec{Kind: "root-collapse", Rounds: 120, Accounts: accounts}, 30)
		}
	})

	t.Run("literal", func(t *testing.T) {
		account := func(id, number int) commitmenttest.Op {
			return commitmenttest.AccountAt(incrAddress(id), commitmenttest.AccountSpec{Kind: "plain", Number: number})
		}
		accounts := func(ids ...int) []commitmenttest.Op {
			ops := make([]commitmenttest.Op, len(ids))
			for i, id := range ids {
				ops[i] = account(id, id)
			}
			return ops
		}
		slot := func(id, slot int, value byte) commitmenttest.Op {
			return commitmenttest.Op{Key: incrSlot(incrAddress(id), slot), Storage: []byte{value}}
		}
		remove := func(id int) commitmenttest.Op { return commitmenttest.DeleteAt(incrAddress(id)) }
		for _, tc := range []struct {
			name   string
			rounds [][]commitmenttest.Op
		}{
			{"account_root_extension", [][]commitmenttest.Op{accounts(0, 3), accounts(1)}},
			{"account_root_extension_pair", [][]commitmenttest.Op{accounts(0, 3), accounts(1, 2)}},
			{"insert_under_shared_prefix", [][]commitmenttest.Op{accounts(5), accounts(8), accounts(15)}},
			{"update_keeps_storage_root", [][]commitmenttest.Op{{account(5, 5), slot(5, 1, 0x11), slot(5, 2, 0x22)}, accounts(8), {account(5, 50)}}},
			{"delete_under_shared_prefix", [][]commitmenttest.Op{accounts(5), accounts(8), accounts(15, 31), {remove(5)}}},
			{"delete_collapses_branch_below_root", [][]commitmenttest.Op{accounts(5), accounts(8), {account(15, 15), remove(8)}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				differential(t, commitmenttest.Case{ID: tc.name, Rounds: tc.rounds}, parityEngines[:2])
			})
		}
	})
}

func FuzzParityRandomSequences(f *testing.F) {
	for _, seed := range []uint64{0, 1, 17, 99} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed uint64) {
		rng := rand.New(rand.NewSource(int64(seed)))
		rounds := make([][]commitmenttest.Op, 24)
		for step := range rounds {
			account := rng.Intn(8)
			op := commitmenttest.DeleteAt(incrAddress(account))
			if rng.Intn(4) != 0 {
				op = commitmenttest.AccountAt(op.Key, commitmenttest.AccountSpec{Kind: "parity", Number: step + account})
			}
			rounds[step] = []commitmenttest.Op{op}
		}
		// v3 only until https://github.com/erigontech/erigon/issues/25625 is fixed; then pass parityEngines.
		differential(t, commitmenttest.Case{ID: fmt.Sprintf("fuzz/%d", seed), Rounds: rounds}, parityEngines[:1])
	})
}

func runWorlds(t *testing.T, spec commitmenttest.SequenceSpec, seeds int64) {
	t.Helper()
	rounds, drifts := 0, 0
	for seed := int64(1); seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("a%d_s%d/seed%d", spec.Accounts, spec.Slots, seed), func(t *testing.T) {
			c := commitmenttest.Generate(commitmenttest.MathRand(seed), spec)
			c.Assertions.TolerateHPHDrift = true
			roots, _, hphDrift := differential(t, c, parityEngines[:2])
			require.Len(t, roots, spec.Rounds, "every configured round must be checked against v3")
			rounds += len(roots)
			drifts += len(hphDrift)
		})
	}
	require.Equal(t, int(seeds)*spec.Rounds, rounds)
	t.Logf("%s accounts=%d slots=%d: %d seeds, %d rounds checked against v3, %d HexPatriciaHashed drifts rebuilt", spec.Kind, spec.Accounts, spec.Slots, seeds, rounds, drifts)
}

func checkDifferentialRecords(t *testing.T, state commitmenttest.State, records map[string][]byte, label string) {
	t.Helper()
	want := map[string]map[string]struct{}{"": {}}
	for key := range state {
		hashed := string(commitment.KeyToHexNibbleHash([]byte(key)))
		owner, leaf := "", hashed
		if len(key) != length.Addr {
			owner, leaf = hashed[:64], hashed[64:]
		}
		if want[owner] == nil {
			want[owner] = make(map[string]struct{})
		}
		want[owner][leaf] = struct{}{}
	}
	ctx := newMockContext()
	ctx.branches = records
	for owner, leaves := range want {
		plane, addrHash := planeAccount, [32]byte{}
		if len(owner) != 0 {
			plane, addrHash = planeStorage, hashAddressPath([]byte(owner))
		}
		got := make(map[string]struct{})
		for path := range exactLeaves(t, ctx, plane, addrHash) {
			got[path] = struct{}{}
		}
		require.Equal(t, leaves, got, "%s: record integrity of owner %x", label, owner)
	}
}
