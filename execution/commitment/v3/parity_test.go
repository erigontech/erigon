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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

type parityUpdate struct {
	key    []byte
	update *commitment.Update
}

type parityContext struct {
	mu       sync.Mutex
	branches map[string][]byte
}

func newParityContext() *parityContext { return &parityContext{branches: make(map[string][]byte)} }

func (p *parityContext) Branch(key []byte) ([]byte, kv.Step, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return bytes.Clone(p.branches[string(key)]), 0, nil
}

func (p *parityContext) PutBranch(key, data, _ []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.branches[string(key)] = bytes.Clone(data)
	return nil
}

func (p *parityContext) Account([]byte) (*commitment.Update, error) {
	return &commitment.Update{Flags: commitment.DeleteUpdate}, nil
}

func (p *parityContext) Storage([]byte) (*commitment.Update, error) {
	return &commitment.Update{Flags: commitment.DeleteUpdate}, nil
}

func accountParityUpdate(i int) *commitment.Update {
	return testAccountUpdate(commitmenttest.Account(commitmenttest.AccountSpec{Kind: "parity", Number: i}))
}

func storageParityUpdate(i int) *commitment.Update {
	return storageUpdate(commitmenttest.Storage(commitmenttest.StorageSpec{Number: i}))
}

func parityAddress(i int) []byte {
	return commitmenttest.Key(commitmenttest.KeySpec{Kind: "wrapping-address", Size: 20}, i)
}

func paritySlot(i int) []byte {
	return commitmenttest.Key(commitmenttest.KeySpec{Kind: "wrapping-slot", Size: 32}, i)
}

func parityFuzzAddress(i int) []byte { return incrAddress(i) }

func incrAddress(i int) []byte {
	return commitmenttest.Key(commitmenttest.KeySpec{Kind: "integer", Size: 20}, i)
}

func incrSlot(addr []byte, j int) []byte {
	return slotKey(addr, commitmenttest.Key(commitmenttest.KeySpec{Kind: "integer", Size: 32}, j))
}

func slotKey(addr, slot []byte) []byte { return append(bytes.Clone(addr), slot...) }

func accountOp(key []byte, spec commitmenttest.AccountSpec) commitmenttest.Op {
	value := commitmenttest.Account(spec)
	return commitmenttest.Op{Key: key, Account: &value}
}

func slotOp(key []byte, number int) commitmenttest.Op {
	return commitmenttest.Op{Key: key, Storage: commitmenttest.Storage(commitmenttest.StorageSpec{Number: number})}
}

var (
	parityEngines = []runner.RunSpec{{Name: "v3", Mode: commitment.ModeCollect}, {Name: "hph", Mode: commitment.ModeUpdate}, {Name: "parallel", Mode: commitment.ModeParallel}}
	serialEngines = []runner.RunSpec{{Name: "v3", Mode: commitment.ModeCollect, Workers: 1}, {Name: "hph", Mode: commitment.ModeUpdate}}
)

func differential(t *testing.T, c commitmenttest.Case, specs []runner.RunSpec) ([][]byte, map[string][]byte, []int) {
	t.Helper()
	c.Assertions = commitmenttest.Assertions{TolerateHPHDrift: c.Assertions.TolerateHPHDrift, ProcessNoError: true, ZeroAccountReads: true, ZeroStorageReads: true, StateReadEngines: []string{"v3", "v3-reload", "fresh-v3"}}
	modes := []runner.RunSpec{specs[0], {Name: "fresh-v3", Mode: commitment.ModeCollect, Workers: 1, Fresh: true}, {Name: "fresh-hph", Mode: commitment.ModeUpdate, Fresh: true}}
	got := runner.Compare(t, c, append(modes, specs[1:]...), openTestTrie)
	roots := make([][]byte, len(c.Rounds))
	state := make(commitmenttest.State)
	for i := range got[0].Rounds {
		round := &got[0].Rounds[i]
		roots[i] = round.Root
		state.Apply(c.Rounds[i])
		checkDifferentialRecords(t, state, round.Records, fmt.Sprintf("case=%s seed=%+v round=%d", c.ID, c.Seed, i))
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
		return accountOp(parityAddress(i), commitmenttest.AccountSpec{Kind: "parity", Number: i})
	}
	t.Run("bulk", func(t *testing.T) {
		for _, kind := range []string{"accounts", "storage", "mixed"} {
			for _, count := range []int{1, 2, 16, 1000, 100000} {
				var initial, ops []commitmenttest.Op
				for i := range count {
					slot := slotOp(slotKey(parityAddress(i), paritySlot(i)), i)
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
		slotA, slotB := slotKey(address, paritySlot(8)), slotKey(address, paritySlot(9))
		account := []commitmenttest.Op{parity(7)}
		fields := func(spec commitmenttest.AccountSpec) commitmenttest.Op {
			spec.CodeHash = empty.CodeHash
			return accountOp(address, spec)
		}
		for _, tc := range []struct {
			name         string
			initial, ops []commitmenttest.Op
		}{
			{"account", nil, account},
			{"slot_after_account", account, []commitmenttest.Op{slotOp(slotA, 8)}},
			{"two_slots_after_account", account, []commitmenttest.Op{slotOp(slotA, 8), slotOp(slotB, 9)}},
			{"account_with_slot", nil, []commitmenttest.Op{parity(7), slotOp(slotA, 8)}},
			{"account_with_other_slot", nil, []commitmenttest.Op{accountOp(address, commitmenttest.AccountSpec{Kind: "parity", Number: 12}), slotOp(slotB, 13)}},
			{"delete_absent_account", nil, []commitmenttest.Op{{Key: address, Delete: true}}},
			{"zeroed_account", nil, []commitmenttest.Op{fields(commitmenttest.AccountSpec{})}},
			{"zeroed_account_with_neighbour", nil, []commitmenttest.Op{fields(commitmenttest.AccountSpec{}), parity(9)}},
			{"nonce_only", nil, []commitmenttest.Op{fields(commitmenttest.AccountSpec{Nonce: 1})}},
			{"balance_only", nil, []commitmenttest.Op{fields(commitmenttest.AccountSpec{Balance: 5})}},
		} {
			differential(t, commitmenttest.Case{ID: tc.name, Rounds: withInitial(tc.initial, tc.ops)}, parityEngines)
		}
	})

	t.Run("incremental", func(t *testing.T) {
		c, err := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "incremental"})
		require.NoError(t, err)
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
			deletes[i] = commitmenttest.Op{Key: op.Key, Delete: true}
		}
		roots, _, _ = differential(t, commitmenttest.Case{ID: "incremental/unwind", Rounds: [][]commitmenttest.Op{final, deletes, final}}, serialEngines)
		require.Equal(t, empty.RootHash[:], roots[1])
		require.Equal(t, roots[0], roots[2])
	})

	t.Run("feed", func(t *testing.T) {
		storage, err := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "storage", Count: 300})
		require.NoError(t, err)
		whale, err := commitmenttest.Generate(commitmenttest.MathRand(424242), commitmenttest.SequenceSpec{Kind: "whale", Count: 3 * defaultStorageFanOutMin})
		require.NoError(t, err)
		seed := storage.Rounds[0]
		seed = append(seed, whale.Rounds[0]...)
		var next []commitmenttest.Op
		for i := range 300 {
			addr, slot := benchAddr(i), slotKey(benchAddr(i), benchSlot(i))
			switch i % 5 {
			case 0:
				next = append(next, accountOp(addr, commitmenttest.AccountSpec{Kind: "parity", Number: i + 1000}))
			case 1:
				next = append(next, slotOp(slot, i+1000))
			case 2:
				next = append(next, commitmenttest.Op{Key: slot, Delete: true})
			case 3:
				next = append(next, commitmenttest.Op{Key: addr, Delete: true})
			default:
				next = append(next, commitmenttest.Op{Key: addr, Delete: true}, commitmenttest.Op{Key: slot, Delete: true})
			}
		}
		for i := 1; i < len(whale.Rounds[0]); i += 3 {
			next = append(next, commitmenttest.Op{Key: whale.Rounds[0][i].Key, Delete: true})
		}
		want, _, _ := differential(t, commitmenttest.Case{ID: "feed", Rounds: [][]commitmenttest.Op{seed, next}}, serialEngines)
		for _, deferred := range []bool{false, true} {
			roots := requireSameRuns(t, nil, v3Config{deferred: deferred}, v3Config{deferred: deferred, feed: true}, parityEntries(seed), parityEntries(next))
			require.Equal(t, want, roots)
		}
	})

	t.Run("whale_batches", func(t *testing.T) {
		for seed := int64(1); seed <= 40; seed++ {
			for _, b1 := range []int{2, 3, 5} {
				for _, b2 := range []int{1, 2, 3} {
					c, err := commitmenttest.Generate(commitmenttest.MathRand(seed), commitmenttest.SequenceSpec{Kind: "whale", BatchSizes: []int{b1, b2}})
					require.NoError(t, err)
					c.ID = fmt.Sprintf("whale/%d+%d", b1, b2)
					differential(t, c, serialEngines)
				}
			}
		}
	})

	t.Run("one_slot_per_account", func(t *testing.T) {
		for _, rewrite := range []bool{false, true} {
			for seed := int64(1); seed <= 40; seed++ {
				for _, n1 := range []int{2, 5, 20} {
					for _, n2 := range []int{1, 2, 5} {
						c, err := commitmenttest.Generate(commitmenttest.MathRand(seed), commitmenttest.SequenceSpec{Kind: "one-slot", BatchSizes: []int{n1, n2}, Rewrite: rewrite})
						require.NoError(t, err)
						c.ID = fmt.Sprintf("one-slot/%d+%d/rewrite=%t", n1, n2, rewrite)
						differential(t, c, serialEngines)
					}
				}
			}
		}
	})

	t.Run("E61/two-slot-seeds", func(t *testing.T) {
		for seed := range int64(200) {
			t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
				c, err := commitmenttest.Generate(commitmenttest.MathRand(seed), commitmenttest.SequenceSpec{Kind: "whale", Count: 2})
				require.NoError(t, err)
				c.ID = "E61/two-slot-seeds"
				differential(t, c, serialEngines[:1])
			})
		}
	})

	t.Run("slot_threshold", func(t *testing.T) {
		for _, seed := range []int64{424242, 1, 2, 3, 7, 99, 12345, 777, 31337, 5150} {
			for n := 1; n <= 80; n++ {
				c, err := commitmenttest.Generate(commitmenttest.MathRand(seed), commitmenttest.SequenceSpec{Kind: "whale", Count: n})
				require.NoError(t, err)
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

	t.Run("root_collapse", func(t *testing.T) {
		for _, accounts := range []int{2, 3, 5, 8} {
			runWorlds(t, commitmenttest.SequenceSpec{Kind: "root-collapse", Rounds: 120, Accounts: accounts}, 30)
		}
	})

	t.Run("literal", func(t *testing.T) {
		account := func(id, number int) commitmenttest.Op {
			return accountOp(incrAddress(id), commitmenttest.AccountSpec{Kind: "plain", Number: number})
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
		remove := func(id int) commitmenttest.Op { return commitmenttest.Op{Key: incrAddress(id), Delete: true} }
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
		state := make(commitmenttest.State)
		for step := range 24 {
			account := rng.Intn(8)
			op := commitmenttest.Op{Key: parityFuzzAddress(account), Delete: true}
			if rng.Intn(4) != 0 {
				op = accountOp(op.Key, commitmenttest.AccountSpec{Kind: "parity", Number: step + account})
			}
			state.Apply([]commitmenttest.Op{op})
			differential(t, commitmenttest.Case{ID: fmt.Sprintf("fuzz/%d/%d", seed, step), Rounds: [][]commitmenttest.Op{state.Ops()}}, parityEngines)
		}
	})
}

func runWorlds(t *testing.T, spec commitmenttest.SequenceSpec, seeds int64) {
	t.Helper()
	rounds, drifts := 0, 0
	for seed := int64(1); seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("a%d_s%d/seed%d", spec.Accounts, spec.Slots, seed), func(t *testing.T) {
			c, err := commitmenttest.Generate(commitmenttest.MathRand(seed), spec)
			require.NoError(t, err)
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
