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

package commitmenttest

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
)

type SequenceSpec struct {
	Rounds      int
	Accounts    int
	Slots       int
	OpsPerRound int
	Kind        string
	Count       int
	BatchSizes  []int
	Rewrite     bool
}

func Generate(seed Seed, spec SequenceSpec) (Case, error) {
	c := Case{ID: spec.Kind, Seed: seed, Assertions: Assertions{ProcessNoError: true}}
	if len(spec.BatchSizes) != 0 {
		if spec.Count != 0 || (spec.Kind != "whale" && spec.Kind != "one-slot") {
			return c, fmt.Errorf("batch sizes require whale or one-slot without a count")
		}
		for _, size := range spec.BatchSizes {
			if size < 0 {
				return c, fmt.Errorf("negative batch size %d", size)
			}
			spec.Count += size
		}
	}
	if spec.Count < 0 {
		return c, fmt.Errorf("negative count %d", spec.Count)
	}
	rng, err := seed.rand()
	if err != nil {
		return c, err
	}
	if spec.Kind == "stress" || spec.Kind == "root-collapse" {
		if spec.Rounds < 0 || spec.Accounts < 1 {
			return c, fmt.Errorf("invalid stress dimensions")
		}
		if spec.Kind == "stress" {
			if spec.Slots < 1 || spec.OpsPerRound < 1 {
				return c, fmt.Errorf("invalid stress operations")
			}
			c.Rounds = stressRounds(rng, spec)
		} else {
			c.Rounds = collapseRounds(rng, spec)
		}
		return c, nil
	}
	if spec.Kind == "incremental" {
		c.Rounds = incrementalRounds()
		return c, nil
	}
	if spec.Kind == "one-slot" {
		if len(spec.BatchSizes) != 2 {
			return c, fmt.Errorf("one-slot requires two batch sizes")
		}
		addrs, slots := make([][]byte, spec.Count), make([][]byte, spec.Count)
		for i := range addrs {
			addrs[i], slots[i] = make([]byte, 20), make([]byte, 32)
			_, _ = rng.Read(addrs[i])
			_, _ = rng.Read(slots[i])
		}
		for round, count := range spec.BatchSizes {
			start, generation := 0, 0
			if round == 1 {
				start = spec.BatchSizes[0]
				if spec.Rewrite {
					start, generation = 0, 1
				}
			}
			var ops []Op
			for i := start; i < start+count; i++ {
				value := Account(AccountSpec{Kind: "parity", Number: i})
				ops = append(ops, Op{Key: addrs[i], Account: &value}, Op{Key: append(bytes.Clone(addrs[i]), slots[i]...), Storage: Storage(StorageSpec{Number: i + generation*1000})})
			}
			c.Rounds = append(c.Rounds, ops)
		}
		return c, nil
	}
	ops := make([]Op, 0, spec.Count)
	addAccount := func(key []byte, number int) {
		value := Account(AccountSpec{Kind: "parity", Number: number})
		ops = append(ops, Op{Key: key, Account: &value})
	}
	address := func(i int) []byte { return Key(KeySpec{Kind: "bench-address", Size: 20}, i) }
	switch spec.Kind {
	case "accounts", "storage":
		for i := range spec.Count {
			addAccount(address(i), i)
			if spec.Kind == "storage" {
				ops = append(ops, Op{Key: append(address(i), Key(KeySpec{Kind: "bench-slot", Size: 32}, i)...), Storage: Storage(StorageSpec{Number: i})})
			}
		}
	case "whale", "whale_mixed":
		accountNumber := 1
		if spec.Kind == "whale_mixed" {
			accountNumber = 7
			for i := range 1000 {
				addAccount(address(i), i)
			}
		}
		addr := make([]byte, 20)
		_, _ = rng.Read(addr)
		addAccount(addr, accountNumber)
		for i := range spec.Count {
			slot := make([]byte, 32)
			_, _ = rng.Read(slot)
			ops = append(ops, Op{Key: append(bytes.Clone(addr), slot...), Storage: Storage(StorageSpec{Number: i})})
		}
		if spec.Kind == "whale_mixed" {
			for i := range 1000 {
				addAccount(address(500000+i), i)
			}
		}
	default:
		return c, fmt.Errorf("unknown sequence kind: %s", spec.Kind)
	}
	c.Rounds = [][]Op{ops}
	if len(spec.BatchSizes) != 0 {
		c.Rounds = make([][]Op, 0, len(spec.BatchSizes))
		start := 0
		for round, count := range spec.BatchSizes {
			if round == 0 {
				count++
			}
			c.Rounds = append(c.Rounds, ops[start:start+count])
			start += count
		}
	}
	return c, nil
}

func incrementalRounds() [][]Op {
	a, b := bytes.Repeat([]byte{0x11}, 20), bytes.Repeat([]byte{0x22}, 20)
	slot := func(addr []byte, number byte) []byte {
		return append(bytes.Clone(addr), bytes.Repeat([]byte{number}, 32)...)
	}
	account := func(addr []byte, nonce, balance uint64) Op {
		value := Account(AccountSpec{Nonce: nonce, Balance: balance, CodeHash: common.HexToHash("0x1234")})
		return Op{Key: addr, Account: &value}
	}
	return [][]Op{
		{account(a, 1, 10), account(b, 2, 20), {Key: slot(a, 0x31), Storage: []byte{1}}, {Key: slot(b, 0x41), Storage: []byte{2}}},
		{account(a, 3, 30), account(a, 4, 40), {Key: slot(a, 0x32), Storage: []byte{3}}, {Key: slot(a, 0x33), Delete: true}, {Key: slot(a, 0x33), Storage: []byte{4}}},
		{{Key: a, Read: true}, {Key: slot(a, 0x31), Read: true}, {Key: b, Delete: true}},
	}
}

type State map[string]Op

func (s State) Apply(ops []Op) {
	for _, op := range ops {
		if op.Read {
			continue
		}
		key := string(op.Key)
		if op.Delete {
			delete(s, key)
			if len(op.Key) == 20 {
				for k := range s {
					if len(k) > 20 && bytes.HasPrefix([]byte(k), op.Key) {
						delete(s, k)
					}
				}
			}
			continue
		}
		op.Key = bytes.Clone(op.Key)
		op.Storage = bytes.Clone(op.Storage)
		if op.Account != nil {
			value := AccountValue{Fields: allFields, CodeHash: empty.CodeHash}
			if prev := s[key].Account; prev != nil {
				value = *prev
			}
			if op.Account.Fields&BalanceField != 0 {
				value.Balance = op.Account.Balance
			}
			if op.Account.Fields&NonceField != 0 {
				value.Nonce = op.Account.Nonce
			}
			if op.Account.Fields&CodeField != 0 {
				value.CodeHash = op.Account.CodeHash
			}
			op.Account = &value
		}
		s[key] = op
	}
}

func (s State) Ops() []Op {
	keys := make([]string, 0, len(s))
	for key := range s {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ops := make([]Op, 0, len(keys))
	for _, key := range keys {
		op := s[key]
		op.Key, op.Storage = bytes.Clone(op.Key), bytes.Clone(op.Storage)
		if op.Account != nil {
			value := *op.Account
			op.Account = &value
		}
		ops = append(ops, op)
	}
	return ops
}

func stressRounds(rng *rand.Rand, spec SequenceSpec) [][]Op {
	rounds := make([][]Op, 0, spec.Rounds)
	live := make(map[string]struct{})
	for range spec.Rounds {
		count := 1 + rng.Intn(spec.OpsPerRound)
		touched := make(map[string]struct{}, count)
		entries := make([]Op, 0, count)
		for range count {
			addr := Key(KeySpec{Kind: "integer", Size: 20}, rng.Intn(spec.Accounts))
			if _, busy := touched[string(addr)]; busy {
				continue
			}
			_, alive := live[string(addr)]
			switch {
			case !alive:
				touched[string(addr)] = struct{}{}
				live[string(addr)] = struct{}{}
				entries = append(entries, accountOp(addr, RandomAccount(rng)))
			case rng.Intn(16) == 0:
				touched[string(addr)] = struct{}{}
				delete(live, string(addr))
				entries = append(entries, Op{Key: addr, Delete: true})
			default:
				touched[string(addr)] = struct{}{}
				if rng.Intn(4) == 0 {
					entries = append(entries, accountOp(addr, RandomAccount(rng)))
				}
				for range 1 + rng.Intn(3) {
					key := append(bytes.Clone(addr), Key(KeySpec{Kind: "integer", Size: 32}, rng.Intn(spec.Slots))...)
					if rng.Intn(5) == 0 {
						entries = append(entries, Op{Key: key, Delete: true})
						continue
					}
					entries = append(entries, Op{Key: key, Storage: RandomStorage(rng)})
				}
			}
		}
		rounds = append(rounds, entries)
	}
	return rounds
}

func collapseRounds(rng *rand.Rand, spec SequenceSpec) [][]Op {
	rounds := make([][]Op, 0, spec.Rounds)
	live := make(map[int]struct{})
	for n := range spec.Rounds {
		entries := make([]Op, 0, 4)
		touched := make(map[int]struct{})
		for i := range spec.Accounts {
			if _, ok := live[i]; ok {
				continue
			}
			if rng.Intn(2) == 0 {
				live[i] = struct{}{}
				touched[i] = struct{}{}
				entries = append(entries, accountOp(Key(KeySpec{Kind: "integer", Size: 20}, i), Account(AccountSpec{Kind: "plain", Number: i*31 + n})))
			}
		}
		for i := range spec.Accounts {
			if _, ok := live[i]; !ok {
				continue
			}
			if _, busy := touched[i]; busy {
				continue
			}
			if rng.Intn(3) != 0 {
				continue
			}
			delete(live, i)
			touched[i] = struct{}{}
			entries = append(entries, Op{Key: Key(KeySpec{Kind: "integer", Size: 20}, i), Delete: true})
		}
		rounds = append(rounds, entries)
	}
	return rounds
}
func accountOp(key []byte, value AccountValue) Op { return Op{Key: key, Account: &value} }
