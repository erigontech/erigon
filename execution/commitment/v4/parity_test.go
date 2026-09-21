// Copyright 2026 The Erigon Authors
// This file is part of the Erigon project.
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

package v4

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

type parityUpdate struct {
	key    []byte
	update *commitment.Update
}

type parityContext struct {
	branches     map[string][]byte
	accounts     map[string]*commitment.Update
	storage      map[string]*commitment.Update
	accountCalls int
	storageCalls int
}

func newParityContext() *parityContext {
	return &parityContext{
		branches: make(map[string][]byte),
		accounts: make(map[string]*commitment.Update),
		storage:  make(map[string]*commitment.Update),
	}
}

func (p *parityContext) Branch(key []byte) ([]byte, kv.Step, error) {
	return bytes.Clone(p.branches[string(key)]), 0, nil
}

func (p *parityContext) PutBranch(key, data, _ []byte) error {
	p.branches[string(key)] = bytes.Clone(data)
	return nil
}

func (p *parityContext) Account(key []byte) (*commitment.Update, error) {
	p.accountCalls++
	if update, ok := p.accounts[string(key)]; ok {
		return update.Copy(), nil
	}
	return &commitment.Update{Flags: commitment.DeleteUpdate}, nil
}

func (p *parityContext) Storage(key []byte) (*commitment.Update, error) {
	p.storageCalls++
	if update, ok := p.storage[string(key)]; ok {
		return update.Copy(), nil
	}
	return &commitment.Update{Flags: commitment.DeleteUpdate}, nil
}

var _ commitment.PatriciaContext = (*parityContext)(nil)

func accountParityUpdate(i int) *commitment.Update {
	balance := uint256.NewInt(uint64(i + 1))
	return &commitment.Update{
		Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
		Balance:  *balance,
		Nonce:    uint64(i + 1),
		CodeHash: common.HexToHash(fmt.Sprintf("0x%064x", i+1)),
	}
}

func storageParityUpdate(i int) *commitment.Update {
	update := &commitment.Update{Flags: commitment.StorageUpdate}
	update.Storage[0] = byte(i)
	update.Storage[1] = byte(i >> 8)
	update.StorageLen = 2
	return update
}

func parityAddress(i int) []byte {
	address := make([]byte, length.Addr)
	for j := range address {
		address[j] = byte(i*17 + j)
	}
	return address
}

func paritySlot(i int) []byte {
	slot := make([]byte, length.Hash)
	for j := range slot {
		slot[j] = byte(i*29 + j*3)
	}
	return slot
}

func makeParityUpdates(t *testing.T, mode commitment.Mode, entries []parityUpdate) *commitment.Updates {
	t.Helper()
	updates := commitment.NewUpdates(mode, t.TempDir(), commitment.KeyToHexNibbleHash)
	for _, entry := range entries {
		updates.TouchPlainKeyDirect(string(entry.key), entry.update)
	}
	return updates
}

func parityRoots(t *testing.T, initial, entries []parityUpdate) ([3][]byte, *parityContext) {
	t.Helper()
	ctxV4 := newParityContext()
	ctxHPH := newParityContext()
	ctxParallel := newParityContext()
	for _, entry := range initial {
		if len(entry.key) == length.Addr {
			ctxHPH.accounts[string(entry.key)] = entry.update.Copy()
			ctxParallel.accounts[string(entry.key)] = entry.update.Copy()
		}
	}

	v4 := &Trie{}
	v4.ResetContext(ctxV4)
	hph := commitment.NewHexPatriciaHashed(length.Addr, ctxHPH, commitment.DefaultTrieConfig())
	parallel := commitment.NewParallelPatriciaHashed(func(context.Context) (commitment.PatriciaContext, func()) {
		return ctxParallel, nil
	}, length.Addr, commitment.DefaultTrieConfig())
	t.Cleanup(func() {
		v4.Release()
		hph.Release()
		parallel.Release()
	})

	if err := processParityBatch(t, v4, hph, parallel, initial); err != nil {
		t.Fatal(err)
	}
	rootV4, err := v4.Process(context.Background(), makeParityUpdates(t, commitment.ModeUpdate, entries), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	rootHPH, err := hph.Process(context.Background(), makeParityUpdates(t, commitment.ModeUpdate, entries), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	rootParallel, err := parallel.Process(context.Background(), makeParityUpdates(t, commitment.ModeParallel, entries), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Zero(t, ctxV4.accountCalls)
	require.Zero(t, ctxV4.storageCalls)
	require.Equal(t, rootHPH, rootV4)
	require.Equal(t, rootHPH, rootParallel)
	return [3][]byte{rootV4, rootHPH, rootParallel}, ctxV4
}

func processParityBatch(t *testing.T, v4 *Trie, hph *commitment.HexPatriciaHashed, parallel *commitment.ParallelPatriciaHashed, entries []parityUpdate) error {
	t.Helper()
	if len(entries) == 0 {
		return nil
	}
	ctx := context.Background()
	if _, err := v4.Process(ctx, makeParityUpdates(t, commitment.ModeUpdate, entries), "", nil, commitment.WarmupConfig{}); err != nil {
		return err
	}
	if _, err := hph.Process(ctx, makeParityUpdates(t, commitment.ModeUpdate, entries), "", nil, commitment.WarmupConfig{}); err != nil {
		return err
	}
	_, err := parallel.Process(ctx, makeParityUpdates(t, commitment.ModeParallel, entries), "", nil, commitment.WarmupConfig{})
	return err
}

func TestParityAccountsStorageAndMixed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		build  func(int) []parityUpdate
		counts []int
	}{
		{name: "accounts", build: func(n int) []parityUpdate {
			entries := make([]parityUpdate, n)
			for i := range entries {
				entries[i] = parityUpdate{key: parityAddress(i), update: accountParityUpdate(i)}
			}
			return entries
		}, counts: []int{1, 2, 16, 1000, 100000}},
		{name: "storage", build: func(n int) []parityUpdate {
			entries := make([]parityUpdate, n)
			for i := range entries {
				key := append(parityAddress(i), paritySlot(i)...)
				entries[i] = parityUpdate{key: key, update: storageParityUpdate(i)}
			}
			return entries
		}, counts: []int{1, 2, 16, 1000, 100000}},
		{name: "mixed", build: func(n int) []parityUpdate {
			entries := make([]parityUpdate, 0, n*2)
			for i := range n {
				entries = append(entries, parityUpdate{key: parityAddress(i), update: accountParityUpdate(i)})
				entries = append(entries, parityUpdate{key: append(parityAddress(i), paritySlot(i)...), update: storageParityUpdate(i)})
			}
			return entries
		}, counts: []int{1, 2, 16, 1000, 100000}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, count := range tc.counts {
				t.Run(fmt.Sprint(count), func(t *testing.T) {
					initial := []parityUpdate(nil)
					if tc.name == "storage" {
						initial = make([]parityUpdate, count)
						for i := range initial {
							initial[i] = parityUpdate{key: parityAddress(i), update: accountParityUpdate(i)}
						}
					}
					parityRoots(t, initial, tc.build(count))
				})
			}
		})
	}
}

func TestParityCorrectnessCases(t *testing.T) {
	address := parityAddress(7)
	slotA := append(append([]byte(nil), address...), paritySlot(8)...)
	slotB := append(append([]byte(nil), address...), paritySlot(9)...)
	account := []parityUpdate{{key: address, update: accountParityUpdate(7)}}
	parityRoots(t, nil, account)
	parityRoots(t, account, []parityUpdate{{key: slotA, update: storageParityUpdate(8)}})
	parityRoots(t, account, []parityUpdate{{key: slotA, update: storageParityUpdate(8)}, {key: slotB, update: storageParityUpdate(9)}})
	parityRoots(t, nil, []parityUpdate{{key: address, update: accountParityUpdate(7)}, {key: slotA, update: storageParityUpdate(8)}})
	parityRoots(t, nil, []parityUpdate{{key: address, update: accountParityUpdate(12)}, {key: slotB, update: storageParityUpdate(13)}})
	parityRoots(t, nil, []parityUpdate{{key: address, update: &commitment.Update{Flags: commitment.DeleteUpdate}}})
}

func parityFuzzAddress(i int) []byte {
	key := make([]byte, length.Addr)
	binary.BigEndian.PutUint64(key[length.Addr-8:], uint64(i))
	return key
}

func FuzzParityRandomSequences(f *testing.F) {
	for _, seed := range []uint64{0, 1, 17, 99} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed uint64) {
		rng := rand.New(rand.NewSource(int64(seed)))
		state := make(map[string]parityUpdate)
		for step := range 24 {
			account := int(rng.Intn(8))
			address := parityFuzzAddress(account)
			if rng.Intn(4) == 0 {
				delete(state, string(address))
			} else {
				state[string(address)] = parityUpdate{key: address, update: accountParityUpdate(step + account)}
			}
			parityRoots(t, nil, parityStateAccounts(state))
		}
	})
}

func parityStateAccounts(state map[string]parityUpdate) []parityUpdate {
	entries := make([]parityUpdate, 0, len(state))
	for _, entry := range state {
		if len(entry.key) == length.Addr {
			entries = append(entries, entry)
		}
	}
	return entries
}
