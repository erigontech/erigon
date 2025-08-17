// Copyright 2024 The Erigon Authors
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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/erigontech/erigon/db/kv"
	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
)

// In memory commitment and state to use with the tests
type MockState struct {
	t          *testing.T
	concurrent atomic.Bool

	mu     sync.Mutex            // to protect sm and cm for concurrent trie
	sm     map[string][]byte     // backbone of the state
	cm     map[string]BranchData // backbone of the commitments
	numBuf [binary.MaxVarintLen64]byte
}

func NewMockState(t *testing.T) *MockState {
	t.Helper()
	return &MockState{
		t:  t,
		sm: make(map[string][]byte),
		cm: make(map[string]BranchData),
	}
}

func (ms *MockState) SetConcurrentCommitment(concurrent bool) {
	ms.concurrent.Store(concurrent)
}

func (ms *MockState) TempDir() string {
	return ms.t.TempDir()
}

func (ms *MockState) PutBranch(prefix []byte, data []byte, prevData []byte, prevStep kv.Step) error {
	// updates already merged by trie
	if ms.concurrent.Load() {
		ms.mu.Lock()
		defer ms.mu.Unlock()
	}
	ms.cm[string(prefix)] = data
	return nil
}

func (ms *MockState) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if ms.concurrent.Load() {
		ms.mu.Lock()
		defer ms.mu.Unlock()
	}
	if exBytes, ok := ms.cm[string(prefix)]; ok {
		//fmt.Printf("GetBranch prefix %x, exBytes (%d) %x [%v]\n", prefix, len(exBytes), []byte(exBytes), BranchData(exBytes).String())
		return exBytes, 0, nil
	}
	return nil, 0, nil
}

func (ms *MockState) Account(plainKey []byte) (*Update, error) {
	if ms.concurrent.Load() {
		ms.mu.Lock()
	}
	exBytes, ok := ms.sm[string(plainKey[:])]
	if ms.concurrent.Load() {
		ms.mu.Unlock()
	}
	if !ok {
		//ms.t.Logf("%p GetAccount not found key [%x]", ms, plainKey)
		u := new(Update)
		u.Flags = DeleteUpdate
		return u, nil
	}

	var ex Update
	pos, err := ex.Decode(exBytes, 0)
	if err != nil {
		ms.t.Fatalf("GetAccount decode existing [%x], bytes: [%x]: %v", plainKey, exBytes, err)
		return nil, nil
	}
	if pos != len(exBytes) {
		ms.t.Fatalf("GetAccount key [%x] leftover %d bytes in [%x], comsumed %x", plainKey, len(exBytes)-pos, exBytes, pos)
		return nil, nil
	}
	if ex.Flags&StorageUpdate != 0 {
		ms.t.Logf("GetAccount reading storage item for key [%x]", plainKey)
		return nil, errors.New("storage read by GetAccount")
	}
	if ex.Flags&DeleteUpdate != 0 {
		ms.t.Fatalf("GetAccount reading deleted account for key [%x]", plainKey)
		return nil, nil
	}
	return &ex, nil
}

func (ms *MockState) Storage(plainKey []byte) (*Update, error) {
	if ms.concurrent.Load() {
		ms.mu.Lock()
	}
	exBytes, ok := ms.sm[string(plainKey[:])]
	if ms.concurrent.Load() {
		ms.mu.Unlock()
	}
	if !ok {
		ms.t.Logf("GetStorage not found key [%x]", plainKey)
		u := new(Update)
		u.Flags = DeleteUpdate
		return u, nil
	}
	var ex Update
	pos, err := ex.Decode(exBytes, 0)
	if err != nil {
		ms.t.Fatalf("GetStorage decode existing [%x], bytes: [%x]: %v", plainKey, exBytes, err)
		return nil, nil
	}
	if pos != len(exBytes) {
		ms.t.Fatalf("GetStorage key [%x] leftover bytes in [%x], comsumed %x", plainKey, exBytes, pos)
		return nil, nil
	}
	if ex.Flags&BalanceUpdate != 0 {
		ms.t.Logf("GetStorage reading balance for key [%x]", plainKey)
		return nil, nil
	}
	if ex.Flags&NonceUpdate != 0 {
		ms.t.Fatalf("GetStorage reading nonce for key [%x]", plainKey)
		return nil, nil
	}
	if ex.Flags&CodeUpdate != 0 {
		ms.t.Fatalf("GetStorage reading codeHash for key [%x]", plainKey)
		return nil, nil
	}
	return &ex, nil
}

// / called sequentially outside of the trie so no need to protect
func (ms *MockState) applyPlainUpdates(plainKeys [][]byte, updates []Update) error {
	for i, key := range plainKeys {
		update := updates[i]
		if update.Flags&DeleteUpdate != 0 {
			delete(ms.sm, string(key))
		} else {
			if exBytes, ok := ms.sm[string(key)]; ok {
				var ex Update
				pos, err := ex.Decode(exBytes, 0)
				if err != nil {
					return fmt.Errorf("applyPlainUpdates decode existing [%x], bytes: [%x]: %w", key, exBytes, err)
				}
				if pos != len(exBytes) {
					return fmt.Errorf("applyPlainUpdates key [%x] leftover bytes in [%x], comsumed %x", key, exBytes, pos)
				}
				ex.Merge(&update)
				ms.sm[string(key)] = ex.Encode(nil, ms.numBuf[:])
			} else {
				ms.sm[string(key)] = update.Encode(nil, ms.numBuf[:])
			}
		}
	}
	return nil
}

// / called sequentially outside of the trie so no need to protect
func (ms *MockState) applyBranchNodeUpdates(updates map[string]BranchData) {
	for key, update := range updates {
		if pre, ok := ms.cm[key]; ok {
			// Merge
			merged, err := pre.MergeHexBranches(update, nil)
			if err != nil {
				panic(err)
			}
			ms.cm[key] = merged
		} else {
			ms.cm[key] = update
		}
	}
}

func decodeHex(in string) []byte {
	payload, err := hex.DecodeString(in)
	if err != nil {
		panic(err)
	}
	return payload
}

// UpdateBuilder collects updates to the state
// and provides them in properly sorted form
type UpdateBuilder struct {
	balances   map[string]*uint256.Int
	nonces     map[string]uint64
	codeHashes map[string][length.Hash]byte
	storages   map[string]map[string][]byte
	deletes    map[string]struct{}
	deletes2   map[string]map[string]struct{}
	keyset     map[string]struct{}
	keyset2    map[string]map[string]struct{}
}

func NewUpdateBuilder() *UpdateBuilder {
	return &UpdateBuilder{
		balances:   make(map[string]*uint256.Int),
		nonces:     make(map[string]uint64),
		codeHashes: make(map[string][length.Hash]byte),
		storages:   make(map[string]map[string][]byte),
		deletes:    make(map[string]struct{}),
		deletes2:   make(map[string]map[string]struct{}),
		keyset:     make(map[string]struct{}),
		keyset2:    make(map[string]map[string]struct{}),
	}
}

func (ub *UpdateBuilder) Balance(addr string, balance uint64) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.deletes, sk)
	ub.balances[sk] = uint256.NewInt(balance)
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) Nonce(addr string, nonce uint64) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.deletes, sk)
	ub.nonces[sk] = nonce
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) CodeHash(addr string, hash string) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.deletes, sk)
	hcode, err := hex.DecodeString(hash)
	if err != nil {
		panic(fmt.Errorf("invalid code hash provided: %w", err))
	}
	if len(hcode) != length.Hash {
		panic(fmt.Errorf("code hash should be %d bytes long, got %d", length.Hash, len(hcode)))
	}

	dst := [length.Hash]byte{}
	copy(dst[:32], hcode)

	ub.codeHashes[sk] = dst
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) Storage(addr string, loc string, value string) *UpdateBuilder {
	sk1 := string(decodeHex(addr))
	sk2 := string(decodeHex(loc))
	v := decodeHex(value)
	if d, ok := ub.deletes2[sk1]; ok {
		delete(d, sk2)
		if len(d) == 0 {
			delete(ub.deletes2, sk1)
		}
	}
	if k, ok := ub.keyset2[sk1]; ok {
		k[sk2] = struct{}{}
	} else {
		ub.keyset2[sk1] = make(map[string]struct{})
		ub.keyset2[sk1][sk2] = struct{}{}
	}
	if s, ok := ub.storages[sk1]; ok {
		s[sk2] = v
	} else {
		ub.storages[sk1] = make(map[string][]byte)
		ub.storages[sk1][sk2] = v
	}
	return ub
}

func (ub *UpdateBuilder) IncrementBalance(addr string, balance []byte) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.deletes, sk)
	increment := uint256.NewInt(0)
	increment.SetBytes(balance)
	if old, ok := ub.balances[sk]; ok {
		balance := uint256.NewInt(0)
		balance.Add(old, increment)
		ub.balances[sk] = balance
	} else {
		ub.balances[sk] = increment
	}
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) Delete(addr string) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.balances, sk)
	delete(ub.nonces, sk)
	delete(ub.codeHashes, sk)
	delete(ub.storages, sk)
	ub.deletes[sk] = struct{}{}
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) DeleteStorage(addr string, loc string) *UpdateBuilder {
	sk1 := string(decodeHex(addr))
	sk2 := string(decodeHex(loc))
	if s, ok := ub.storages[sk1]; ok {
		delete(s, sk2)
		if len(s) == 0 {
			delete(ub.storages, sk1)
		}
	}
	if k, ok := ub.keyset2[sk1]; ok {
		k[sk2] = struct{}{}
	} else {
		ub.keyset2[sk1] = make(map[string]struct{})
		ub.keyset2[sk1][sk2] = struct{}{}
	}
	if d, ok := ub.deletes2[sk1]; ok {
		d[sk2] = struct{}{}
	} else {
		ub.deletes2[sk1] = make(map[string]struct{})
		ub.deletes2[sk1][sk2] = struct{}{}
	}
	return ub
}

// Build returns three slices (in the order sorted by the hashed keys)
// 1. Plain keys
// 2. Corresponding hashed keys
// 3. Corresponding updates
func (ub *UpdateBuilder) Build() (plainKeys [][]byte, updates []Update) {
	hashed := make([]string, 0, len(ub.keyset)+len(ub.keyset2))
	preimages := make(map[string][]byte)
	preimages2 := make(map[string][]byte)
	keccak := sha3.NewLegacyKeccak256()
	for key := range ub.keyset {
		keccak.Reset()
		keccak.Write([]byte(key))
		h := keccak.Sum(nil)
		hashedKey := make([]byte, len(h)*2)
		for i, c := range h {
			hashedKey[i*2] = (c >> 4) & 0xf
			hashedKey[i*2+1] = c & 0xf
		}
		hashed = append(hashed, string(hashedKey))
		preimages[string(hashedKey)] = []byte(key)
	}
	hashedKey := make([]byte, 128)
	for sk1, k := range ub.keyset2 {
		keccak.Reset()
		keccak.Write([]byte(sk1))
		h := keccak.Sum(nil)
		for i, c := range h {
			hashedKey[i*2] = (c >> 4) & 0xf
			hashedKey[i*2+1] = c & 0xf
		}
		for sk2 := range k {
			keccak.Reset()
			keccak.Write([]byte(sk2))
			h2 := keccak.Sum(nil)
			for i, c := range h2 {
				hashedKey[64+i*2] = (c >> 4) & 0xf
				hashedKey[64+i*2+1] = c & 0xf
			}
			hs := string(common.Copy(hashedKey))
			hashed = append(hashed, hs)
			preimages[hs] = []byte(sk1)
			preimages2[hs] = []byte(sk2)
		}

	}
	slices.Sort(hashed)
	plainKeys = make([][]byte, len(hashed))
	updates = make([]Update, len(hashed))
	for i, hashedKey := range hashed {
		key := preimages[hashedKey]
		key2 := preimages2[hashedKey]
		plainKey := make([]byte, len(key)+len(key2))
		copy(plainKey[:], key)
		if key2 != nil {
			copy(plainKey[len(key):], key2)
		}
		plainKeys[i] = plainKey
		u := &updates[i]
		if key2 == nil {
			if balance, ok := ub.balances[string(key)]; ok {
				u.Flags |= BalanceUpdate
				u.Balance.Set(balance)
			}
			if nonce, ok := ub.nonces[string(key)]; ok {
				u.Flags |= NonceUpdate
				u.Nonce = nonce
			}
			if codeHash, ok := ub.codeHashes[string(key)]; ok {
				u.Flags |= CodeUpdate
				copy(u.CodeHash[:], codeHash[:])
			}
			if _, del := ub.deletes[string(key)]; del {
				u.Flags = DeleteUpdate
				continue
			}
		} else {
			if dm, ok1 := ub.deletes2[string(key)]; ok1 {
				if _, ok2 := dm[string(key2)]; ok2 {
					u.Flags = DeleteUpdate
					continue
				}
			}
			if sm, ok1 := ub.storages[string(key)]; ok1 {
				if storage, ok2 := sm[string(key2)]; ok2 {
					u.Flags |= StorageUpdate
					u.Storage = [length.Hash]byte{}
					u.StorageLen = len(storage)
					copy(u.Storage[:], storage)
				}
			}
		}
	}
	return
}

func WrapKeyUpdatesParallel(tb testing.TB, mode Mode, hasher keyHasher, keys [][]byte, updates []Update) *Updates {
	tb.Helper()

	upd := NewUpdates(mode, tb.TempDir(), hasher)
	upd.SetConcurrentCommitment(true)
	for i, key := range keys {
		ks := toStringZeroCopy(key)
		upd.TouchPlainKey(ks, nil, func(c *KeyUpdate, _ []byte) {
			c.plainKey = ks
			c.hashedKey = hasher(key)
			c.update = &updates[i]
		})
	}
	return upd
}

func WrapKeyUpdates(tb testing.TB, mode Mode, hasher keyHasher, keys [][]byte, updates []Update) *Updates {
	tb.Helper()

	upd := NewUpdates(mode, tb.TempDir(), hasher)
	for i, key := range keys {
		upd.TouchPlainKey(string(key), nil, func(c *KeyUpdate, _ []byte) {
			c.plainKey = string(key)
			c.hashedKey = hasher(key)
			c.update = &updates[i]
		})
	}
	return upd
}

// it's caller problem to keep track of upd contents. If given Updates is not empty, it will NOT be cleared before adding new keys
func WrapKeyUpdatesInto(tb testing.TB, upd *Updates, keys [][]byte, updates []Update) {
	tb.Helper()
	for i, key := range keys {
		upd.TouchPlainKey(string(key), nil, func(c *KeyUpdate, _ []byte) {
			c.update = &updates[i]
		})
	}
}

type ParallelMockState struct {
	MockState
	accMu  sync.Mutex
	stoMu  sync.Mutex
	commMu sync.RWMutex
}
