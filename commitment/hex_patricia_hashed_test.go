/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package commitment

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"golang.org/x/crypto/sha3"
)

// In memory commitment and state to use with the tests
type MockState struct {
	numBuf [binary.MaxVarintLen64]byte
	sm     map[string][]byte // backbone of the state
	cm     map[string][]byte // backbone of the commitments
}

func NewMockState() *MockState {
	return &MockState{
		sm: make(map[string][]byte),
		cm: make(map[string][]byte),
	}
}

func (ms MockState) branchFn(prefix []byte) ([]byte, error) {
	if exBytes, ok := ms.cm[string(prefix)]; ok {
		return exBytes, nil
	}
	return nil, nil
}

func (ms MockState) accountFn(plainKey []byte, cell *Cell) error {
	exBytes, ok := ms.sm[string(plainKey)]
	if !ok {
		return fmt.Errorf("accountFn not found key [%x]", plainKey)
	}
	var ex Update
	pos, err := ex.decode(exBytes, 0)
	if err != nil {
		return fmt.Errorf("accountFn decode existing [%x], bytes: [%x]: %w", plainKey, exBytes, err)
	}
	if pos != len(exBytes) {
		return fmt.Errorf("accountFn key [%x] leftover bytes in [%x], comsumed %x", plainKey, exBytes, pos)
	}
	if ex.Flags&STORAGE_UPDATE != 0 {
		return fmt.Errorf("accountFn reading storage item for key [%x]", plainKey)
	}
	if ex.Flags&DELETE_UPDATE != 0 {
		return fmt.Errorf("accountFn reading deleted account for key [%x]", plainKey)
	}
	if ex.Flags&BALANCE_UPDATE != 0 {
		cell.Balance.Set(&ex.Balance)
	} else {
		cell.Balance.Clear()
	}
	if ex.Flags&NONCE_UPDATE != 0 {
		cell.Nonce = ex.Nonce
	} else {
		cell.Nonce = 0
	}
	if ex.Flags&CODE_UPDATE != 0 {
		copy(cell.CodeHash[:], ex.CodeHashOrStorage[:])
	} else {
		cell.CodeHash = [32]byte{}
	}
	return nil
}

func (ms MockState) storageFn(plainKey []byte, cell *Cell) error {
	exBytes, ok := ms.sm[string(plainKey)]
	if !ok {
		return fmt.Errorf("storageFn not found key [%x]", plainKey)
	}
	var ex Update
	pos, err := ex.decode(exBytes, 0)
	if err != nil {
		return fmt.Errorf("storageFn decode existing [%x], bytes: [%x]: %w", plainKey, exBytes, err)
	}
	if pos != len(exBytes) {
		return fmt.Errorf("storageFn key [%x] leftover bytes in [%x], comsumed %x", plainKey, exBytes, pos)
	}
	if ex.Flags&BALANCE_UPDATE != 0 {
		return fmt.Errorf("storageFn reading balance for key [%x]", plainKey)
	}
	if ex.Flags&NONCE_UPDATE != 0 {
		return fmt.Errorf("storageFn reading nonce for key [%x]", plainKey)
	}
	if ex.Flags&CODE_UPDATE != 0 {
		return fmt.Errorf("storageFn reading codeHash for key [%x]", plainKey)
	}
	if ex.Flags&DELETE_UPDATE != 0 {
		return fmt.Errorf("storageFn reading deleted item for key [%x]", plainKey)
	}
	if ex.Flags&STORAGE_UPDATE != 0 {
		copy(cell.Storage[:], ex.CodeHashOrStorage[:])
	} else {
		cell.Storage = [32]byte{}
	}
	return nil
}

func (ms *MockState) applyPlainUpdates(plainKeys [][]byte, updates []Update) error {
	for i, key := range plainKeys {
		update := updates[i]
		if update.Flags&DELETE_UPDATE != 0 {
			delete(ms.sm, string(key))
		} else {
			if exBytes, ok := ms.sm[string(key)]; ok {
				var ex Update
				pos, err := ex.decode(exBytes, 0)
				if err != nil {
					return fmt.Errorf("applyPlainUpdates decode existing [%x], bytes: [%x]: %w", key, exBytes, err)
				}
				if pos != len(exBytes) {
					return fmt.Errorf("applyPlainUpdates key [%x] leftover bytes in [%x], comsumed %x", key, exBytes, pos)
				}
				if update.Flags&BALANCE_UPDATE != 0 {
					ex.Flags |= BALANCE_UPDATE
					ex.Balance.Set(&update.Balance)
				}
				if update.Flags&NONCE_UPDATE != 0 {
					ex.Flags |= NONCE_UPDATE
					ex.Nonce = update.Nonce
				}
				if update.Flags&CODE_UPDATE != 0 {
					ex.Flags |= CODE_UPDATE
					copy(ex.CodeHashOrStorage[:], update.CodeHashOrStorage[:])
				}
				if update.Flags&STORAGE_UPDATE != 0 {
					ex.Flags |= STORAGE_UPDATE
					copy(ex.CodeHashOrStorage[:], update.CodeHashOrStorage[:])
				}
				ms.sm[string(key)] = ex.encode(nil, ms.numBuf[:])
			} else {
				ms.sm[string(key)] = update.encode(nil, ms.numBuf[:])
			}
		}
	}
	return nil
}

func (ms *MockState) applyBranchNodeUpdates(updates map[string][]byte) {
	for key, update := range updates {
		ms.cm[key] = update
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
	codeHashes map[string][32]byte
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
		codeHashes: make(map[string][32]byte),
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

func (ub *UpdateBuilder) CodeHash(addr string, hash [32]byte) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.deletes, sk)
	ub.codeHashes[sk] = hash
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
func (ub *UpdateBuilder) Build() (plainKeys, hashedKeys [][]byte, updates []Update) {
	var hashed []string
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
	sort.Strings(hashed)
	plainKeys = make([][]byte, len(hashed))
	hashedKeys = make([][]byte, len(hashed))
	updates = make([]Update, len(hashed))
	for i, hashedKey := range hashed {
		hashedKeys[i] = []byte(hashedKey)
		key := preimages[hashedKey]
		key2 := preimages2[hashedKey]
		plainKey := make([]byte, len(key)+len(key2))
		copy(plainKey[:], []byte(key))
		if key2 != nil {
			copy(plainKey[len(key):], []byte(key2))
		}
		plainKeys[i] = plainKey
		u := &updates[i]
		if key2 == nil {
			if balance, ok := ub.balances[string(key)]; ok {
				u.Flags |= BALANCE_UPDATE
				u.Balance.Set(balance)
			}
			if nonce, ok := ub.nonces[string(key)]; ok {
				u.Flags |= NONCE_UPDATE
				u.Nonce = nonce
			}
			if codeHash, ok := ub.codeHashes[string(key)]; ok {
				u.Flags |= CODE_UPDATE
				copy(u.CodeHashOrStorage[:], codeHash[:])
			}
		} else {
			if sm, ok1 := ub.storages[string(key)]; ok1 {
				if storage, ok2 := sm[string(key2)]; ok2 {
					u.Flags |= STORAGE_UPDATE
					u.CodeHashOrStorage = [32]byte{}
					copy(u.CodeHashOrStorage[32-len(storage):], storage)
				}
			}
		}
	}
	return
}

func TestEmptyState(t *testing.T) {
	ms := NewMockState()
	hph := NewHexPatriciaHashed(1, ms.branchFn, ms.accountFn, ms.storageFn)
	hph.SetTrace(false)
	plainKeys, hashedKeys, updates := NewUpdateBuilder().
		Balance("00", 4).
		Balance("01", 5).
		Balance("02", 6).
		Balance("03", 7).
		Balance("04", 8).
		Storage("04", "01", "0401").
		Storage("03", "56", "050505").
		Storage("03", "57", "060606").
		Balance("05", 9).
		Storage("05", "02", "8989").
		Storage("05", "04", "9898").
		Build()
	if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
		t.Fatal(err)
	}
	branchNodeUpdates, err := hph.ProcessUpdates(plainKeys, hashedKeys, updates)
	if err != nil {
		t.Fatal(err)
	}
	ms.applyBranchNodeUpdates(branchNodeUpdates)
	fmt.Printf("1. Generated updates\n")
	var keys []string
	for key := range branchNodeUpdates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		branchNodeUpdate := branchNodeUpdates[key]
		fmt.Printf("%x => %s\n", key, branchToString(branchNodeUpdate))
	}
	// More updates
	hph.Reset()
	plainKeys, hashedKeys, updates = NewUpdateBuilder().
		Storage("03", "58", "050505").
		Build()
	if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
		t.Fatal(err)
	}
	branchNodeUpdates, err = hph.ProcessUpdates(plainKeys, hashedKeys, updates)
	if err != nil {
		t.Fatal(err)
	}
	ms.applyBranchNodeUpdates(branchNodeUpdates)
	fmt.Printf("2. Generated updates\n")
	keys = keys[:0]
	for key := range branchNodeUpdates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		branchNodeUpdate := branchNodeUpdates[key]
		fmt.Printf("%x => %s\n", key, branchToString(branchNodeUpdate))
	}
	// More updates
	hph.Reset()
	plainKeys, hashedKeys, updates = NewUpdateBuilder().
		Storage("03", "58", "070807").
		Build()
	if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
		t.Fatal(err)
	}
	branchNodeUpdates, err = hph.ProcessUpdates(plainKeys, hashedKeys, updates)
	if err != nil {
		t.Fatal(err)
	}
	ms.applyBranchNodeUpdates(branchNodeUpdates)
	fmt.Printf("3. Generated updates\n")
	keys = keys[:0]
	for key := range branchNodeUpdates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		branchNodeUpdate := branchNodeUpdates[key]
		fmt.Printf("%x => %s\n", key, branchToString(branchNodeUpdate))
	}
}
