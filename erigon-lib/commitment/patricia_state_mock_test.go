package commitment

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"
	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
)

// In memory commitment and state to use with the tests
type MockState struct {
	t      *testing.T
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

func (ms MockState) branchFn(prefix []byte) ([]byte, error) {
	if exBytes, ok := ms.cm[string(prefix)]; ok {
		return exBytes[2:], nil // Skip touchMap, but keep afterMap
	}
	return nil, nil
}

func (ms MockState) accountFn(plainKey []byte, cell *Cell) error {
	exBytes, ok := ms.sm[string(plainKey[:])]
	if !ok {
		ms.t.Logf("accountFn not found key [%x]", plainKey)
		cell.Delete = true
		return nil
	}
	var ex Update
	pos, err := ex.Decode(exBytes, 0)
	if err != nil {
		ms.t.Fatalf("accountFn decode existing [%x], bytes: [%x]: %v", plainKey, exBytes, err)
		return nil
	}
	if pos != len(exBytes) {
		ms.t.Fatalf("accountFn key [%x] leftover bytes in [%x], comsumed %x", plainKey, exBytes, pos)
		return nil
	}
	if ex.Flags&StorageUpdate != 0 {
		ms.t.Logf("accountFn reading storage item for key [%x]", plainKey)
		return fmt.Errorf("storage read by accountFn")
	}
	if ex.Flags&DeleteUpdate != 0 {
		ms.t.Fatalf("accountFn reading deleted account for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&BalanceUpdate != 0 {
		cell.Balance.Set(&ex.Balance)
	} else {
		cell.Balance.Clear()
	}
	if ex.Flags&NonceUpdate != 0 {
		cell.Nonce = ex.Nonce
	} else {
		cell.Nonce = 0
	}
	if ex.Flags&CodeUpdate != 0 {
		copy(cell.CodeHash[:], ex.CodeHashOrStorage[:])
	} else {
		copy(cell.CodeHash[:], EmptyCodeHash)
	}
	return nil
}

func (ms MockState) storageFn(plainKey []byte, cell *Cell) error {
	exBytes, ok := ms.sm[string(plainKey[:])]
	if !ok {
		ms.t.Logf("storageFn not found key [%x]", plainKey)
		cell.Delete = true
		return nil
	}
	var ex Update
	pos, err := ex.Decode(exBytes, 0)
	if err != nil {
		ms.t.Fatalf("storageFn decode existing [%x], bytes: [%x]: %v", plainKey, exBytes, err)
		return nil
	}
	if pos != len(exBytes) {
		ms.t.Fatalf("storageFn key [%x] leftover bytes in [%x], comsumed %x", plainKey, exBytes, pos)
		return nil
	}
	if ex.Flags&BalanceUpdate != 0 {
		ms.t.Logf("storageFn reading balance for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&NonceUpdate != 0 {
		ms.t.Fatalf("storageFn reading nonce for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&CodeUpdate != 0 {
		ms.t.Fatalf("storageFn reading codeHash for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&DeleteUpdate != 0 {
		ms.t.Fatalf("storageFn reading deleted item for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&StorageUpdate != 0 {
		copy(cell.Storage[:], ex.CodeHashOrStorage[:])
		cell.StorageLen = len(ex.CodeHashOrStorage)
	} else {
		cell.StorageLen = 0
		cell.Storage = [length.Hash]byte{}
	}
	return nil
}

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
				if update.Flags&BalanceUpdate != 0 {
					ex.Flags |= BalanceUpdate
					ex.Balance.Set(&update.Balance)
				}
				if update.Flags&NonceUpdate != 0 {
					ex.Flags |= NonceUpdate
					ex.Nonce = update.Nonce
				}
				if update.Flags&CodeUpdate != 0 {
					ex.Flags |= CodeUpdate
					copy(ex.CodeHashOrStorage[:], update.CodeHashOrStorage[:])
				}
				if update.Flags&StorageUpdate != 0 {
					ex.Flags |= StorageUpdate
					copy(ex.CodeHashOrStorage[:], update.CodeHashOrStorage[:])
				}
				ms.sm[string(key)] = ex.Encode(nil, ms.numBuf[:])
			} else {
				ms.sm[string(key)] = update.Encode(nil, ms.numBuf[:])
			}
		}
	}
	return nil
}

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
func (ub *UpdateBuilder) Build() (plainKeys, hashedKeys [][]byte, updates []Update) {
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
	hashedKeys = make([][]byte, len(hashed))
	updates = make([]Update, len(hashed))
	for i, hashedKey := range hashed {
		hashedKeys[i] = []byte(hashedKey)
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
				copy(u.CodeHashOrStorage[:], codeHash[:])
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
					u.CodeHashOrStorage = [length.Hash]byte{}
					u.ValLength = len(storage)
					copy(u.CodeHashOrStorage[:], storage)
				}
			}
		}
	}
	return
}
