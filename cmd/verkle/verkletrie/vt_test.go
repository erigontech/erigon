package verkletrie

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"
	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"

	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
)

func testDb(t *testing.T, prefixLen int) (string, kv.RwDB) {
	t.Helper()
	path := t.TempDir()
	t.Cleanup(func() { os.RemoveAll(path) })
	logger := log.New()
	db := mdbx.NewMDBX(logger).Path(filepath.Join(path, "db4")).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	t.Cleanup(db.Close)
	return path, db
}

func Test_VerkeExternal_ResetThenSingularUpdates(t *testing.T) {
	ms := NewMockState(t)
	pth, db := testDb(t, length.Addr)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer db.Close()
	defer tx.Rollback()

	hph := NewExternalVerkleTree(&tx, path.Join(pth, "verkletmp"))
	hph.ResetFns(ms.branchFn, ms.accountFn, ms.storageFn)

	plainKeys, hashedKeys, updates := NewUpdateBuilder().
		Balance("d191539976efcd7cb4ae34933acc39621366c2e5", 4).
		Balance("85c6e622a66c01b5c278964f7df960c01f4cfe90", 5).
		Balance("5ba848f30a3cce401051a743027c2f483e2c7c8c", 6).
		Balance("578a5ebbf1ef6c4e372de541a0b2044dfc52f3f7", 7).
		Balance("b3368add80b7a0c17ad5c1facd58f99856cf8406", 8).
		Storage("954ab86ded074c4fadc1586f5c1dd52810bd4e02", "c2f0faed16f5c6aaa32b046a636e13c7520b281f1dfdd18d7bfee19879092e41", "0401").
		Storage("eb73939a81802cc30228acd1e2309045f6195c8d", "163c742b3b3fbaf5de56aac0477934d0dcd0300927eba72bd3a614658b8750f4", "050505").
		Storage("7aa8cc52453570f8680160c0fe068758928ca7df", "3695bded6afb4ef45c898594a6197e0cb109dbc426a4d9e18b37ae75ae57eac9", "060606").
		Storage("5f6d2a26175ffbda897b6168fd0c1839bc6fc465", "53eada64dc69437bb52226e121430c81e94beed4486152b0078c246e37e2c729", "8989").
		Storage("5f6d2a26175ffbda897b6168fd0c1839bc6fc465", "de851b3fe7b36e49873a97d0f44eb5cc46bb6573c06818fe98f8a3d8894afef4", "9898").
		Build()

	err = ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	for i, pk := range plainKeys {
		hashedKeys[i] = vtree.GetTreeKeyVersion(pk)
	}

	firstRootHash, branchNodeUpdates, err := hph.ReviewKeys(plainKeys, hashedKeys)
	require.NoError(t, err)

	t.Logf("root hash %x\n", firstRootHash)

	ms.applyBranchNodeUpdates(branchNodeUpdates)

	fmt.Printf("1. Generated updates\n")
	//renderUpdates(branchNodeUpdates)

	// More updates
	hph.Reset()
	//hph.SetTrace(false)
	plainKeys, hashedKeys, updates = NewUpdateBuilder().
		Storage("03", "58", "050505").
		Build()
	err = ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	secondRootHash, branchNodeUpdates, err := hph.ReviewKeys(plainKeys, hashedKeys)
	require.NoError(t, err)
	require.NotEqualValues(t, firstRootHash, secondRootHash)

	ms.applyBranchNodeUpdates(branchNodeUpdates)
	fmt.Printf("2. Generated single update\n")
	//renderUpdates(branchNodeUpdates)

	// More updates
	hph.Reset()
	//hph.SetTrace(false)
	plainKeys, hashedKeys, updates = NewUpdateBuilder().
		Storage("03", "58", "070807").
		Build()
	err = ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	thirdRootHash, branchNodeUpdates, err := hph.ReviewKeys(plainKeys, hashedKeys)
	require.NoError(t, err)
	require.NotEqualValues(t, secondRootHash, thirdRootHash)

	ms.applyBranchNodeUpdates(branchNodeUpdates)
	fmt.Printf("3. Generated single update\n")
	//renderUpdates(branchNodeUpdates)
}

// In memory commitment and state to use with the tests
type MockState struct {
	t      *testing.T
	sm     map[string][]byte                // backbone of the state
	cm     map[string]commitment.BranchData // backbone of the commitments
	numBuf [binary.MaxVarintLen64]byte
}

func NewMockState(t *testing.T) *MockState {
	t.Helper()
	return &MockState{
		t:  t,
		sm: make(map[string][]byte),
		cm: make(map[string]commitment.BranchData),
	}
}

func (ms MockState) branchFn(prefix []byte) ([]byte, error) {
	if exBytes, ok := ms.cm[string(prefix)]; ok {
		return exBytes[2:], nil // Skip touchMap, but keep afterMap
	}
	return nil, nil
}

func (ms MockState) accountFn(plainKey []byte, cell *commitment.Cell) error {
	exBytes, ok := ms.sm[string(plainKey[:])]
	if !ok {
		ms.t.Logf("accountFn not found key [%x]", plainKey)
		cell.Delete = true
		return nil
	}
	var ex commitment.Update
	pos, err := ex.Decode(exBytes, 0)
	if err != nil {
		ms.t.Fatalf("accountFn decode existing [%x], bytes: [%x]: %v", plainKey, exBytes, err)
		return nil
	}
	if pos != len(exBytes) {
		ms.t.Fatalf("accountFn key [%x] leftover bytes in [%x], comsumed %x", plainKey, exBytes, pos)
		return nil
	}
	if ex.Flags&commitment.STORAGE_UPDATE != 0 {
		ms.t.Logf("accountFn reading storage item for key [%x]", plainKey)
		return fmt.Errorf("storage read by accountFn")
	}
	if ex.Flags&commitment.DELETE_UPDATE != 0 {
		ms.t.Fatalf("accountFn reading deleted account for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&commitment.BALANCE_UPDATE != 0 {
		cell.Balance.Set(&ex.Balance)
	} else {
		cell.Balance.Clear()
	}
	if ex.Flags&commitment.NONCE_UPDATE != 0 {
		cell.Nonce = ex.Nonce
	} else {
		cell.Nonce = 0
	}
	if ex.Flags&commitment.CODE_UPDATE != 0 {
		copy(cell.CodeHash[:], ex.CodeHashOrStorage[:])
	} else {
		copy(cell.CodeHash[:], commitment.EmptyCodeHash)
	}
	return nil
}

func (ms MockState) storageFn(plainKey []byte, cell *commitment.Cell) error {
	exBytes, ok := ms.sm[string(plainKey[:])]
	if !ok {
		ms.t.Logf("storageFn not found key [%x]", plainKey)
		cell.Delete = true
		return nil
	}
	var ex commitment.Update
	pos, err := ex.Decode(exBytes, 0)
	if err != nil {
		ms.t.Fatalf("storageFn decode existing [%x], bytes: [%x]: %v", plainKey, exBytes, err)
		return nil
	}
	if pos != len(exBytes) {
		ms.t.Fatalf("storageFn key [%x] leftover bytes in [%x], comsumed %x", plainKey, exBytes, pos)
		return nil
	}
	if ex.Flags&commitment.BALANCE_UPDATE != 0 {
		ms.t.Logf("storageFn reading balance for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&commitment.NONCE_UPDATE != 0 {
		ms.t.Fatalf("storageFn reading nonce for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&commitment.CODE_UPDATE != 0 {
		ms.t.Fatalf("storageFn reading codeHash for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&commitment.DELETE_UPDATE != 0 {
		ms.t.Fatalf("storageFn reading deleted item for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&commitment.STORAGE_UPDATE != 0 {
		copy(cell.Storage[:], ex.CodeHashOrStorage[:])
		cell.StorageLen = len(ex.CodeHashOrStorage)
	} else {
		cell.StorageLen = 0
		cell.Storage = [length.Hash]byte{}
	}
	return nil
}

func (ms *MockState) applyPlainUpdates(plainKeys [][]byte, updates []commitment.Update) error {
	for i, key := range plainKeys {
		update := updates[i]
		if update.Flags&commitment.DELETE_UPDATE != 0 {
			delete(ms.sm, string(key))
		} else {
			if exBytes, ok := ms.sm[string(key)]; ok {
				var ex commitment.Update
				pos, err := ex.Decode(exBytes, 0)
				if err != nil {
					return fmt.Errorf("applyPlainUpdates decode existing [%x], bytes: [%x]: %w", key, exBytes, err)
				}
				if pos != len(exBytes) {
					return fmt.Errorf("applyPlainUpdates key [%x] leftover bytes in [%x], comsumed %x", key, exBytes, pos)
				}
				if update.Flags&commitment.BALANCE_UPDATE != 0 {
					ex.Flags |= commitment.BALANCE_UPDATE
					ex.Balance.Set(&update.Balance)
				}
				if update.Flags&commitment.NONCE_UPDATE != 0 {
					ex.Flags |= commitment.NONCE_UPDATE
					ex.Nonce = update.Nonce
				}
				if update.Flags&commitment.CODE_UPDATE != 0 {
					ex.Flags |= commitment.CODE_UPDATE
					copy(ex.CodeHashOrStorage[:], update.CodeHashOrStorage[:])
				}
				if update.Flags&commitment.STORAGE_UPDATE != 0 {
					ex.Flags |= commitment.STORAGE_UPDATE
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

func (ms *MockState) applyBranchNodeUpdates(updates map[string]commitment.BranchData) {
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
func (ub *UpdateBuilder) Build() (plainKeys, hashedKeys [][]byte, updates []commitment.Update) {
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
	updates = make([]commitment.Update, len(hashed))
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
				u.Flags |= commitment.BALANCE_UPDATE
				u.Balance.Set(balance)
			}
			if nonce, ok := ub.nonces[string(key)]; ok {
				u.Flags |= commitment.NONCE_UPDATE
				u.Nonce = nonce
			}
			if codeHash, ok := ub.codeHashes[string(key)]; ok {
				u.Flags |= commitment.CODE_UPDATE
				copy(u.CodeHashOrStorage[:], codeHash[:])
			}
			if _, del := ub.deletes[string(key)]; del {
				u.Flags = commitment.DELETE_UPDATE
				continue
			}
		} else {
			if dm, ok1 := ub.deletes2[string(key)]; ok1 {
				if _, ok2 := dm[string(key2)]; ok2 {
					u.Flags = commitment.DELETE_UPDATE
					continue
				}
			}
			if sm, ok1 := ub.storages[string(key)]; ok1 {
				if storage, ok2 := sm[string(key2)]; ok2 {
					u.Flags |= commitment.STORAGE_UPDATE
					u.CodeHashOrStorage = [length.Hash]byte{}
					u.ValLength = len(storage)
					copy(u.CodeHashOrStorage[:], storage)
				}
			}
		}
	}
	return
}
