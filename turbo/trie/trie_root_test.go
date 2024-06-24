package trie_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv/dbutils"

	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

// seedInitialAccounts uses the provided hashes to commit simpleAccountValBytes
// at each corresponding key in the HashedAccounts table.
func seedInitialAccounts(t *testing.T, db kv.RwDB, hashes []libcommon.Hash) {
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	for _, hash := range hashes {
		//t.Logf("Seeding initial account with hash %s", hash)
		err = tx.Put(kv.HashedAccounts, hash[:], simpleAccountValBytes)
		require.NoError(t, err)

	}
	err = tx.Commit()
	require.NoError(t, err)
}

// seedModifiedAccounts uses the provided hashes to commit
// simpleModifiedAccountValBytes at each corresponding key in the HashedAccounts
// table.  It returns a slice of keys to be used when constructing the retain
// list for the FlatDBTrie computations.
func seedModifiedAccounts(t *testing.T, db kv.RwDB, hashes []libcommon.Hash) [][]byte {
	toRetain := make([][]byte, 0, len(hashes))
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	for _, hash := range hashes {
		//t.Logf("Seeding modified account with hash %s", hash)
		err = tx.Put(kv.HashedAccounts, hash[:], simpleModifiedAccountValBytes)
		require.NoError(t, err)
		toRetain = append(toRetain, append([]byte{}, hash[:]...))
	}
	err = tx.Commit()
	require.NoError(t, err)
	return toRetain
}

// seedInitialStorage creates a single account with hash storageAccountHash in
// the HashedAccounts table, as well as a storage entry in HashedStorage for
// this account with value storageInitialValue for each provided hash.  A slice
// of the constructed storage keys is returned.
func seedInitialStorage(t *testing.T, db kv.RwDB, hashes []libcommon.Hash) [][]byte {
	// We seed a single account, and all storage keys belong to this account.
	keys := make([][]byte, 0, len(hashes))
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	err = tx.Put(kv.HashedAccounts, storageAccountHash[:], simpleContractAccountValBytes)
	require.NoError(t, err)
	var storageKey [72]byte
	binary.BigEndian.PutUint64(storageKey[32:40], 1)
	for _, hash := range hashes {
		copy(storageKey[:32], storageAccountHash[:])
		copy(storageKey[40:], hash[:])
		//t.Logf("Seeding storage with key 0x%x", storageKey)
		err = tx.Put(kv.HashedStorage, storageKey[:], storageInitialValue[:])
		require.NoError(t, err)
		keys = append(keys, append([]byte{}, storageKey[:]...))
	}
	err = tx.Commit()
	require.NoError(t, err)
	return keys
}

// seedModifedStorage writes a storage entry in HashedStorage for
// storageAccountHash with value storageModifiedValue for each provided hash.  A
// slice containing the modified keys is returned to help the caller construct
// the retain list necessary for the FlatDBTrie computations.
func seedModifiedStorage(t *testing.T, db kv.RwDB, hashes []libcommon.Hash) [][]byte {
	// Modify this same account's storage entries.  For now, this test only
	// validates modifying existing or adding new keys (no deletions).
	toRetain := make([][]byte, 0, len(hashes))
	var storageKey [72]byte
	binary.BigEndian.PutUint64(storageKey[32:40], 1)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	for _, hash := range hashes {
		copy(storageKey[:32], storageAccountHash[:])
		copy(storageKey[40:], hash[:])
		//t.Logf("Seeding storage with modified hash 0x%x", storageKey)
		err = tx.Put(kv.HashedStorage, storageKey[:], storageModifiedValue[:])
		require.NoError(t, err)
		toRetain = append(toRetain, append([]byte{}, storageKey[:]...))
	}
	err = tx.Commit()
	require.NoError(t, err)
	return toRetain
}

// rebuildFlatDBTrieHash will re-compute the root hash using the given retain
// list.  Even without a retain list, this is not a no-op in the accounts case,
// as the root of the trie is not actually stored and must be re-computed
// regardless.  However, for storage entries at least one element must be in the
// retain list or the root is simply returned.
func rebuildFlatDBTrieHash(t *testing.T, rl *trie.RetainList, db kv.RoDB) libcommon.Hash {
	t.Helper()
	//startTime := time.Now()
	loader := trie.NewFlatDBTrieLoader("test", rl, nil, nil, false)
	tx, err := db.BeginRo(context.Background())
	defer tx.Rollback()
	require.NoError(t, err)
	hash, err := loader.CalcTrieRoot(tx, nil)
	tx.Rollback()
	require.NoError(t, err)
	//t.Logf("Rebuilt hash is %s and took %v", hash, time.Since(startTime))
	return hash
}

// proveFlatDB will generate an account proof result utilizing the FlatDBTrie
// hashing mechanism.  The proofKeys cannot span more than one account (ie, an
// account and its storage nodes is acceptable).  It returns the root hash for
// the computation as well as the proof result.
func proveFlatDB(t *testing.T, db kv.RoDB, accountMissing bool, retainKeys, proofKeys [][]byte) (libcommon.Hash, *accounts.AccProofResult) {
	t.Helper()
	//startTime := time.Now()
	rl := trie.NewRetainList(0)
	for _, retainKey := range retainKeys {
		rl.AddKeyWithMarker(retainKey, true)
	}
	loader := trie.NewFlatDBTrieLoader("test", rl, nil, nil, false)
	acc := &accounts.Account{}
	if !accountMissing {
		acc.Incarnation = 1
		acc.Initialised = true
	}
	pr := trie.NewManualProofRetainer(t, acc, rl, proofKeys)
	loader.SetProofRetainer(pr)
	tx, err := db.BeginRo(context.Background())
	defer tx.Rollback()
	require.NoError(t, err)
	hash, err := loader.CalcTrieRoot(tx, nil)
	tx.Rollback()
	require.NoError(t, err)
	//t.Logf("Proof root hash is %s and took %v", hash, time.Since(startTime))
	res, err := pr.ProofResult()
	require.NoError(t, err)
	return hash, res
}

// logTrieTables simply writes the TrieOfAccounts and TrieOfStorage to the test
// output.  It can be very helpful when debugging failed fuzzing cases.
func logTrieTables(t *testing.T, db kv.RoDB) { //nolint
	t.Helper()
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	trieAccC, err := tx.Cursor(kv.TrieOfAccounts)
	require.NoError(t, err)
	defer trieAccC.Close()
	var k, v []byte
	t.Logf("TrieOfAccounts iteration begin")
	for k, v, err = trieAccC.First(); err == nil && k != nil; k, v, err = trieAccC.Next() {
		t.Logf("\t%x=%x", k, v)
	}
	require.NoError(t, err)
	t.Logf("TrieOfAccounts iteration end")
	trieStorageC, err := tx.CursorDupSort(kv.TrieOfStorage)
	require.NoError(t, err)
	defer trieStorageC.Close()
	t.Logf("TrieOfStorage iteration begin")
	for k, v, err = trieStorageC.First(); err == nil && k != nil; k, v, err = trieStorageC.Next() {
		t.Logf("\t%x=%x", k, v)
	}
	require.NoError(t, err)
	t.Logf("TrieStorage iteration end")
}

var (
	// simpleAccountValBytes is a simple marshaled EoA without storage or balance
	simpleAccountValBytes = func() []byte {
		accVal := accounts.Account{Nonce: 1, Initialised: true}
		encodedAccVal := make([]byte, accVal.EncodingLengthForStorage())
		accVal.EncodeForStorage(encodedAccVal)
		return encodedAccVal
	}()

	simpleModifiedAccountValBytes = func() []byte {
		accVal := accounts.Account{Nonce: 2, Initialised: true}
		encodedAccVal := make([]byte, accVal.EncodingLengthForStorage())
		accVal.EncodeForStorage(encodedAccVal)
		return encodedAccVal
	}()

	// simpleContractAccountValBytes is a simple marshaled account with storage and code
	simpleContractAccountValBytes = func() []byte {
		accVal := accounts.Account{Nonce: 1, Initialised: true, Incarnation: 1, CodeHash: libcommon.Hash{9}}
		encodedAccVal := make([]byte, accVal.EncodingLengthForStorage())
		accVal.EncodeForStorage(encodedAccVal)
		return encodedAccVal
	}()

	storageAccountHash     = libcommon.Hash{1}
	storageAccountCodeHash = libcommon.Hash{9}
	storageInitialValue    = libcommon.Hash{2}
	storageModifiedValue   = libcommon.Hash{3}

	// missingHash is a constant value that is impossible for the fuzzers to
	// generate (since the last 26 bytes of all fuzzing keys must be 0).  The
	// tests know that when this value is encountered the expected result should
	// be 'missing', ie in the storage proof the value should be 0, and in the
	// account proof the nonce/codeHash/storageHash/balance should be empty.
	missingHash = libcommon.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
)

// naiveTriesAndHashFromDB constructs the naive trie implementation by simply adding
// all of the accounts and storage keys linearly.  It also returns a map of
// storage tries
func naiveTriesAndHashFromDB(t *testing.T, db kv.RoDB) (*trie.Trie, *trie.Trie, libcommon.Hash) {
	t.Helper()
	startTime := time.Now()
	testTrie := trie.NewTestRLPTrie(libcommon.Hash{})
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	ac, err := tx.Cursor(kv.HashedAccounts)
	require.NoError(t, err)
	defer ac.Close()
	var k, v []byte
	var storageTrie *trie.Trie
	for k, v, err = ac.First(); k != nil; k, v, err = ac.Next() {
		accHash := k[:32]
		var acc accounts.Account
		err := acc.DecodeForStorage(v)
		require.NoError(t, err)
		sc, err := tx.Cursor(kv.HashedStorage)
		require.NoError(t, err)
		defer sc.Close()
		startStorageKey := dbutils.GenerateCompositeStorageKey(libcommon.BytesToHash(k), acc.Incarnation, libcommon.Hash{})
		endStorageKey := dbutils.GenerateCompositeStorageKey(libcommon.BytesToHash(k), acc.Incarnation+1, libcommon.Hash{})
		storageTrie = trie.NewTestRLPTrie(libcommon.Hash{})
		for k, v, err = sc.Seek(startStorageKey); k != nil; k, v, err = sc.Next() {
			if bytes.Compare(k, endStorageKey) >= 0 {
				break
			}
			sk := k[32+8:]
			sv := append([]byte{}, v...)
			esv, err := trie.EncodeAsValue(sv)
			require.NoError(t, err)
			storageTrie.Update(sk, esv)
		}
		require.NoError(t, err)
		acc.Root = storageTrie.Hash()
		accBytes := make([]byte, acc.EncodingLengthForHashing())
		acc.EncodeForHashing(accBytes)
		testTrie.Update(accHash, accBytes)
	}
	require.NoError(t, err)

	naiveHash := testTrie.Hash()
	t.Logf("Naive hash is %s and took %v", naiveHash, time.Since(startTime))
	return testTrie, storageTrie, naiveHash
}

// addFuzzTrieSeeds adds a common set of trie hashes which are problematic for
// the flatDB trie hash construction.  Because there is a code path for account
// tries and a slightly separate one for storage tries, we use this common set
// for both.  These seeeds were constructed by executing the fuzzing until a
// failing case was detected, then refining to the minimal set of keys (and
// re-naming nibbles to be more readable if possible").
func addFuzzTrieSeeds(f *testing.F) {
	f.Helper()
	noModificationTests := [][]string{
		{"0x0a00", "0x0bc0", "0x0bd0"},
		{"0xff00", "0xfff0", "0xffff"},
		{"0xa000", "0xaa00", "0xaaa0", "0xb000", "0xcc00", "0xccc0", "0xcdd0"},
		{"0xa000", "0xaa00", "0xbb00", "0xbbb0", "0xbbbb"},
		{"0xa00000", "0xaaa000", "0xaaaa00", "0xaaaaa0", "0xb00000", "0xccc000", "0xccc0c0", "0xcccc00"},
		{"a000", "b000", "b0b0", "bbb0"},
		{"a00000", "bbbb00", "bbbb0b", "bbbbb0", "c00000", "cc0000", "ccc000"},
	}

	for _, tt := range noModificationTests {
		var buffer bytes.Buffer
		fragLen := len(tt[0])
		require.Equal(f, 0, fragLen%2, "Fragments must be even in length")
		for _, fragment := range tt {
			buffer.Write(hexutility.FromHex(fragment))
			require.Len(f, fragment, fragLen, "All fragments must be the same length")
		}
		f.Add(len(tt), 0, buffer.Bytes())
	}

	modificationTests := []struct {
		initial  []string
		modified []string
	}{
		{initial: []string{"0xaa"}, modified: []string{"0xbb"}},
		{initial: []string{"0xa000", "0xa0aa", "0xaaaa"}, modified: []string{"0xa00a"}},
		{initial: []string{"0xa000", "0xa0a0", "0xab00", "0xabb0"}, modified: []string{"0xaa00"}},
		{initial: []string{"0xaaa0", "0xaaaa", "0xaaba", "0xaba0", "0xabb0"}, modified: []string{"0xaaa0", "0xaba0"}},
	}
	for _, tt := range modificationTests {
		var buffer bytes.Buffer
		fragLen := len(tt.initial[0])
		require.Equal(f, 0, fragLen%2, "Fragments must be even in length")
		for _, fragment := range tt.initial {
			buffer.Write(hexutility.FromHex(fragment))
			require.Len(f, fragment, fragLen, "All fragments must be the same length")
		}
		for _, fragment := range tt.modified {
			buffer.Write(hexutility.FromHex(fragment))
			require.Len(f, fragment, fragLen, "All fragments must be the same length")
		}
		f.Add(len(tt.initial), len(tt.modified), buffer.Bytes())
	}
}

// validateFuzzInputs ensures that the raw fuzzing inputs can be mapped into the
// actual required fuzzing structure of a series of hashes.  For the purpose of
// narrowing the search space, we require that all hashes are between 1 and 6
// bytes long.
func validateFuzzInputs(t *testing.T, initialCount, modifiedCount int, hashFragments []byte) ([]libcommon.Hash, []libcommon.Hash) {
	t.Helper()
	if initialCount <= 0 || modifiedCount < 0 || len(hashFragments) == 0 {
		t.Skip("account count must be positive and hashFragments cannot be empty")
	}
	count := initialCount + modifiedCount
	if count > 100 {
		t.Skip("all failures thusfar can be reproduced within 10 keys, so 100 seems like a reasonable cutoff to balance speed of fuzzing with coverage")
	}
	if len(hashFragments)%count != 0 {
		t.Skip("hash fragments must be divisible by accountCount")
	}
	fragmentSize := len(hashFragments) / count
	if fragmentSize > 6 {
		t.Skip("hash fragments cannot exceed 6 bytes")
	}

	uniqueInitialKeys := map[libcommon.Hash]struct{}{}
	uniqueModifiedKeys := map[libcommon.Hash]struct{}{}
	var inititalKeys, modifiedKeys []libcommon.Hash
	for i := 0; i < count; i++ {
		var hash libcommon.Hash
		copy(hash[:], hashFragments[i*fragmentSize:(i+1)*fragmentSize])
		if i < initialCount {
			if _, ok := uniqueInitialKeys[hash]; ok {
				t.Skip("hash fragments must be unique")
			}
			uniqueInitialKeys[hash] = struct{}{}
			inititalKeys = append(inititalKeys, hash)
		} else {
			if _, ok := uniqueModifiedKeys[hash]; ok {
				t.Skip("hash fragments must be unique")
			}
			uniqueModifiedKeys[hash] = struct{}{}
			modifiedKeys = append(modifiedKeys, hash)
		}
	}
	return inititalKeys, modifiedKeys
}
