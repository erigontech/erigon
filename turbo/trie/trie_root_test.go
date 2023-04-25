package trie_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/stretchr/testify/require"
)

// initialFlatDBTrieBuild leverages the stagedsync code to perform the initial
// trie computation while also collecting the assorted hashes and loading them
// into the TrieOfAccounts and TrieOfStorage tables
func initialFlatDBTrieBuild(t *testing.T, db kv.RwDB) libcommon.Hash {
	t.Helper()
	startTime := time.Now()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	stageTrieCfg := stagedsync.StageTrieCfg(db, false, false, false, t.TempDir(), nil, nil, false, nil)
	hash, err := stagedsync.RegenerateIntermediateHashes("test", tx, stageTrieCfg, libcommon.Hash{}, context.Background())
	require.NoError(t, err)
	tx.Commit()
	t.Logf("Initial hash is %s and took %v", hash, time.Since(startTime))
	return hash
}

// rebuildFlatDBTrieHash will re-compute the root hash using the given retain
// list.  Even without a retain list, this is not a no-op in the accounts case,
// as the root of the trie is not actually stored and must be re-computed
// regardless.  However, for storage entries at least one element must be in the
// retain list or the root is simply returned.
func rebuildFlatDBTrieHash(t *testing.T, rl *trie.RetainList, db kv.RoDB) libcommon.Hash {
	t.Helper()
	startTime := time.Now()
	loader := trie.NewFlatDBTrieLoader("test", rl, nil, nil, false)
	tx, err := db.BeginRo(context.Background())
	defer tx.Rollback()
	require.NoError(t, err)
	hash, err := loader.CalcTrieRoot(tx, nil)
	tx.Rollback()
	require.NoError(t, err)
	t.Logf("Rebuilt hash is %s and took %v", hash, time.Since(startTime))
	return hash
}

// logTrieTables simply writes the TrieOfAccounts and TrieOfStorage to the test
// output.  It can be very helpful when debugging failed fuzzing cases.
func logTrieTables(t *testing.T, db kv.RoDB) {
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
)

// naiveTrieHashFromDB constructs the naive trie implementation by simply adding
// all of the accounts and storage keys linearly.
func naiveTrieHashFromDB(t *testing.T, db kv.RoDB) libcommon.Hash {
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
		storageTrie := trie.NewTestRLPTrie(libcommon.Hash{})
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
	return naiveHash
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

// FuzzTrieRootStorage will fuzz assorted storage hash tables.  First, it will
// build an initial hash, adding the storage keys to a single account entry.
// Then, if the set of modifications was empty, it will verify that the same
// hash is re-computed.  Then, it will compute the naive trie hash and compare it
// with the flat db hash.  In the event that the modfiications are empty, this
// check additionally confirms that the initial trie hashing was correct.
// See the validateFuzzInputs function for details on how the paramaters are
// translated to initial and modified hashes.
func FuzzTrieRootStorage(f *testing.F) {
	addFuzzTrieSeeds(f)

	f.Fuzz(func(t *testing.T, initialCount, modifiedCount int, hashFragments []byte) {
		initialKeys, modifiedKeys := validateFuzzInputs(t, initialCount, modifiedCount, hashFragments)

		db := memdb.NewTestDB(t)
		defer db.Close()

		// We seed a single account, and all storage keys belong to this account.
		tx, err := db.BeginRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		accHash := libcommon.Hash{1}
		err = tx.Put(kv.HashedAccounts, accHash[:], simpleContractAccountValBytes)
		require.NoError(t, err)
		var storageKey [72]byte
		binary.BigEndian.PutUint64(storageKey[32:40], 1)
		rl := trie.NewRetainList(0)
		for i, hash := range initialKeys {
			copy(storageKey[:32], accHash[:])
			copy(storageKey[40:], hash[:])
			t.Logf("Seeding with hash 0x%x", storageKey)
			err = tx.Put(kv.HashedStorage, storageKey[:], storageKey[40:])
			require.NoError(t, err)
			if len(modifiedKeys) == 0 && i == 0 {
				// If there are no modifications, we add the first storage key to the
				// retain list, because the inputs are random, this is not necessarily
				// the actual lexicographically first key, but without some retained key
				// then the root hash is simply returned.
				rl.AddKey(storageKey[:])
			}
		}
		err = tx.Commit()
		require.NoError(t, err)

		initialHash := initialFlatDBTrieBuild(t, db)
		logTrieTables(t, db)

		// Modify this same account's storage entries.  For now, this test only
		// validates modifying existing or adding new keys (no deletions).
		tx, err = db.BeginRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		for _, hash := range modifiedKeys {
			copy(storageKey[:32], accHash[:])
			copy(storageKey[40:], hash[:])
			t.Logf("Seeding with modified hash 0x%x", storageKey)
			err = tx.Put(kv.HashedStorage, storageKey[:], storageKey[39:])
			require.NoError(t, err)
			rl.AddKeyWithMarker(storageKey[:], true)
		}
		err = tx.Commit()
		require.NoError(t, err)

		reHash := rebuildFlatDBTrieHash(t, rl, db)
		if len(modifiedKeys) == 0 {
			require.Equal(t, initialHash, reHash)
		}
		naiveHash := naiveTrieHashFromDB(t, db)
		require.Equal(t, reHash, naiveHash)
	})
}

// FuzzTrieRootAccounts will fuzz assorted account hash tables.  First, it will
// build an initial hash, then modify the account hash table and re-compute the
// hash.  If the set of modifications was empty, it will verify that the same
// hash is re-computed.  Then, it will compute the naive trie hash and compare
// it with the flat db hash.  In the event that the modfiications are empty,
// this check additionally confirms that the initial trie hashing was correct.
// See the validateFuzzInputs function for details on how the paramaters are
// translated to initial and modified hashes.
func FuzzTrieRootAccounts(f *testing.F) {
	addFuzzTrieSeeds(f)

	f.Fuzz(func(t *testing.T, initialCount, modifiedCount int, hashFragments []byte) {
		initialKeys, modifiedKeys := validateFuzzInputs(t, initialCount, modifiedCount, hashFragments)

		db := memdb.NewTestDB(t)
		defer db.Close()

		// Seed the accounts into HashedAccounts table
		tx, err := db.BeginRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		for _, hash := range initialKeys {
			t.Logf("Seeding with hash %s", hash)
			err = tx.Put(kv.HashedAccounts, hash[:], simpleAccountValBytes)
			require.NoError(t, err)
		}
		err = tx.Commit()
		require.NoError(t, err)

		initialHash := initialFlatDBTrieBuild(t, db)
		logTrieTables(t, db)

		// Modify the account hash table.  For now, this test only validates
		// modifying existing or adding new keys (no deletions).
		tx, err = db.BeginRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		rl := trie.NewRetainList(0)
		for _, hash := range modifiedKeys {
			t.Logf("Seeding with modified hash %s", hash)
			err = tx.Put(kv.HashedAccounts, hash[:], simpleModifiedAccountValBytes)
			require.NoError(t, err)
			rl.AddKeyWithMarker(hash[:], true)
		}
		err = tx.Commit()
		require.NoError(t, err)

		reHash := rebuildFlatDBTrieHash(t, rl, db)
		if len(modifiedKeys) == 0 {
			require.Equal(t, initialHash, reHash)
		}
		naiveHash := naiveTrieHashFromDB(t, db)
		require.Equal(t, reHash, naiveHash)
	})
}
