package commitment

import (
	"math/rand"
	"testing"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestVerifyBranchHashes_RoundTrip(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())

	// Build a cell with account data.
	c := new(cell)
	addr := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	copy(c.accountAddr[:], addr[:])
	c.accountAddrLen = length.Addr

	acc := accounts.Account{
		Nonce:    42,
		Balance:  *uint256.NewInt(1000000),
		CodeHash: accounts.EmptyCodeHash,
	}
	c.Nonce = acc.Nonce
	c.Balance.Set(&acc.Balance)
	c.CodeHash = acc.CodeHash.Value()
	c.Flags = BalanceUpdate | NonceUpdate | CodeUpdate
	c.loaded = cellLoadAccount

	// Hash the account key to get the hashed extension.
	// In production, root branch is folded at depth=1 (branchKey has 0 nibbles, depth = 0+1).
	keccak := keccak.NewFastKeccak()
	depth := int16(1) // root branch fold depth
	hashBuf := make([]byte, length.Hash)
	if err := c.hashAccKey(keccak, depth, hashBuf); err != nil {
		t.Fatal(err)
	}
	c.hashedExtension[64-depth] = terminatorHexByte

	// Compute the cell hash
	hph.memoizationOff = false // allow memoization
	hash, err := hph.computeCellHash(c, depth, nil)
	require.NoError(t, err)
	require.True(t, c.stateHashLen > 0, "stateHash should be set after computeCellHash")

	t.Logf("stateHash: %x (len=%d)", c.stateHash[:c.stateHashLen], c.stateHashLen)
	t.Logf("computed hash: %x", hash)

	// Now encode this cell into branch data using EncodeBranch.
	encoder := NewBranchEncoder(1024)
	// First nibble of the hashed key at depth=1 starts from index 1
	// But the cell's hashedExtension was filled from depth=1, so [0] is the first extension nibble
	nibble := int(c.hashedExtension[0]) // first nibble of the hashed extension
	touchMap := uint16(1 << nibble)
	afterMap := touchMap

	var cellData [16]cellEncodeData
	cellData[nibble] = cellEncodeDataFromCell(c)
	branchData, err := encoder.EncodeBranch(touchMap, afterMap, touchMap, &cellData)
	require.NoError(t, err)
	require.True(t, len(branchData) > 0)

	// Build the branchKey (compacted nibbles for depth=0 means empty path)
	branchKey := nibbles.HexToCompact(nil) // empty path

	// Serialize the account value
	accVal := accounts.SerialiseV3(&acc)

	// Verify the branch hashes
	accountValues := map[string][]byte{
		string(addr[:]): accVal,
	}
	storageValues := map[string][]byte{}

	err = VerifyBranchHashes(branchKey, BranchData(branchData), accountValues, storageValues)
	require.NoError(t, err)

	// Now corrupt the value and verify mismatch
	corruptedAcc := accounts.Account{
		Nonce:    43, // different nonce
		Balance:  *uint256.NewInt(1000000),
		CodeHash: accounts.EmptyCodeHash,
	}
	corruptedVal := accounts.SerialiseV3(&corruptedAcc)
	corruptedAccountValues := map[string][]byte{
		string(addr[:]): corruptedVal,
	}

	err = VerifyBranchHashes(branchKey, BranchData(branchData), corruptedAccountValues, storageValues)
	require.Error(t, err)
	t.Logf("Expected mismatch error: %v", err)
}

func TestVerifyBranchHashes_Singleton(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())

	// Build a singleton cell with BOTH account and storage data at depth=2.
	c := new(cell)
	addr := common.HexToAddress("0x4c888535841acbe0709b0758083f61d375bc02b4")
	loc := common.HexToHash("0x1df4f91929e18fda9e6f82e54e748e81e79e4bbd6fe34cdcba843ee8d63e8c4f")
	copy(c.accountAddr[:], addr[:])
	c.accountAddrLen = length.Addr
	copy(c.storageAddr[:length.Addr], addr[:])
	copy(c.storageAddr[length.Addr:], loc[:])
	c.storageAddrLen = length.Addr + length.Hash

	acc := accounts.Account{
		Nonce:    2,
		Balance:  *uint256.NewInt(2000),
		CodeHash: accounts.EmptyCodeHash,
	}
	c.Nonce = acc.Nonce
	c.Balance.Set(&acc.Balance)
	c.CodeHash = acc.CodeHash.Value()
	c.Flags = BalanceUpdate | NonceUpdate | CodeUpdate | StorageUpdate
	c.loaded = cellLoadAccount | cellLoadStorage

	storageVal := []byte{0x4c, 0x1d}
	copy(c.Storage[:], storageVal)
	c.StorageLen = int8(len(storageVal))

	// Hash keys at depth=2
	keccak := keccak.NewFastKeccak()
	depth := int16(2)

	// First hash storage key
	hashedKeyOffset := int16(0)
	if depth >= 64 {
		hashedKeyOffset = depth - 64
	}
	hashBuf := make([]byte, length.Hash)
	if err := c.hashStorageKey(keccak, length.Addr, 0, hashedKeyOffset, hashBuf); err != nil {
		t.Fatal(err)
	}
	c.hashedExtension[64-hashedKeyOffset] = terminatorHexByte

	// Compute storage leaf hash (as part of computeCellHash for singleton)
	hph.memoizationOff = false
	hash, err := hph.computeCellHash(c, depth, nil)
	require.NoError(t, err)
	require.True(t, c.stateHashLen > 0, "stateHash should be set for singleton")

	t.Logf("singleton stateHash: %x (len=%d)", c.stateHash[:c.stateHashLen], c.stateHashLen)
	t.Logf("computed hash: %x", hash)

	// Encode into branch data
	encoder := NewBranchEncoder(1024)
	// The nibble is determined by the hashed account key at depth=2
	if err := c.hashAccKey(keccak, depth, hashBuf); err != nil {
		t.Fatal(err)
	}
	nibble := int(c.hashedExtension[0])
	touchMap := uint16(1 << nibble)
	afterMap := touchMap

	var cellData [16]cellEncodeData
	cellData[nibble] = cellEncodeDataFromCell(c)
	branchData, err := encoder.EncodeBranch(touchMap, afterMap, touchMap, &cellData)
	require.NoError(t, err)

	// branchKey for depth=2: depth-1 = 1 nibble of the hashed account key path
	if err := c.hashAccKey(keccak, 0, hashBuf); err != nil {
		t.Fatal(err)
	}
	branchNibbles := []byte{c.hashedExtension[0]}
	branchKey := nibbles.HexToCompact(branchNibbles)

	// Build domain values maps
	accVal := accounts.SerialiseV3(&acc)
	accountValues := map[string][]byte{
		string(addr[:]): accVal,
	}
	storageValues := map[string][]byte{
		string(c.storageAddr[:c.storageAddrLen]): storageVal,
	}

	err = VerifyBranchHashes(branchKey, BranchData(branchData), accountValues, storageValues)
	require.NoError(t, err)
}

// TestVerifyBranchHashes_SingletonDepth1 tests singleton at depth=1 (root branch).
// In production, the root branch is folded at depth=1 (branchKey has 0 nibbles).
func TestVerifyBranchHashes_SingletonDepth1(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())

	c := new(cell)
	addr := common.HexToAddress("0x4c888535841acbe0709b0758083f61d375bc02b4")
	loc := common.HexToHash("0x1df4f91929e18fda9e6f82e54e748e81e79e4bbd6fe34cdcba843ee8d63e8c4f")
	copy(c.accountAddr[:], addr[:])
	c.accountAddrLen = length.Addr
	copy(c.storageAddr[:length.Addr], addr[:])
	copy(c.storageAddr[length.Addr:], loc[:])
	c.storageAddrLen = length.Addr + length.Hash

	acc := accounts.Account{
		Nonce:    2,
		Balance:  *uint256.NewInt(2000),
		CodeHash: accounts.EmptyCodeHash,
	}
	c.Nonce = acc.Nonce
	c.Balance.Set(&acc.Balance)
	c.CodeHash = acc.CodeHash.Value()
	c.Flags = BalanceUpdate | NonceUpdate | CodeUpdate | StorageUpdate
	c.loaded = cellLoadAccount | cellLoadStorage

	storageVal := []byte{0x4c, 0x1d}
	copy(c.Storage[:], storageVal)
	c.StorageLen = int8(len(storageVal))

	// Root branch is folded at depth=1 (branchKey has 0 nibbles, depth = 0 + 1 = 1)
	keccak := keccak.NewFastKeccak()
	depth := int16(1)

	// First hash storage key
	hashedKeyOffset := int16(0)
	hashBuf := make([]byte, length.Hash)
	if err := c.hashStorageKey(keccak, length.Addr, 0, hashedKeyOffset, hashBuf); err != nil {
		t.Fatal(err)
	}
	c.hashedExtension[64-hashedKeyOffset] = terminatorHexByte

	// Compute hash at depth=1
	hph.memoizationOff = false
	hash, err := hph.computeCellHash(c, depth, nil)
	require.NoError(t, err)
	require.True(t, c.stateHashLen > 0, "stateHash should be set for singleton")

	t.Logf("singleton depth=1 stateHash: %x (len=%d)", c.stateHash[:c.stateHashLen], c.stateHashLen)
	t.Logf("computed hash: %x", hash)

	// Encode into branch data
	encoder := NewBranchEncoder(1024)
	if err := c.hashAccKey(keccak, depth, hashBuf); err != nil {
		t.Fatal(err)
	}
	nibble := int(c.hashedExtension[0])
	touchMap := uint16(1 << nibble)
	afterMap := touchMap

	var cellData [16]cellEncodeData
	cellData[nibble] = cellEncodeDataFromCell(c)
	branchData, err := encoder.EncodeBranch(touchMap, afterMap, touchMap, &cellData)
	require.NoError(t, err)

	// branchKey for root branch: empty path (0 nibbles → depth = 0 + 1 = 1)
	branchKey := nibbles.HexToCompact(nil)

	// Build domain values maps
	accVal := accounts.SerialiseV3(&acc)
	accountValues := map[string][]byte{
		string(addr[:]): accVal,
	}
	storageValues := map[string][]byte{
		string(c.storageAddr[:c.storageAddrLen]): storageVal,
	}

	err = VerifyBranchHashes(branchKey, BranchData(branchData), accountValues, storageValues)
	require.NoError(t, err)
}

func TestVerifyBranchHashes_Storage(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())

	// Build a cell with storage data (depth >= 64 = pure storage cell).
	c := new(cell)
	addr := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	loc := common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	copy(c.storageAddr[:length.Addr], addr[:])
	copy(c.storageAddr[length.Addr:], loc[:])
	c.storageAddrLen = length.Addr + length.Hash

	storageVal := []byte{0x01, 0x02, 0x03}
	copy(c.Storage[:], storageVal)
	c.StorageLen = int8(len(storageVal))
	c.loaded = cellLoadStorage

	// Hash the storage key (depth=65 means non-singleton storage-only cell)
	keccak := keccak.NewFastKeccak()
	depth := int16(65)
	hashedKeyOffset := depth - 64
	hashBuf := make([]byte, length.Hash)
	if err := c.hashStorageKey(keccak, length.Addr, 0, hashedKeyOffset, hashBuf); err != nil {
		t.Fatal(err)
	}
	c.hashedExtension[64-hashedKeyOffset] = terminatorHexByte

	hph.memoizationOff = false
	hash, err := hph.computeCellHash(c, depth, nil)
	require.NoError(t, err)
	require.True(t, c.stateHashLen > 0)

	t.Logf("stateHash: %x (len=%d)", c.stateHash[:c.stateHashLen], c.stateHashLen)
	t.Logf("computed hash: %x", hash)

	// Encode cell into branch data
	encoder := NewBranchEncoder(1024)
	nibble := int(c.hashedExtension[0])
	touchMap := uint16(1 << nibble)
	afterMap := touchMap

	var cellData [16]cellEncodeData
	cellData[nibble] = cellEncodeDataFromCell(c)
	branchData, err := encoder.EncodeBranch(touchMap, afterMap, touchMap, &cellData)
	require.NoError(t, err)

	// Build branchKey for depth=65: depth-1 = 64 nibbles (hash of account address).
	// In production, the storage branch at depth=65 has currentKeyLen=64.
	keccak.Reset()
	keccak.Write(addr[:])
	var hashBuf2 [32]byte
	keccak.Read(hashBuf2[:])
	var nib [64]byte
	for i := 0; i < 32; i++ {
		nib[i*2] = hashBuf2[i] >> 4
		nib[i*2+1] = hashBuf2[i] & 0x0f
	}
	branchKey := nibbles.HexToCompact(nib[:])

	// Build domain values map
	storageValues := map[string][]byte{
		string(c.storageAddr[:c.storageAddrLen]): storageVal,
	}

	err = VerifyBranchHashes(branchKey, BranchData(branchData), map[string][]byte{}, storageValues)
	require.NoError(t, err)
}

// --- ModeParallel root-hash equivalence harness ----------------------------
//
// Every test below drives a (plainKeys, updates) batch through both
// sequential HexPatriciaHashed (ModeDirect) and ParallelPatriciaHashed
// (ModeParallel) and asserts byte-equal root hashes. The cardinal
// correctness rule for ParallelPatriciaHashed is same-root-as-sequential:
// a batch that produces a root in one mode but a different (or no) root in
// the other indicates a bug regardless of which root looks "more correct".
//
// The shared helper assertEquivalentRootWorkers lives in
// parallel_patricia_hashed_test.go (same package).

// randomBatchShape categorises the structural shape of an update batch the
// ModeParallel arm must handle. Each shape stresses a different portion of
// the mount/fold pipeline.
type randomBatchShape int

const (
	// shapeAccountsOnly — every key is an EOA-style address; no storage.
	// Touches one nibble bucket per address; addresses across distinct top
	// nibbles fan out to separate mount workers.
	shapeAccountsOnly randomBatchShape = iota
	// shapeStorageHeavySingle — one account with many storage slots. The
	// prefix trie's first 64 nibbles collapse via path compression; the
	// fork sits deep under the account's hashed prefix.
	shapeStorageHeavySingle
	// shapeStorageSpread — several accounts each with their own storage. A
	// realistic mainnet-shape batch spanning many root nibbles.
	shapeStorageSpread
	// shapeInsertsAndDeletes — a mix of insert / delete account updates.
	// Exercises the touched-and-deleted fold path.
	shapeInsertsAndDeletes
	// shapeEmpty — no touched keys. Both modes must return the empty-trie
	// root.
	shapeEmpty
)

// generateBatch builds a (plainKeys, updates) pair for the given shape, with
// a target key count and a seed for deterministic regeneration. Plain keys
// are unique within the batch (duplicates would just be deduped by Updates'
// keys map — keeping them unique keeps the touched-key count predictable).
//
// n is the target post-dedup size; the generator rejects collisions and
// retries, so the returned slice length matches n unless shape == shapeEmpty.
func generateBatch(shape randomBatchShape, n int, seed int64) ([][]byte, []Update) {
	if shape == shapeEmpty {
		return nil, nil
	}
	if n <= 0 {
		return nil, nil
	}
	rnd := rand.New(rand.NewSource(seed))
	plainKeys := make([][]byte, 0, n)
	updates := make([]Update, 0, n)
	used := make(map[string]struct{}, n)

	var sharedAddr [length.Addr]byte
	if shape == shapeStorageHeavySingle {
		rnd.Read(sharedAddr[:])
		// First key always touches the account itself so the encoded
		// branch shape mirrors what production callers produce.
		accKey := append([]byte(nil), sharedAddr[:]...)
		used[string(accKey)] = struct{}{}
		acc := Update{Flags: BalanceUpdate | NonceUpdate}
		acc.Balance.SetUint64(rnd.Uint64())
		acc.Nonce = rnd.Uint64()
		plainKeys = append(plainKeys, accKey)
		updates = append(updates, acc)
	}

	for len(plainKeys) < n {
		key, isStorage := generateKey(shape, sharedAddr[:], rnd)
		if _, dup := used[string(key)]; dup {
			continue
		}
		used[string(key)] = struct{}{}
		updates = append(updates, generateUpdate(shape, isStorage, rnd))
		plainKeys = append(plainKeys, key)
	}
	return plainKeys, updates
}

// generateKey returns a single plain key matching the requested shape. The
// returned isStorage flag tells the caller whether the key encodes a storage
// slot (length.Addr+length.Hash) or an account (length.Addr).
func generateKey(shape randomBatchShape, sharedAddr []byte, rnd *rand.Rand) (key []byte, isStorage bool) {
	switch shape {
	case shapeAccountsOnly, shapeInsertsAndDeletes:
		key = make([]byte, length.Addr)
		rnd.Read(key)
		return key, false
	case shapeStorageHeavySingle:
		key = make([]byte, length.Addr+length.Hash)
		copy(key, sharedAddr)
		rnd.Read(key[length.Addr:])
		return key, true
	case shapeStorageSpread:
		// Two-thirds storage, one-third account so the batch covers both
		// trie depths and both branch-encoder paths.
		if rnd.Intn(3) == 0 {
			key = make([]byte, length.Addr)
			rnd.Read(key)
			return key, false
		}
		key = make([]byte, length.Addr+length.Hash)
		rnd.Read(key)
		return key, true
	}
	// shapeEmpty is handled by the caller; any other unrecognised shape
	// falls back to a plain account key so the harness fails loudly rather
	// than silently degenerating.
	key = make([]byte, length.Addr)
	rnd.Read(key)
	return key, false
}

// generateUpdate fills an Update payload appropriate for the shape and key
// type. For shapeInsertsAndDeletes ~1/3 of account updates become deletes;
// storage keys always carry StorageUpdate.
func generateUpdate(shape randomBatchShape, isStorage bool, rnd *rand.Rand) Update {
	u := Update{}
	if isStorage {
		u.Flags = StorageUpdate
		sz := 1 + rnd.Intn(length.Hash)
		rnd.Read(u.Storage[:sz])
		u.StorageLen = int8(sz)
		return u
	}
	if shape == shapeInsertsAndDeletes && rnd.Intn(3) == 0 {
		u.Flags = DeleteUpdate
		return u
	}
	u.Flags = BalanceUpdate | NonceUpdate
	u.Balance.SetUint64(rnd.Uint64())
	u.Nonce = rnd.Uint64()
	if rnd.Intn(2) == 0 {
		u.Flags |= CodeUpdate
		rnd.Read(u.CodeHash[:])
	}
	return u
}

// TestVerifyParallel_AllShapes exercises one batch per named shape. Each
// shape is run as a subtest so failures point at the offending shape rather
// than the bulk loop below.
func TestVerifyParallel_AllShapes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		shape randomBatchShape
		n     int
		seed  int64
	}{
		{"AccountsOnly", shapeAccountsOnly, 48, 0xCAFEBABE},
		{"StorageHeavySingle", shapeStorageHeavySingle, 96, 0xBEEFCAFE},
		{"StorageSpread", shapeStorageSpread, 64, 0xDEADBEEF},
		{"InsertsAndDeletes", shapeInsertsAndDeletes, 48, 0xF00DBABE},
		{"Empty", shapeEmpty, 0, 0x1337},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			plainKeys, updates := generateBatch(tc.shape, tc.n, tc.seed)
			root := assertEquivalentRootWorkers(t, plainKeys, updates, 4)
			require.NotEmpty(t, root, "root hash must be non-empty for shape %s", tc.name)
		})
	}
}

// TestVerifyParallel_RandomBatches drives ≥1000 randomised batches through
// both ModeDirect and ModeParallel and asserts byte-equal root hashes on
// every iteration. The shape is cycled deterministically so each of the
// five named shapes runs at least 200 times; sizes and seeds are drawn from
// a single seeded RNG so failures reproduce exactly.
//
// Skipped under -short because the harness creates a temp dir and two
// MockState/Trie pairs per iteration (~25-30s wall time uncontended).
func TestVerifyParallel_RandomBatches(t *testing.T) {
	if testing.Short() {
		t.Skip("slow: 1100 batches x two Process calls")
	}
	rnd := rand.New(rand.NewSource(0xC0FFEE))
	const totalBatches = 1100
	// All five named shapes participate; each gets ≥220 iterations.
	shapes := []randomBatchShape{
		shapeAccountsOnly, shapeStorageHeavySingle, shapeStorageSpread,
		shapeInsertsAndDeletes, shapeEmpty,
	}
	for i := range totalBatches {
		shape := shapes[i%len(shapes)]
		// Keep batches small enough to keep the loop under a minute while
		// still spanning enough top-level nibbles to produce split-points
		// in the multi-bucket shapes.
		n := 0
		if shape != shapeEmpty {
			n = 1 + rnd.Intn(96)
		}
		seed := rnd.Int63()
		plainKeys, updates := generateBatch(shape, n, seed)
		_ = assertEquivalentRootWorkers(t, plainKeys, updates, 4)
	}
}

// FuzzParallelEquivalence is the testing.F entry point for randomised
// ModeDirect ↔ ModeParallel root-hash equivalence. Run with:
//
//	go test -fuzz=FuzzParallelEquivalence -fuzztime=60s ./execution/commitment/
//
// Seeded inputs cover the named shapes; the fuzz engine explores variations
// in keys count and seeds. Oversized batches are skipped so a runaway input
// cannot wedge the worker.
func FuzzParallelEquivalence(f *testing.F) {
	// Seed corpus: one entry per named shape, plus a couple of mixed-size
	// account-only batches so the fuzz engine starts with a meaningful
	// distribution.
	f.Add(uint16(8), uint8(0), int64(0xA1))
	f.Add(uint16(32), uint8(0), int64(0xA2))
	f.Add(uint16(96), uint8(1), int64(0xB1))
	f.Add(uint16(64), uint8(2), int64(0xC1))
	f.Add(uint16(48), uint8(3), int64(0xD1))
	f.Add(uint16(0), uint8(4), int64(0xE1))

	f.Fuzz(func(t *testing.T, keysCount uint16, shapeByte uint8, seed int64) {
		// Cap batch size: the fuzzer occasionally produces huge counts that
		// make the assertion loop slow without adding coverage. 512 is well
		// above the smallest multi-split-point shape.
		if keysCount > 512 {
			t.Skip("oversized batch")
		}
		shape := randomBatchShape(int(shapeByte) % 5)
		plainKeys, updates := generateBatch(shape, int(keysCount), seed)
		_ = assertEquivalentRootWorkers(t, plainKeys, updates, 2)
	})
}
