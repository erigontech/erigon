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

// newAccountCell returns a cell loaded with the given account data at addr.
func newAccountCell(addr common.Address, acc *accounts.Account) *cell {
	c := new(cell)
	copy(c.accountAddr[:], addr[:])
	c.accountAddrLen = length.Addr
	c.Nonce = acc.Nonce
	c.Balance.Set(&acc.Balance)
	c.CodeHash = acc.CodeHash.Value()
	c.Flags = BalanceUpdate | NonceUpdate | CodeUpdate
	c.loaded = cellLoadAccount
	return c
}

// addStorageToCell loads the given storage slot and value into the cell, making it a
// singleton account+storage cell when account data is already present.
func addStorageToCell(c *cell, addr common.Address, loc common.Hash, val []byte) {
	copy(c.storageAddr[:length.Addr], addr[:])
	copy(c.storageAddr[length.Addr:], loc[:])
	c.storageAddrLen = length.Addr + length.Hash
	copy(c.Storage[:], val)
	c.StorageLen = int8(len(val))
	c.Flags |= StorageUpdate
	c.loaded |= cellLoadStorage
}

// encodeCellToBranch computes the cell hash at the given depth (memoizing its stateHash) and
// encodes the cell as a single-child branch at the given nibble.
func encodeCellToBranch(t *testing.T, hph *HexPatriciaHashed, c *cell, depth int16, nibble int) BranchData {
	t.Helper()

	_, err := hph.computeCellHash(c, depth, nil)
	require.NoError(t, err)
	require.True(t, c.stateHashLen > 0, "stateHash should be set after computeCellHash")

	touchMap := uint16(1 << nibble)
	var cellData [16]cellEncodeData
	cellData[nibble] = cellEncodeDataFromCell(c)
	branchData, err := NewBranchEncoder(1024).EncodeBranch(touchMap, touchMap, touchMap, &cellData)
	require.NoError(t, err)
	require.NotEmpty(t, branchData)
	return BranchData(branchData)
}

func TestVerifyBranchHashes_RoundTrip(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())

	addr := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	acc := accounts.Account{
		Nonce:    42,
		Balance:  *uint256.NewInt(1000000),
		CodeHash: accounts.EmptyCodeHash,
	}
	c := newAccountCell(addr, &acc)

	// Hash the account key to get the hashed extension.
	// In production, root branch is folded at depth=1 (branchKey has 0 nibbles, depth = 0+1).
	keccak := keccak.NewFastKeccak()
	depth := int16(1)
	hashBuf := make([]byte, length.Hash)
	require.NoError(t, c.hashAccKey(keccak, depth, hashBuf))
	c.hashedExtension[64-depth] = terminatorHexByte

	branchData := encodeCellToBranch(t, hph, c, depth, int(c.hashedExtension[0]))

	// Root branch: empty compacted path
	branchKey := nibbles.HexToCompact(nil)

	accountValues := map[string][]byte{
		string(addr[:]): accounts.SerialiseV3(&acc),
	}
	storageValues := map[string][]byte{}

	err := VerifyBranchHashes(branchKey, branchData, accountValues, storageValues)
	require.NoError(t, err)

	// Now corrupt the value and verify mismatch
	corruptedAcc := accounts.Account{
		Nonce:    43, // different nonce
		Balance:  *uint256.NewInt(1000000),
		CodeHash: accounts.EmptyCodeHash,
	}
	corruptedAccountValues := map[string][]byte{
		string(addr[:]): accounts.SerialiseV3(&corruptedAcc),
	}

	err = VerifyBranchHashes(branchKey, branchData, corruptedAccountValues, storageValues)
	require.Error(t, err)
	t.Logf("Expected mismatch error: %v", err)
}

func TestVerifyBranchHashes_Singleton(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())

	// Singleton cell with BOTH account and storage data at depth=2.
	addr := common.HexToAddress("0x4c888535841acbe0709b0758083f61d375bc02b4")
	loc := common.HexToHash("0x1df4f91929e18fda9e6f82e54e748e81e79e4bbd6fe34cdcba843ee8d63e8c4f")
	acc := accounts.Account{
		Nonce:    2,
		Balance:  *uint256.NewInt(2000),
		CodeHash: accounts.EmptyCodeHash,
	}
	storageVal := []byte{0x4c, 0x1d}
	c := newAccountCell(addr, &acc)
	addStorageToCell(c, addr, loc, storageVal)

	keccak := keccak.NewFastKeccak()
	depth := int16(2)

	// First hash storage key (offset 0 since depth < 64)
	hashBuf := make([]byte, length.Hash)
	require.NoError(t, c.hashStorageKey(keccak, length.Addr, 0, 0, hashBuf))
	c.hashedExtension[64] = terminatorHexByte

	// The nibble is determined by the hashed account key at depth=2
	require.NoError(t, c.hashAccKey(keccak, depth, hashBuf))
	nibble := int(c.hashedExtension[0])

	// hashAccKey overwrote the storage path in hashedExtension; restore it before hashing the cell
	require.NoError(t, c.hashStorageKey(keccak, length.Addr, 0, 0, hashBuf))
	c.hashedExtension[64] = terminatorHexByte

	branchData := encodeCellToBranch(t, hph, c, depth, nibble)

	// branchKey for depth=2: 1 nibble of the hashed account key path
	require.NoError(t, c.hashAccKey(keccak, 0, hashBuf))
	branchKey := nibbles.HexToCompact([]byte{c.hashedExtension[0]})

	accountValues := map[string][]byte{
		string(addr[:]): accounts.SerialiseV3(&acc),
	}
	storageValues := map[string][]byte{
		string(c.storageAddr[:c.storageAddrLen]): storageVal,
	}

	err := VerifyBranchHashes(branchKey, branchData, accountValues, storageValues)
	require.NoError(t, err)
}

// TestVerifyBranchHashes_SingletonDepth1 tests singleton at depth=1 (root branch).
// In production, the root branch is folded at depth=1 (branchKey has 0 nibbles).
func TestVerifyBranchHashes_SingletonDepth1(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())

	addr := common.HexToAddress("0x4c888535841acbe0709b0758083f61d375bc02b4")
	loc := common.HexToHash("0x1df4f91929e18fda9e6f82e54e748e81e79e4bbd6fe34cdcba843ee8d63e8c4f")
	acc := accounts.Account{
		Nonce:    2,
		Balance:  *uint256.NewInt(2000),
		CodeHash: accounts.EmptyCodeHash,
	}
	storageVal := []byte{0x4c, 0x1d}
	c := newAccountCell(addr, &acc)
	addStorageToCell(c, addr, loc, storageVal)

	keccak := keccak.NewFastKeccak()
	depth := int16(1)

	hashBuf := make([]byte, length.Hash)
	require.NoError(t, c.hashStorageKey(keccak, length.Addr, 0, 0, hashBuf))
	c.hashedExtension[64] = terminatorHexByte

	require.NoError(t, c.hashAccKey(keccak, depth, hashBuf))
	nibble := int(c.hashedExtension[0])

	require.NoError(t, c.hashStorageKey(keccak, length.Addr, 0, 0, hashBuf))
	c.hashedExtension[64] = terminatorHexByte

	branchData := encodeCellToBranch(t, hph, c, depth, nibble)

	// branchKey for root branch: empty path (0 nibbles → depth = 0 + 1 = 1)
	branchKey := nibbles.HexToCompact(nil)

	accountValues := map[string][]byte{
		string(addr[:]): accounts.SerialiseV3(&acc),
	}
	storageValues := map[string][]byte{
		string(c.storageAddr[:c.storageAddrLen]): storageVal,
	}

	err := VerifyBranchHashes(branchKey, branchData, accountValues, storageValues)
	require.NoError(t, err)
}

func TestVerifyBranchHashes_Storage(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())

	// Pure storage cell (depth >= 64).
	addr := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	loc := common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	storageVal := []byte{0x01, 0x02, 0x03}
	c := new(cell)
	addStorageToCell(c, addr, loc, storageVal)

	// Hash the storage key (depth=65 means non-singleton storage-only cell)
	keccak := keccak.NewFastKeccak()
	depth := int16(65)
	hashedKeyOffset := depth - 64
	hashBuf := make([]byte, length.Hash)
	require.NoError(t, c.hashStorageKey(keccak, length.Addr, 0, hashedKeyOffset, hashBuf))
	c.hashedExtension[64-hashedKeyOffset] = terminatorHexByte

	branchData := encodeCellToBranch(t, hph, c, depth, int(c.hashedExtension[0]))

	// branchKey for depth=65: 64 nibbles (hash of the account address), matching the
	// production storage branch at currentKeyLen=64.
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

	storageValues := map[string][]byte{
		string(c.storageAddr[:c.storageAddrLen]): storageVal,
	}

	err := VerifyBranchHashes(branchKey, branchData, map[string][]byte{}, storageValues)
	require.NoError(t, err)
}

type randomBatchShape int

const (
	shapeAccountsOnly randomBatchShape = iota
	shapeStorageHeavySingle
	shapeStorageSpread
	shapeInsertsAndDeletes
	shapeEmpty
)

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
		// Seed the account before its storage so the branch shape matches production.
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
		// Mix accounts and storage to exercise both trie depths.
		if rnd.Intn(3) == 0 {
			key = make([]byte, length.Addr)
			rnd.Read(key)
			return key, false
		}
		key = make([]byte, length.Addr+length.Hash)
		rnd.Read(key)
		return key, true
	}
	key = make([]byte, length.Addr)
	rnd.Read(key)
	return key, false
}

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

func TestVerifyParallel_RandomBatches(t *testing.T) {
	if testing.Short() {
		t.Skip("slow: 1100 batches x two Process calls")
	}
	rnd := rand.New(rand.NewSource(0xC0FFEE))
	const totalBatches = 1100
	shapes := []randomBatchShape{
		shapeAccountsOnly, shapeStorageHeavySingle, shapeStorageSpread,
		shapeInsertsAndDeletes, shapeEmpty,
	}
	for i := range totalBatches {
		shape := shapes[i%len(shapes)]
		n := 0
		if shape != shapeEmpty {
			n = 1 + rnd.Intn(96)
		}
		seed := rnd.Int63()
		plainKeys, updates := generateBatch(shape, n, seed)
		_ = assertEquivalentRootWorkers(t, plainKeys, updates, 4)
	}
}

// Run: go test -fuzz=FuzzParallelEquivalence -fuzztime=60s ./execution/commitment/
func FuzzParallelEquivalence(f *testing.F) {
	f.Add(uint16(8), uint8(0), int64(0xA1))
	f.Add(uint16(32), uint8(0), int64(0xA2))
	f.Add(uint16(96), uint8(1), int64(0xB1))
	f.Add(uint16(64), uint8(2), int64(0xC1))
	f.Add(uint16(48), uint8(3), int64(0xD1))
	f.Add(uint16(0), uint8(4), int64(0xE1))

	f.Fuzz(func(t *testing.T, keysCount uint16, shapeByte uint8, seed int64) {
		// Larger batches only slow the loop without adding coverage.
		if keysCount > 512 {
			t.Skip("oversized batch")
		}
		shape := randomBatchShape(int(shapeByte) % 5)
		plainKeys, updates := generateBatch(shape, int(keysCount), seed)
		_ = assertEquivalentRootWorkers(t, plainKeys, updates, 2)
	})
}
