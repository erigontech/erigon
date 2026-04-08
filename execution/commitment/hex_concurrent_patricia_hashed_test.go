package commitment

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
)

// maxAddrSearchIters bounds the brute-force address search helpers below so a
// broken search space (e.g. hash function change) produces a descriptive panic
// instead of an infinite hang. 1M iterations is well above the expected work:
// a single-nibble hit averages ~16 iters; a 16-bit shared-prefix hit averages
// ~65k, both comfortably under the cap.
const maxAddrSearchIters = 1 << 20

// nibbleSeedKey is the composite cache key for findAddressForNibble.
type nibbleSeedKey struct{ nibble, seed int }

// nibbleAddressCache caches brute-forced addresses keyed by (nibble, seed) to
// avoid repeated keccak work across tests and ensure each seed always returns
// the same deterministic address regardless of call order.
var (
	nibbleAddressCacheMu sync.Mutex
	nibbleAddressCache   = make(map[nibbleSeedKey][]byte)
)

// findAddressForNibble brute-force searches for a 20-byte address whose
// keccak256 first nibble (upper 4 bits of hash[0]) matches targetNibble.
// seed controls the starting point for the search; each unique seed produces
// a different address. Results are cached globally.
func findAddressForNibble(targetNibble int, seed int) []byte {
	key := nibbleSeedKey{targetNibble, seed}

	nibbleAddressCacheMu.Lock()
	if cached, ok := nibbleAddressCache[key]; ok {
		nibbleAddressCacheMu.Unlock()
		return cached
	}
	nibbleAddressCacheMu.Unlock()

	// Brute force: we encode a counter into the first 8 bytes of a 20-byte
	// address and increment until keccak(addr)[0] >> 4 == targetNibble.
	var addr [20]byte
	// Use seed * large prime to separate search spaces for different seeds.
	counter := uint64(seed) * 1_000_003
	for iter := 0; iter < maxAddrSearchIters; iter++ {
		binary.BigEndian.PutUint64(addr[:8], counter)
		h := crypto.Keccak256(addr[:])
		if int(h[0]>>4) == targetNibble {
			result := make([]byte, 20)
			copy(result, addr[:])

			nibbleAddressCacheMu.Lock()
			nibbleAddressCache[key] = result
			nibbleAddressCacheMu.Unlock()
			return result
		}
		counter++
	}
	panic(fmt.Sprintf("findAddressForNibble(nibble=%d, seed=%d): exceeded %d iterations", targetNibble, seed, maxAddrSearchIters))
}

// mockTrieCtxFactory returns a TrieContextFactory that always returns the
// given MockState and a no-op cleanup.
func mockTrieCtxFactory(ms *MockState) TrieContextFactory {
	return func() (PatriciaContext, func()) {
		return ms, func() {}
	}
}

// setupTriePair creates two independent MockState instances and matching tries:
//   - seqTrie: a sequential HexPatriciaHashed
//   - parTrie: a ConcurrentPatriciaHashed wrapping its own HexPatriciaHashed
//
// Returns (seqMs, parMs, seqTrie, parTrie).
func setupTriePair(t *testing.T) (*MockState, *MockState, *HexPatriciaHashed, *ConcurrentPatriciaHashed) {
	t.Helper()

	seqMs := NewMockState(t)
	parMs := NewMockState(t)

	seqTrie := NewHexPatriciaHashed(length.Addr, seqMs)
	parMs.SetConcurrentCommitment(true)
	parTrieInner := NewHexPatriciaHashed(length.Addr, parMs)
	parTrie := NewConcurrentPatriciaHashed(parTrieInner, parMs)

	return seqMs, parMs, seqTrie, parTrie
}

// compareRoots applies the same plainKeys/updates to both sequential and
// concurrent tries and asserts that the resulting root hashes are identical.
// Returns the root hash for chaining in multi-batch tests.
func compareRoots(
	t *testing.T,
	seqMs, parMs *MockState,
	seqTrie *HexPatriciaHashed,
	parTrie *ConcurrentPatriciaHashed,
	plainKeys [][]byte,
	updates []Update,
) []byte {
	t.Helper()
	ctx := context.Background()

	// 1. Apply updates to both MockState instances
	err := seqMs.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)
	err = parMs.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	// 2. Wrap via sequential and concurrent paths
	seqUpds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer seqUpds.Close()

	parUpds := WrapKeyUpdatesParallel(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer parUpds.Close()

	// 3. Process through each trie
	seqRoot, err := seqTrie.Process(ctx, seqUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	parRoot, err := parTrie.Process(ctx, parUpds, "", nil, WarmupConfig{
		CtxFactory: mockTrieCtxFactory(parMs),
	})
	require.NoError(t, err)

	// 4. Assert root equality
	require.Equal(t, seqRoot, parRoot,
		"sequential and concurrent root hashes must match")

	return seqRoot
}

// multiBatchComparer tracks the concurrent/sequential mode between batches,
// respecting ConcurrentPatriciaHashed.CanDoConcurrentNext() — matching
// production behavior where the trie decides the next batch's mode.
type multiBatchComparer struct {
	t             *testing.T
	seqMs, parMs  *MockState
	seqTrie       *HexPatriciaHashed
	parTrie       *ConcurrentPatriciaHashed
	useConcurrent bool // whether parTrie should use concurrent mode for the next batch
}

func newMultiBatchComparer(t *testing.T) *multiBatchComparer {
	t.Helper()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)
	return &multiBatchComparer{
		t: t, seqMs: seqMs, parMs: parMs,
		seqTrie: seqTrie, parTrie: parTrie,
		useConcurrent: true,
	}
}

// compareBatch applies a batch of updates and asserts root hash equality.
// It uses concurrent or sequential mode for the parTrie based on the prior
// batch's CanDoConcurrentNext() result.
func (c *multiBatchComparer) compareBatch(plainKeys [][]byte, updates []Update) []byte {
	t := c.t
	t.Helper()
	ctx := context.Background()

	err := c.seqMs.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)
	err = c.parMs.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	seqUpds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer seqUpds.Close()

	var parUpds *Updates
	if c.useConcurrent {
		parUpds = WrapKeyUpdatesParallel(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	} else {
		// Sequential fallback — matches production behavior when
		// CanDoConcurrentNext() returned false after the prior batch.
		parUpds = WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	}
	defer parUpds.Close()

	seqRoot, err := c.seqTrie.Process(ctx, seqUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	parRoot, err := c.parTrie.Process(ctx, parUpds, "", nil, WarmupConfig{
		CtxFactory: mockTrieCtxFactory(c.parMs),
	})
	require.NoError(t, err)

	require.Equal(t, seqRoot, parRoot,
		"sequential and concurrent root hashes must match (concurrent mode: %v)", c.useConcurrent)

	// Update mode for the next batch based on the trie's recommendation
	nextConcurrent, err := c.parTrie.CanDoConcurrentNext()
	require.NoError(t, err)
	c.useConcurrent = nextConcurrent

	return seqRoot
}

func TestCompareRoots_Smoke(t *testing.T) {
	t.Parallel()

	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 42).
		Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root, "root hash should not be empty after an update")
	t.Logf("smoke test root: %x", root)
}

// addrHex returns the hex-encoded string of a 20-byte address (no 0x prefix),
// suitable for passing to UpdateBuilder methods.
func addrHex(addr []byte) string {
	return hex.EncodeToString(addr)
}

// --- Layer A: Key distribution pattern tests ---

func TestCompareRoots_AllKeysSingleNibble(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	const targetNibble = 0x0
	const numAddrs = 55

	ub := NewUpdateBuilder()
	for i := 0; i < numAddrs; i++ {
		addr := findAddressForNibble(targetNibble, i)
		ub.Balance(addrHex(addr), uint64(100+i))
	}
	plainKeys, updates := ub.Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)
	t.Logf("AllKeysSingleNibble root (%d keys in nibble %x): %x", numAddrs, targetNibble, root)
}

func TestCompareRoots_AllNibblesPopulated(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	ub := NewUpdateBuilder()
	total := 0
	for nibble := 0; nibble < 16; nibble++ {
		for seed := 0; seed < 3; seed++ {
			addr := findAddressForNibble(nibble, seed)
			ub.Balance(addrHex(addr), uint64(1000*nibble+seed))
			total++
		}
	}
	plainKeys, updates := ub.Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)
	t.Logf("AllNibblesPopulated root (%d keys across 16 nibbles): %x", total, root)
}

func TestCompareRoots_TwoNibblesOnly(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	ub := NewUpdateBuilder()
	for _, nibble := range []int{0x0, 0xF} {
		for seed := 0; seed < 10; seed++ {
			addr := findAddressForNibble(nibble, seed)
			ub.Balance(addrHex(addr), uint64(500+seed))
		}
	}
	plainKeys, updates := ub.Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)
	t.Logf("TwoNibblesOnly root (nibbles 0x0 and 0xF): %x", root)
}

func TestCompareRoots_FifteenNibbles(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	const skippedNibble = 0x7
	ub := NewUpdateBuilder()
	total := 0
	for nibble := 0; nibble < 16; nibble++ {
		if nibble == skippedNibble {
			continue
		}
		for seed := 0; seed < 3; seed++ {
			addr := findAddressForNibble(nibble, seed)
			ub.Balance(addrHex(addr), uint64(200*nibble+seed+1))
			total++
		}
	}
	plainKeys, updates := ub.Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)
	t.Logf("FifteenNibbles root (%d keys, skipped nibble %x): %x", total, skippedNibble, root)
}

func TestCompareRoots_SingleKey(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	addr := findAddressForNibble(0xA, 0)
	plainKeys, updates := NewUpdateBuilder().
		Balance(addrHex(addr), 999).
		Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)
	t.Logf("SingleKey root: %x", root)
}

func TestCompareRoots_HeavySkew(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	const heavyNibble = 0x5
	const heavyCount = 45 // ~90%
	const spreadCount = 5 // ~10% spread across other nibbles

	ub := NewUpdateBuilder()
	// Heavy nibble: 45 addresses
	for i := 0; i < heavyCount; i++ {
		addr := findAddressForNibble(heavyNibble, i)
		ub.Balance(addrHex(addr), uint64(i+1))
	}
	// Spread: 5 addresses in different nibbles (skip heavyNibble)
	spreadNibbles := []int{0x0, 0x3, 0x8, 0xC, 0xF}
	for i, nibble := range spreadNibbles {
		if i >= spreadCount {
			break
		}
		addr := findAddressForNibble(nibble, 0)
		ub.Balance(addrHex(addr), uint64(1000+i))
	}
	plainKeys, updates := ub.Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)
	t.Logf("HeavySkew root (%d in nibble %x, %d spread): %x",
		heavyCount, heavyNibble, spreadCount, root)
}

// --- Layer B: Update type combination tests ---

// makeStorageLoc returns a deterministic 32-byte storage location as a hex
// string, derived from the given index.
func makeStorageLoc(index int) string {
	var loc [32]byte
	binary.BigEndian.PutUint64(loc[24:], uint64(index))
	return hex.EncodeToString(loc[:])
}

// makeCodeHash returns a deterministic 32-byte code hash as a hex string,
// derived from the given index (non-zero to avoid empty code hash).
func makeCodeHash(index int) string {
	var h [32]byte
	binary.BigEndian.PutUint64(h[:8], uint64(index+1))
	h[31] = 0xcc // marker byte
	return hex.EncodeToString(h[:])
}

func TestCompareRoots_AccountsOnly(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	ub := NewUpdateBuilder()
	// Spread across all 16 nibbles with balance + nonce + codeHash updates
	for nibble := 0; nibble < 16; nibble++ {
		for seed := 0; seed < 2; seed++ {
			addr := findAddressForNibble(nibble, seed)
			ah := addrHex(addr)
			ub.Balance(ah, uint64(1000*nibble+seed+1))
			ub.Nonce(ah, uint64(nibble*10+seed+1))
			ub.CodeHash(ah, makeCodeHash(nibble*2+seed))
		}
	}
	plainKeys, updates := ub.Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)
	t.Logf("AccountsOnly root (32 accounts with balance+nonce+codeHash): %x", root)
}

func TestCompareRoots_AccountsWithStorage(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	ub := NewUpdateBuilder()
	// First create accounts with balances (storage needs an account to exist)
	for nibble := 0; nibble < 16; nibble++ {
		addr := findAddressForNibble(nibble, 0)
		ah := addrHex(addr)
		ub.Balance(ah, uint64(nibble+1))
		// Add 3 storage slots per account
		for slot := 0; slot < 3; slot++ {
			loc := makeStorageLoc(nibble*100 + slot)
			val := makeStorageLoc(nibble*100 + slot + 1) // non-zero value
			ub.Storage(ah, loc, val)
		}
	}
	plainKeys, updates := ub.Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)
	t.Logf("StorageOnly root (16 accounts x 3 storage slots): %x", root)
}

func TestCompareRoots_MixedAccountStorage(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	ub := NewUpdateBuilder()
	// Each address gets both account updates AND storage slots
	for nibble := 0; nibble < 16; nibble++ {
		addr := findAddressForNibble(nibble, 0)
		ah := addrHex(addr)
		// Account update: balance + nonce
		ub.Balance(ah, uint64(5000+nibble))
		ub.Nonce(ah, uint64(nibble+1))
		// Storage slots on the same address
		for slot := 0; slot < 2; slot++ {
			loc := makeStorageLoc(nibble*10 + slot)
			val := makeStorageLoc(nibble*10 + slot + 500)
			ub.Storage(ah, loc, val)
		}
	}
	plainKeys, updates := ub.Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)
	t.Logf("MixedAccountStorage root (16 accounts with balance+nonce+storage): %x", root)
}

func TestCompareRoots_Deletes(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	// Step 1: Create 32 accounts across nibbles
	ub1 := NewUpdateBuilder()
	addrs := make([][]byte, 0, 32)
	for nibble := 0; nibble < 16; nibble++ {
		for seed := 0; seed < 2; seed++ {
			addr := findAddressForNibble(nibble, seed)
			addrs = append(addrs, addr)
			ub1.Balance(addrHex(addr), uint64(1000+nibble*2+seed))
		}
	}
	plainKeys1, updates1 := ub1.Build()
	root1 := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys1, updates1)
	require.NotEmpty(t, root1)
	t.Logf("Deletes step 1 root (create 32): %x", root1)

	// Step 2: Delete half the accounts (every other one)
	ub2 := NewUpdateBuilder()
	deleted := 0
	for i := 0; i < len(addrs); i += 2 {
		ub2.Delete(addrHex(addrs[i]))
		deleted++
	}
	plainKeys2, updates2 := ub2.Build()
	root2 := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys2, updates2)
	require.NotEmpty(t, root2)
	require.NotEqual(t, root1, root2, "root should change after deletes")
	t.Logf("Deletes step 2 root (deleted %d): %x", deleted, root2)
}

func TestCompareRoots_FullAccountUpdate(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	ub := NewUpdateBuilder()
	// Every account gets balance + nonce + codeHash + storage — the full set
	for nibble := 0; nibble < 16; nibble++ {
		for seed := 0; seed < 2; seed++ {
			addr := findAddressForNibble(nibble, seed)
			ah := addrHex(addr)
			ub.Balance(ah, uint64(9999+nibble*2+seed))
			ub.Nonce(ah, uint64(100+nibble*2+seed))
			ub.CodeHash(ah, makeCodeHash(nibble*2+seed+100))
			// 2 storage slots per account
			for slot := 0; slot < 2; slot++ {
				loc := makeStorageLoc(nibble*1000 + seed*100 + slot)
				val := makeStorageLoc(nibble*1000 + seed*100 + slot + 1)
				ub.Storage(ah, loc, val)
			}
		}
	}
	plainKeys, updates := ub.Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)
	t.Logf("FullAccountUpdate root (32 accounts with balance+nonce+codeHash+storage): %x", root)
}

// --- Layer C: Multi-batch sequencing tests ---
// These tests DO NOT reset tries between batches — state carries forward.
// This targets the exact regression scenario where foldNibble extension
// trimming corrupts carried-forward state across batches.

func TestCompareRoots_MultiBatch_SpreadThenConcentrate(t *testing.T) {
	t.Parallel()
	c := newMultiBatchComparer(t)

	// Batch 1: accounts spread across all 16 nibbles
	ub1 := NewUpdateBuilder()
	for nibble := 0; nibble < 16; nibble++ {
		for seed := 0; seed < 2; seed++ {
			addr := findAddressForNibble(nibble, seed)
			ub1.Balance(addrHex(addr), uint64(1000+nibble*2+seed))
		}
	}
	plainKeys1, updates1 := ub1.Build()
	root1 := c.compareBatch(plainKeys1, updates1)
	require.NotEmpty(t, root1)
	t.Logf("SpreadThenConcentrate batch 1 root (32 spread): %x", root1)

	// Batch 2: all new accounts in a single nibble
	const concentrateNibble = 0x3
	ub2 := NewUpdateBuilder()
	for i := 0; i < 20; i++ {
		addr := findAddressForNibble(concentrateNibble, 10+i)
		ub2.Balance(addrHex(addr), uint64(5000+i))
	}
	plainKeys2, updates2 := ub2.Build()
	root2 := c.compareBatch(plainKeys2, updates2)
	require.NotEmpty(t, root2)
	require.NotEqual(t, root1, root2, "root should change after batch 2")
	t.Logf("SpreadThenConcentrate batch 2 root (20 in nibble %x): %x", concentrateNibble, root2)
}

func TestCompareRoots_MultiBatch_ThreePhases(t *testing.T) {
	t.Parallel()
	c := newMultiBatchComparer(t)

	// Batch 1: create 32 accounts across nibbles
	ub1 := NewUpdateBuilder()
	addrs := make([][]byte, 0, 32)
	for nibble := 0; nibble < 16; nibble++ {
		for seed := 0; seed < 2; seed++ {
			addr := findAddressForNibble(nibble, seed)
			addrs = append(addrs, addr)
			ub1.Balance(addrHex(addr), uint64(1000+nibble*2+seed))
			ub1.Nonce(addrHex(addr), uint64(1))
		}
	}
	plainKeys1, updates1 := ub1.Build()
	root1 := c.compareBatch(plainKeys1, updates1)
	require.NotEmpty(t, root1)
	t.Logf("ThreePhases batch 1 root (create 32): %x", root1)

	// Batch 2: update balances of the first half (16 accounts)
	ub2 := NewUpdateBuilder()
	for i := 0; i < 16; i++ {
		ub2.Balance(addrHex(addrs[i]), uint64(9000+i))
	}
	plainKeys2, updates2 := ub2.Build()
	root2 := c.compareBatch(plainKeys2, updates2)
	require.NotEmpty(t, root2)
	require.NotEqual(t, root1, root2, "root should change after balance updates")
	t.Logf("ThreePhases batch 2 root (update 16 balances): %x", root2)

	// Batch 3: delete a quarter of accounts (8 accounts — every 4th)
	ub3 := NewUpdateBuilder()
	deleted := 0
	for i := 0; i < len(addrs); i += 4 {
		ub3.Delete(addrHex(addrs[i]))
		deleted++
	}
	plainKeys3, updates3 := ub3.Build()
	root3 := c.compareBatch(plainKeys3, updates3)
	require.NotEmpty(t, root3)
	require.NotEqual(t, root2, root3, "root should change after deletes")
	t.Logf("ThreePhases batch 3 root (deleted %d): %x", deleted, root3)
}

func TestCompareRoots_MultiBatch_CreateThenDeleteAll(t *testing.T) {
	t.Parallel()
	c := newMultiBatchComparer(t)

	// Batch 1: create accounts across nibbles
	ub1 := NewUpdateBuilder()
	addrs := make([][]byte, 0, 32)
	for nibble := 0; nibble < 16; nibble++ {
		for seed := 0; seed < 2; seed++ {
			addr := findAddressForNibble(nibble, seed)
			addrs = append(addrs, addr)
			ub1.Balance(addrHex(addr), uint64(500+nibble*2+seed))
		}
	}
	plainKeys1, updates1 := ub1.Build()
	root1 := c.compareBatch(plainKeys1, updates1)
	require.NotEmpty(t, root1)
	t.Logf("CreateThenDeleteAll batch 1 root (create 32): %x", root1)

	// Batch 2: delete every account
	ub2 := NewUpdateBuilder()
	for _, addr := range addrs {
		ub2.Delete(addrHex(addr))
	}
	plainKeys2, updates2 := ub2.Build()
	root2 := c.compareBatch(plainKeys2, updates2)
	require.NotEqual(t, root1, root2, "root should change after deleting all accounts")
	t.Logf("CreateThenDeleteAll batch 2 root (delete all): %x", root2)
}

func TestCompareRoots_MultiBatch_RepeatedSingleNibble(t *testing.T) {
	t.Parallel()
	c := newMultiBatchComparer(t)

	// Mimics the "27M keys in one nibble" regression with multi-batch state
	// carry-forward. Starts with a spread to establish concurrent mode, then
	// concentrates subsequent batches into a single nibble.
	const targetNibble = 0x0

	// Batch 1: establish the trie with at least one account per nibble
	// (required for concurrent multi-batch: CanDoConcurrentNext needs root.extLen==0)
	ub1 := NewUpdateBuilder()
	spreadAddrs := make([][]byte, 16)
	for nibble := 0; nibble < 16; nibble++ {
		addr := findAddressForNibble(nibble, 50) // high seed to avoid collisions
		spreadAddrs[nibble] = addr
		ub1.Balance(addrHex(addr), uint64(1+nibble))
	}
	plainKeys1, updates1 := ub1.Build()
	root1 := c.compareBatch(plainKeys1, updates1)
	require.NotEmpty(t, root1)
	t.Logf("RepeatedSingleNibble batch 1 root (16 spread): %x", root1)

	// Batch 2: create 20 accounts all in nibble 0 — heavy single-nibble concentration
	ub2 := NewUpdateBuilder()
	batch2Addrs := make([][]byte, 20)
	for i := 0; i < 20; i++ {
		addr := findAddressForNibble(targetNibble, i)
		batch2Addrs[i] = addr
		ub2.Balance(addrHex(addr), uint64(100+i))
	}
	plainKeys2, updates2 := ub2.Build()
	root2 := c.compareBatch(plainKeys2, updates2)
	require.NotEmpty(t, root2)
	require.NotEqual(t, root1, root2)
	t.Logf("RepeatedSingleNibble batch 2 root (add 20 in nibble %x): %x", targetNibble, root2)

	// Batch 3: update existing + add more in same nibble — incremental single-nibble
	ub3 := NewUpdateBuilder()
	for i := 0; i < 10; i++ {
		ub3.Balance(addrHex(batch2Addrs[i]), uint64(5000+i))
	}
	batch3NewAddrs := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		addr := findAddressForNibble(targetNibble, 20+i)
		batch3NewAddrs[i] = addr
		ub3.Balance(addrHex(addr), uint64(3000+i))
	}
	plainKeys3, updates3 := ub3.Build()
	root3 := c.compareBatch(plainKeys3, updates3)
	require.NotEmpty(t, root3)
	require.NotEqual(t, root2, root3)
	t.Logf("RepeatedSingleNibble batch 3 root (update 10 + add 10 in nibble %x): %x", targetNibble, root3)

	// Batch 4: delete half, update rest — all still in nibble 0
	allNibbleAddrs := append(batch2Addrs, batch3NewAddrs...)
	ub4 := NewUpdateBuilder()
	for i := 0; i < 15; i++ {
		ub4.Delete(addrHex(allNibbleAddrs[i]))
	}
	for i := 15; i < 30; i++ {
		ub4.Balance(addrHex(allNibbleAddrs[i]), uint64(99999+i))
	}
	plainKeys4, updates4 := ub4.Build()
	root4 := c.compareBatch(plainKeys4, updates4)
	require.NotEmpty(t, root4)
	require.NotEqual(t, root3, root4)
	t.Logf("RepeatedSingleNibble batch 4 root (delete 15, update 15 in nibble %x): %x", targetNibble, root4)
}

func TestCompareRoots_MultiBatch_AlternatingConcentration(t *testing.T) {
	t.Parallel()
	c := newMultiBatchComparer(t)

	// Tests extension trimming across different nibbles with multi-batch
	// carry-forward. Starts with a spread to enable concurrent mode, then
	// alternates concentrated updates across different nibbles.

	// Batch 1: spread across all 16 nibbles to establish concurrent mode
	ub1 := NewUpdateBuilder()
	for nibble := 0; nibble < 16; nibble++ {
		addr := findAddressForNibble(nibble, 50)
		ub1.Balance(addrHex(addr), uint64(1+nibble))
	}
	plainKeys1, updates1 := ub1.Build()
	root1 := c.compareBatch(plainKeys1, updates1)
	require.NotEmpty(t, root1)
	t.Logf("AlternatingConcentration batch 1 root (16 spread): %x", root1)

	// Batch 2: concentrate in nibble 0x3
	ub2 := NewUpdateBuilder()
	for i := 0; i < 15; i++ {
		addr := findAddressForNibble(0x3, i)
		ub2.Balance(addrHex(addr), uint64(100+i))
	}
	plainKeys2, updates2 := ub2.Build()
	root2 := c.compareBatch(plainKeys2, updates2)
	require.NotEmpty(t, root2)
	require.NotEqual(t, root1, root2)
	t.Logf("AlternatingConcentration batch 2 root (15 in nibble 0x3): %x", root2)

	// Batch 3: concentrate in nibble 0xA
	ub3 := NewUpdateBuilder()
	for i := 0; i < 15; i++ {
		addr := findAddressForNibble(0xA, i)
		ub3.Balance(addrHex(addr), uint64(2000+i))
	}
	plainKeys3, updates3 := ub3.Build()
	root3 := c.compareBatch(plainKeys3, updates3)
	require.NotEmpty(t, root3)
	require.NotEqual(t, root2, root3)
	t.Logf("AlternatingConcentration batch 3 root (15 in nibble 0xA): %x", root3)

	// Batch 4: spread across all 16 nibbles again
	ub4 := NewUpdateBuilder()
	total := 0
	for nibble := 0; nibble < 16; nibble++ {
		addr := findAddressForNibble(nibble, 20)
		ub4.Balance(addrHex(addr), uint64(8000+nibble))
		total++
	}
	plainKeys4, updates4 := ub4.Build()
	root4 := c.compareBatch(plainKeys4, updates4)
	require.NotEmpty(t, root4)
	require.NotEqual(t, root3, root4)
	t.Logf("AlternatingConcentration batch 4 root (%d spread across all nibbles): %x", total, root4)
}

// --- Edge case tests ---

func TestCompareRoots_EmptyUpdates(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	// Zero keys — both tries should produce the same (empty) root.
	plainKeys, updates := NewUpdateBuilder().Build()
	require.Empty(t, plainKeys, "sanity: no keys expected")

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	t.Logf("EmptyUpdates root: %x", root)
}

func TestCompareRoots_SingleAccountManyStorageSlots(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	addr := findAddressForNibble(0x7, 0)
	ah := addrHex(addr)

	ub := NewUpdateBuilder()
	ub.Balance(ah, 42)
	// 120 storage slots on a single account
	for slot := 0; slot < 120; slot++ {
		loc := makeStorageLoc(slot)
		val := makeStorageLoc(slot + 10000)
		ub.Storage(ah, loc, val)
	}
	plainKeys, updates := ub.Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)
	t.Logf("SingleAccountManyStorageSlots root (1 account, 120 storage slots): %x", root)
}

// findAddressesWithSharedPrefix brute-force searches for `count` addresses
// whose keccak256 hashes share the first `sharedNibbles` nibbles. Returns
// a slice of plain 20-byte addresses.
func findAddressesWithSharedPrefix(sharedNibbles int, count int) [][]byte {
	// We'll find one reference address, then brute force more that share
	// the same keccak prefix.
	sharedBytes := sharedNibbles / 2
	oddNibble := sharedNibbles%2 == 1

	var results [][]byte
	var refHash []byte

	var addr [20]byte
	counter := uint64(0)
	iter := 0
	for len(results) < count {
		if iter >= maxAddrSearchIters {
			panic(fmt.Sprintf("findAddressesWithSharedPrefix(sharedNibbles=%d, count=%d): exceeded %d iterations after collecting %d", sharedNibbles, count, maxAddrSearchIters, len(results)))
		}
		iter++
		binary.BigEndian.PutUint64(addr[:8], counter)
		// Use second 8 bytes too for more entropy
		binary.BigEndian.PutUint64(addr[10:18], counter*7+3)
		h := crypto.Keccak256(addr[:])

		if refHash == nil {
			// First address defines the reference prefix
			refHash = h
			result := make([]byte, 20)
			copy(result, addr[:])
			results = append(results, result)
		} else {
			match := true
			for i := 0; i < sharedBytes && match; i++ {
				if h[i] != refHash[i] {
					match = false
				}
			}
			if match && oddNibble {
				// Check the upper nibble of the next byte
				if (h[sharedBytes] >> 4) != (refHash[sharedBytes] >> 4) {
					match = false
				}
			}
			if match {
				result := make([]byte, 20)
				copy(result, addr[:])
				results = append(results, result)
			}
		}
		counter++
	}
	return results
}

func TestCompareRoots_ExtensionNodes(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	// Find addresses that share the first 4 nibbles (2 bytes) of their
	// keccak hash. These will land in the same nibble and force extension
	// node creation deep in the trie.
	addrs := findAddressesWithSharedPrefix(4, 10)

	ub := NewUpdateBuilder()
	for i, addr := range addrs {
		ah := addrHex(addr)
		ub.Balance(ah, uint64(1000+i))
	}
	plainKeys, updates := ub.Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)

	// Verify the addresses actually share a prefix by checking keccak hashes
	firstHash := crypto.Keccak256(addrs[0])
	for i := 1; i < len(addrs); i++ {
		h := crypto.Keccak256(addrs[i])
		require.Equal(t, firstHash[:2], h[:2],
			"address %d keccak prefix mismatch", i)
	}
	t.Logf("ExtensionNodes root (%d addresses sharing 4-nibble keccak prefix %x): %x",
		len(addrs), firstHash[:2], root)
}

func TestCompareRoots_LargeScale(t *testing.T) {
	t.Parallel()
	seqMs, parMs, seqTrie, parTrie := setupTriePair(t)

	const numAccounts = 1200
	ub := NewUpdateBuilder()
	for i := 0; i < numAccounts; i++ {
		nibble := i % 16
		seed := i / 16
		addr := findAddressForNibble(nibble, seed)
		ub.Balance(addrHex(addr), uint64(i+1))
	}
	plainKeys, updates := ub.Build()

	root := compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)
	require.NotEmpty(t, root)
	t.Logf("LargeScale root (%d accounts spread across 16 nibbles): %x", numAccounts, root)
}
