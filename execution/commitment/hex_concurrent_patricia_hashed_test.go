package commitment

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
)

// nibbleAddressCache caches brute-forced addresses for each nibble to avoid
// repeated keccak work across tests.
var (
	nibbleAddressCacheMu sync.Mutex
	nibbleAddressCache   = make(map[int][][]byte) // nibble -> list of addresses found so far
)

// findAddressForNibble brute-force searches for a 20-byte address whose
// keccak256 first nibble (upper 4 bits of hash[0]) matches targetNibble.
// seed controls the starting point for the search; each unique seed produces
// a different address. Results are cached globally.
func findAddressForNibble(targetNibble int, seed int) []byte {
	nibbleAddressCacheMu.Lock()
	cached := nibbleAddressCache[targetNibble]
	if seed < len(cached) {
		addr := cached[seed]
		nibbleAddressCacheMu.Unlock()
		return addr
	}
	nibbleAddressCacheMu.Unlock()

	// Brute force: we encode a counter into the first 8 bytes of a 20-byte
	// address and increment until keccak(addr)[0] >> 4 == targetNibble.
	var addr [20]byte
	// Use seed * large prime to separate search spaces for different seeds.
	counter := uint64(seed) * 1_000_003
	for {
		binary.BigEndian.PutUint64(addr[:8], counter)
		h := crypto.Keccak256(addr[:])
		if int(h[0]>>4) == targetNibble {
			result := make([]byte, 20)
			copy(result, addr[:])

			nibbleAddressCacheMu.Lock()
			nibbleAddressCache[targetNibble] = append(nibbleAddressCache[targetNibble], result)
			nibbleAddressCacheMu.Unlock()
			return result
		}
		counter++
	}
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
	const heavyCount = 45  // ~90%
	const spreadCount = 5  // ~10% spread across other nibbles

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

