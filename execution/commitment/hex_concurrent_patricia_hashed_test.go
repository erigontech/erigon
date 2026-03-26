package commitment

import (
	"context"
	"encoding/binary"
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
