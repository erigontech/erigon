package commitment

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func Test_BinPatriciaTrie_UniqueRepresentation(t *testing.T) {
	t.Skip()

	ms := NewMockState(t)
	ms2 := NewMockState(t)

	trie := NewBinPatriciaHashed(length.Addr, ms.branchFn, ms.accountFn, ms.storageFn)
	trieBatch := NewBinPatriciaHashed(length.Addr, ms2.branchFn, ms2.accountFn, ms2.storageFn)

	plainKeys, hashedKeys, updates := NewUpdateBuilder().
		Balance("e25652aaa6b9417973d325f9a1246b48ff9420bf", 12).
		Balance("cdd0a12034e978f7eccda72bd1bd89a8142b704e", 120000).
		Balance("5bb6abae12c87592b940458437526cb6cad60d50", 170).
		Nonce("5bb6abae12c87592b940458437526cb6cad60d50", 152512).
		Balance("2fcb355beb0ea2b5fcf3b62a24e2faaff1c8d0c0", 100000).
		Balance("463510be61a7ccde354509c0ab813e599ee3fc8a", 200000).
		Balance("cd3e804beea486038609f88f399140dfbe059ef3", 200000).
		Storage("cd3e804beea486038609f88f399140dfbe059ef3", "01023402", "98").
		Balance("82c88c189d5deeba0ad11463b80b44139bd519c1", 300000).
		Balance("0647e43e8f9ba3fb8b14ad30796b7553d667c858", 400000).
		Delete("cdd0a12034e978f7eccda72bd1bd89a8142b704e").
		Balance("06548d648c23b12f2e9bfd1bae274b658be208f4", 500000).
		Balance("e5417f49640cf8a0b1d6e38f9dfdc00196e99e8b", 600000).
		Nonce("825ac9fa5d015ec7c6b4cbbc50f78d619d255ea7", 184).
		Build()

	ms.applyPlainUpdates(plainKeys, updates)
	ms2.applyPlainUpdates(plainKeys, updates)

	fmt.Println("1. Running sequential updates over the bin trie")
	var seqHash []byte
	for i := 0; i < len(updates); i++ {
		sh, branchNodeUpdates, err := trie.ReviewKeys(plainKeys[i:i+1], hashedKeys[i:i+1])
		require.NoError(t, err)
		require.Len(t, sh, length.Hash)
		ms.applyBranchNodeUpdates(branchNodeUpdates)
		// WARN! provided sequential branch updates are incorrect - lead to deletion of prefixes (afterMap is zero)
		//       while root hashes are equal
		renderUpdates(branchNodeUpdates)

		fmt.Printf("h=%x\n", sh)
		seqHash = sh
	}

	fmt.Println("2. Running batch updates over the bin trie")

	batchHash, branchBatchUpdates, err := trieBatch.ReviewKeys(plainKeys, hashedKeys)
	require.NoError(t, err)
	ms2.applyBranchNodeUpdates(branchBatchUpdates)

	renderUpdates(branchBatchUpdates)

	require.EqualValues(t, seqHash, batchHash)
	// require.EqualValues(t, seqHash, batchHash)

	// expectedHash, _ := hex.DecodeString("3ed2b89c0f9c6ebc7fa11a181baac21aa0236b12bb4492c708562cb3e40c7c9e")
	// require.EqualValues(t, expectedHash, seqHash)
}

func Test_BinPatriciaTrie_EmptyState(t *testing.T) {
	t.Skip()

	ms := NewMockState(t)
	hph := NewBinPatriciaHashed(1, ms.branchFn, ms.accountFn, ms.storageFn)
	hph.SetTrace(true)

	plainKeys, hashedKeys, updates := NewUpdateBuilder().
		Balance("00", 4).
		Balance("01", 5).
		Balance("02", 6).
		Balance("03", 7).
		Balance("04", 8).
		Storage("04", "01", "0401").
		Storage("03", "56", "050505").
		Storage("03", "a057", "060606").
		Balance("05", 9).
		Storage("05", "02", "8989").
		Storage("05", "04", "9898").
		Build()
	if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
		t.Fatal(err)
	}

	_, branchNodeUpdates, err := hph.ReviewKeys(plainKeys, hashedKeys)
	if err != nil {
		t.Fatal(err)
	}
	ms.applyBranchNodeUpdates(branchNodeUpdates)
	fmt.Printf("1. Generated updates\n")
	renderUpdates(branchNodeUpdates)

	// More updates
	hph.Reset()
	hph.SetTrace(false)
	plainKeys, hashedKeys, updates = NewUpdateBuilder().
		Balance("03", 1231234).
		Storage("03", "58", "050505").
		Build()
	if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
		t.Fatal(err)
	}

	almostEmptyBinRoot, branchNodeUpdates, err := hph.ReviewKeys(plainKeys, hashedKeys)
	if err != nil {
		t.Fatal(err)
	}
	ms.applyBranchNodeUpdates(branchNodeUpdates)
	fmt.Printf("2. Generated updates\n")
	renderUpdates(branchNodeUpdates)

	// More updates
	hph.Reset()
	hph.SetTrace(false)
	plainKeys, hashedKeys, updates = NewUpdateBuilder().
		Balance("03", 1231234).
		Storage("03", "58", "070807").
		Build()
	if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
		t.Fatal(err)
	}
	almostEmptySecondBinRoot, branchNodeUpdates, err := hph.ReviewKeys(plainKeys, hashedKeys)
	if err != nil {
		t.Fatal(err)
	}
	ms.applyBranchNodeUpdates(branchNodeUpdates)
	fmt.Printf("3. Generated updates\n")
	require.EqualValues(t, almostEmptyBinRoot, almostEmptySecondBinRoot)
	renderUpdates(branchNodeUpdates)
}

func renderUpdates(branchNodeUpdates map[string]BranchData) {
	keys := make([]string, 0, len(branchNodeUpdates))
	for key := range branchNodeUpdates {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		branchNodeUpdate := branchNodeUpdates[key]
		fmt.Printf("%x => %s\n", CompactedKeyToHex([]byte(key)), branchNodeUpdate.String())
	}
}
func Test_BinPatriciaHashed_ProcessUpdates_UniqueRepresentation(t *testing.T) {
	t.Skip()

	ms := NewMockState(t)
	ms2 := NewMockState(t)

	plainKeys, hashedKeys, updates := NewUpdateBuilder().
		Balance("f4", 4).
		Storage("04", "01", "0401").
		Balance("ba", 065606).
		Balance("00", 4).
		Balance("01", 5).
		Balance("02", 6).
		Balance("03", 7).
		Storage("03", "56", "050505").
		Balance("05", 9).
		Storage("03", "57", "060606").
		Balance("b9", 6).
		Nonce("ff", 169356).
		Storage("05", "02", "8989").
		Storage("f5", "04", "9898").
		Build()

	trieOne := NewBinPatriciaHashed(1, ms.branchFn, ms.accountFn, ms.storageFn)
	trieTwo := NewBinPatriciaHashed(1, ms2.branchFn, ms2.accountFn, ms2.storageFn)

	trieTwo.Reset()
	trieOne.Reset()

	trieOne.SetTrace(true)
	trieTwo.SetTrace(true)

	// single sequential update
	roots := make([][]byte, 0)
	branchNodeUpdatesOne := make(map[string]BranchData)
	for i := 0; i < len(updates); i++ {
		if err := ms.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1]); err != nil {
			t.Fatal(err)
		}
		sequentialRoot, branchNodeUpdates, err := trieOne.ReviewKeys(plainKeys[i:i+1], hashedKeys[i:i+1])
		require.NoError(t, err)

		roots = append(roots, sequentialRoot)

		ms.applyBranchNodeUpdates(branchNodeUpdates)

		for br, upd := range branchNodeUpdates {
			branchNodeUpdatesOne[br] = upd
		}
		renderUpdates(branchNodeUpdatesOne)
	}

	fmt.Printf("1. Trie sequential update generated following branch updates\n")
	renderUpdates(branchNodeUpdatesOne)

	err := ms2.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)
	for i, root := range roots {
		fmt.Printf("%d [%s]\n", i, hex.EncodeToString(root))
	}

	fmt.Printf("\n\n")

	// batch update
	batchRoot, branchNodeUpdatesTwo, err := trieTwo.ReviewKeys(plainKeys, hashedKeys)
	require.NoError(t, err)

	ms2.applyBranchNodeUpdates(branchNodeUpdatesTwo)

	fmt.Printf("2. Trie batch update generated following branch updates\n")
	renderUpdates(branchNodeUpdatesTwo)

	sequentialRoot, err := trieOne.RootHash()
	require.NoError(t, err)

	require.NotContainsf(t, roots[:len(roots)-1], sequentialRoot, "sequential root %s found in previous hashes", hex.EncodeToString(sequentialRoot))

	require.EqualValues(t, batchRoot, sequentialRoot,
		"expected equal roots, got sequential [%v] != batch [%v]", hex.EncodeToString(sequentialRoot), hex.EncodeToString(batchRoot))
	require.Lenf(t, batchRoot, 32, "root hash length should be equal to 32 bytes")
}
