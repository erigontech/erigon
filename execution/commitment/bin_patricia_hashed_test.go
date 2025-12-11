// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commitment

//import (
//	"context"
//	"encoding/hex"
//	"fmt"
//	"slices"
//	"testing"
//
//	"github.com/stretchr/testify/require"
//
//	"github.com/erigontech/erigon-lib/common/length"
//)
//
//func Test_BinPatriciaTrie_UniqueRepresentation(t *testing.T) {
//	t.Skip()
//	ctx := context.Background()
//
//	ms := NewMockState(t)
//	ms2 := NewMockState(t)
//
//	trie := NewBinPatriciaHashed(length.Addr, ms, ms.TempDir())
//	trieBatch := NewBinPatriciaHashed(length.Addr, ms2, ms2.TempDir())
//
//	plainKeys, updates := NewUpdateBuilder().
//		Balance("e25652aaa6b9417973d325f9a1246b48ff9420bf", 12).
//		Balance("cdd0a12034e978f7eccda72bd1bd89a8142b704e", 120000).
//		Balance("5bb6abae12c87592b940458437526cb6cad60d50", 170).
//		Nonce("5bb6abae12c87592b940458437526cb6cad60d50", 152512).
//		Balance("2fcb355beb0ea2b5fcf3b62a24e2faaff1c8d0c0", 100000).
//		Balance("463510be61a7ccde354509c0ab813e599ee3fc8a", 200000).
//		Balance("cd3e804beea486038609f88f399140dfbe059ef3", 200000).
//		Storage("cd3e804beea486038609f88f399140dfbe059ef3", "01023402", "98").
//		Balance("82c88c189d5deeba0ad11463b80b44139bd519c1", 300000).
//		Balance("0647e43e8f9ba3fb8b14ad30796b7553d667c858", 400000).
//		Delete("cdd0a12034e978f7eccda72bd1bd89a8142b704e").
//		Balance("06548d648c23b12f2e9bfd1bae274b658be208f4", 500000).
//		Balance("e5417f49640cf8a0b1d6e38f9dfdc00196e99e8b", 600000).
//		Nonce("825ac9fa5d015ec7c6b4cbbc50f78d619d255ea7", 184).
//		Build()
//
//	ms.applyPlainUpdates(plainKeys, updates)
//	ms2.applyPlainUpdates(plainKeys, updates)
//
//	fmt.Println("1. Running sequential updates over the bin trie")
//	var seqHash []byte
//	for i := 0; i < len(updates); i++ {
//		sh, err := trie.ProcessKeys(ctx, plainKeys[i:i+1], "")
//		require.NoError(t, err)
//		require.Len(t, sh, length.Hash)
//		// WARN! provided sequential branch updates are incorrect - lead to deletion of prefixes (afterMap is zero)
//		//       while root hashes are equal
//		//renderUpdates(branchNodeUpdates)
//
//		fmt.Printf("h=%x\n", sh)
//		seqHash = sh
//	}
//
//	fmt.Println("2. Running batch updates over the bin trie")
//
//	batchHash, err := trieBatch.ProcessKeys(ctx, plainKeys, "")
//	require.NoError(t, err)
//	//ms2.applyBranchNodeUpdates(branchBatchUpdates)
//
//	//renderUpdates(branchBatchUpdates)
//
//	require.EqualValues(t, seqHash, batchHash)
//	// require.EqualValues(t, seqHash, batchHash)
//
//	// expectedHash, _ := hex.DecodeString("3ed2b89c0f9c6ebc7fa11a181baac21aa0236b12bb4492c708562cb3e40c7c9e")
//	// require.EqualValues(t, expectedHash, seqHash)
//}
//
//func renderUpdates(branchNodeUpdates map[string]BranchData) {
//	keys := make([]string, 0, len(branchNodeUpdates))
//	for key := range branchNodeUpdates {
//		keys = append(keys, key)
//	}
//	slices.Sort(keys)
//	for _, key := range keys {
//		branchNodeUpdate := branchNodeUpdates[key]
//		fmt.Printf("%x => %s\n", CompactedKeyToHex([]byte(key)), branchNodeUpdate.String())
//	}
//}
//
//func Test_BinPatriciaHashed_UniqueRepresentation(t *testing.T) {
//	t.Skip()
//	ctx := context.Background()
//
//	ms := NewMockState(t)
//	ms2 := NewMockState(t)
//
//	plainKeys, updates := NewUpdateBuilder().
//		Balance("f5", 4).
//		Balance("ff", 900234).
//		Balance("04", 1233).
//		Storage("04", "01", "0401").
//		Balance("ba", 065606).
//		Balance("00", 4).
//		Balance("01", 5).
//		Balance("02", 6).
//		Balance("03", 7).
//		Storage("03", "56", "050505").
//		Balance("05", 9).
//		Storage("03", "87", "060606").
//		Balance("b9", 6).
//		Nonce("ff", 169356).
//		Storage("05", "02", "8989").
//		Storage("f5", "04", "9898").
//		Build()
//
//	trieOne := NewBinPatriciaHashed(1, ms, ms.TempDir())
//	trieTwo := NewBinPatriciaHashed(1, ms2, ms2.TempDir())
//
//	trieOne.SetTrace(true)
//	trieTwo.SetTrace(true)
//
//	// single sequential update
//	roots := make([][]byte, 0)
//	// branchNodeUpdatesOne := make(map[string]BranchData)
//	fmt.Printf("1. Trie sequential update generated following branch updates\n")
//	for i := 0; i < len(updates); i++ {
//		if err := ms.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1]); err != nil {
//			t.Fatal(err)
//		}
//
//		sequentialRoot, err := trieOne.ProcessKeys(ctx, plainKeys[i:i+1], "")
//		require.NoError(t, err)
//		roots = append(roots, sequentialRoot)
//
//		//ms.applyBranchNodeUpdates(branchNodeUpdates)
//		//renderUpdates(branchNodeUpdates)
//	}
//
//	err := ms2.applyPlainUpdates(plainKeys, updates)
//	require.NoError(t, err)
//
//	fmt.Printf("\n2. Trie batch update generated following branch updates\n")
//	// batch update
//	batchRoot, err := trieTwo.ProcessKeys(ctx, plainKeys, "")
//	require.NoError(t, err)
//	//renderUpdates(branchNodeUpdatesTwo)
//
//	fmt.Printf("\n sequential roots:\n")
//	for i, rh := range roots {
//		fmt.Printf("%2d %+v\n", i, hex.EncodeToString(rh))
//	}
//
//	//ms2.applyBranchNodeUpdates(branchNodeUpdatesTwo)
//
//	require.EqualValues(t, batchRoot, roots[len(roots)-1],
//		"expected equal roots, got sequential [%v] != batch [%v]", hex.EncodeToString(roots[len(roots)-1]), hex.EncodeToString(batchRoot))
//	require.Lenf(t, batchRoot, 32, "root hash length should be equal to 32 bytes")
//}
//func Test_BinPatriciaHashed_EmptyState(t *testing.T) {
//	ctx := context.Background()
//	ms := NewMockState(t)
//	hph := NewBinPatriciaHashed(1, ms, ms.TempDir())
//	hph.SetTrace(false)
//	plainKeys, updates := NewUpdateBuilder().
//		Balance("00", 4).
//		Balance("01", 5).
//		Balance("02", 6).
//		Balance("03", 7).
//		Balance("04", 8).
//		Storage("04", "01", "0401").
//		Storage("03", "56", "050505").
//		Storage("03", "57", "060606").
//		Balance("05", 9).
//		Storage("05", "02", "8989").
//		Storage("05", "04", "9898").
//		Build()
//
//	err := ms.applyPlainUpdates(plainKeys, updates)
//	require.NoError(t, err)
//
//	firstRootHash, err := hph.ProcessKeys(ctx, plainKeys, "")
//	require.NoError(t, err)
//
//	t.Logf("root hash %x\n", firstRootHash)
//
//	//ms.applyBranchNodeUpdates(branchNodeUpdates)
//
//	fmt.Printf("1. Generated updates\n")
//	//renderUpdates(branchNodeUpdates)
//
//	// More updates
//	hph.Reset()
//	hph.SetTrace(false)
//	plainKeys, updates = NewUpdateBuilder().
//		Storage("03", "58", "050505").
//		Build()
//	err = ms.applyPlainUpdates(plainKeys, updates)
//	require.NoError(t, err)
//
//	secondRootHash, err := hph.ProcessKeys(ctx, plainKeys, "")
//	require.NoError(t, err)
//	require.NotEqualValues(t, firstRootHash, secondRootHash)
//
//	//ms.applyBranchNodeUpdates(branchNodeUpdates)
//	fmt.Printf("2. Generated single update\n")
//	//renderUpdates(branchNodeUpdates)
//
//	// More updates
//	//hph.Reset() // one update - no need to reset
//	hph.SetTrace(false)
//	plainKeys, updates = NewUpdateBuilder().
//		Storage("03", "58", "070807").
//		Build()
//	err = ms.applyPlainUpdates(plainKeys, updates)
//	require.NoError(t, err)
//
//	thirdRootHash, err := hph.ProcessKeys(ctx, plainKeys, "")
//	require.NoError(t, err)
//	require.NotEqualValues(t, secondRootHash, thirdRootHash)
//
//	//ms.applyBranchNodeUpdates(branchNodeUpdates)
//	fmt.Printf("3. Generated single update\n")
//	//renderUpdates(branchNodeUpdates)
//}
//
//func Test_BinPatriciaHashed_EmptyUpdateState(t *testing.T) {
//	ctx := context.Background()
//	ms := NewMockState(t)
//	hph := NewBinPatriciaHashed(1, ms, ms.TempDir())
//	hph.SetTrace(false)
//	plainKeys, updates := NewUpdateBuilder().
//		Balance("00", 4).
//		Nonce("00", 246462653).
//		Balance("01", 5).
//		CodeHash("03", "aaaaaaaaaaf7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a870").
//		Delete("00").
//		Storage("04", "01", "0401").
//		Storage("03", "56", "050505").
//		Build()
//
//	err := ms.applyPlainUpdates(plainKeys, updates)
//	require.NoError(t, err)
//
//	hashBeforeEmptyUpdate, err := hph.ProcessKeys(ctx, plainKeys, "")
//	require.NoError(t, err)
//	require.NotEmpty(t, hashBeforeEmptyUpdate)
//
//	//ms.applyBranchNodeUpdates(branchNodeUpdates)
//
//	fmt.Println("1. Updates applied")
//	//renderUpdates(branchNodeUpdates)
//
//	// generate empty updates and do NOT reset tree
//	hph.SetTrace(true)
//
//	plainKeys, updates = NewUpdateBuilder().Build()
//
//	err = ms.applyPlainUpdates(plainKeys, updates)
//	require.NoError(t, err)
//
//	hashAfterEmptyUpdate, err := hph.ProcessKeys(ctx, plainKeys, "")
//	require.NoError(t, err)
//
//	//ms.applyBranchNodeUpdates(branchNodeUpdates)
//	fmt.Println("2. Empty updates applied without state reset")
//
//	require.EqualValues(t, hashBeforeEmptyUpdate, hashAfterEmptyUpdate)
//}
