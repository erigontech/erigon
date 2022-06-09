/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package commitment

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyState(t *testing.T) {
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(1, ms.branchFn, ms.accountFn, ms.storageFn)
	hph.SetTrace(false)
	plainKeys, hashedKeys, updates := NewUpdateBuilder().
		Balance("00", 4).
		Balance("01", 5).
		Balance("02", 6).
		Balance("03", 7).
		Balance("04", 8).
		Storage("04", "01", "0401").
		Storage("03", "56", "050505").
		Storage("03", "57", "060606").
		Balance("05", 9).
		Storage("05", "02", "8989").
		Storage("05", "04", "9898").
		Build()

	err := ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	rootHash, branchNodeUpdates, err := hph.ReviewKeys(plainKeys, hashedKeys)
	require.NoError(t, err)

	t.Logf("root hash %x\n", rootHash)

	ms.applyBranchNodeUpdates(branchNodeUpdates)

	fmt.Printf("1. Generated updates\n")
	renderUpdates(branchNodeUpdates)

	// More updates
	hph.Reset()
	hph.SetTrace(false)
	plainKeys, hashedKeys, updates = NewUpdateBuilder().
		Storage("03", "58", "050505").
		Build()
	if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
		t.Fatal(err)
	}
	_, branchNodeUpdates, err = hph.ReviewKeys(plainKeys, hashedKeys)
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
		Storage("03", "58", "070807").
		Build()
	if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
		t.Fatal(err)
	}
	_, branchNodeUpdates, err = hph.ReviewKeys(plainKeys, hashedKeys)
	require.NoError(t, err)

	ms.applyBranchNodeUpdates(branchNodeUpdates)
	fmt.Printf("3. Generated updates\n")
	renderUpdates(branchNodeUpdates)
}

func Test_HexPatriciaHashed_EmptyUpdateState(t *testing.T) {
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(1, ms.branchFn, ms.accountFn, ms.storageFn)
	hph.SetTrace(false)
	plainKeys, hashedKeys, updates := NewUpdateBuilder().
		Balance("00", 4).
		Nonce("00", 246462653).
		Balance("01", 5).
		CodeHash("03", "aaaaaaaaaaf7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a870").
		Delete("00").
		Storage("04", "01", "0401").
		Storage("03", "56", "050505").
		Build()

	err := ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	hashBeforeEmptyUpdate, branchNodeUpdates, err := hph.ReviewKeys(plainKeys, hashedKeys)
	require.NoError(t, err)
	require.NotEmpty(t, hashBeforeEmptyUpdate)

	ms.applyBranchNodeUpdates(branchNodeUpdates)

	fmt.Println("1. Updates applied")
	renderUpdates(branchNodeUpdates)

	// generate empty updates and do NOT reset tree
	hph.SetTrace(true)

	plainKeys, hashedKeys, updates = NewUpdateBuilder().Build()

	err = ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	hashAfterEmptyUpdate, branchNodeUpdates, err := hph.ReviewKeys(plainKeys, hashedKeys)
	require.NoError(t, err)

	ms.applyBranchNodeUpdates(branchNodeUpdates)
	fmt.Println("2. Empty updates applied without state reset")

	require.EqualValues(t, hashBeforeEmptyUpdate, hashAfterEmptyUpdate)
}

func Test_HexPatriciaHashed_ProcessUpdates_UniqueRepresentation(t *testing.T) {
	ms := NewMockState(t)
	ms2 := NewMockState(t)

	plainKeys, hashedKeys, updates := NewUpdateBuilder().
		Balance("f4", 4).
		Balance("ff", 900234).
		Balance("04", 1233).
		Storage("04", "01", "0401").
		Balance("ba", 065606).
		Balance("00", 4).
		Balance("01", 5).
		Balance("02", 6).
		Balance("03", 7).
		Storage("03", "56", "050505").
		Balance("05", 9).
		Storage("03", "87", "060606").
		Balance("b9", 6).
		Nonce("ff", 169356).
		Storage("05", "02", "8989").
		Storage("f5", "04", "9898").
		Build()

	trieOne := NewHexPatriciaHashed(1, ms.branchFn, ms.accountFn, ms.storageFn)
	trieTwo := NewHexPatriciaHashed(1, ms2.branchFn, ms2.accountFn, ms2.storageFn)

	trieOne.SetTrace(true)
	trieTwo.SetTrace(true)

	// single sequential update
	roots := make([][]byte, 0)
	// branchNodeUpdatesOne := make(map[string]BranchData)
	fmt.Printf("1. Trie sequential update generated following branch updates\n")
	for i := 0; i < len(updates); i++ {
		if err := ms.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1]); err != nil {
			t.Fatal(err)
		}

		sequentialRoot, branchNodeUpdates, err := trieOne.ReviewKeys(plainKeys[i:i+1], hashedKeys[i:i+1])
		require.NoError(t, err)
		roots = append(roots, sequentialRoot)

		ms.applyBranchNodeUpdates(branchNodeUpdates)
		renderUpdates(branchNodeUpdates)
	}

	err := ms2.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	fmt.Printf("\n2. Trie batch update generated following branch updates\n")
	// batch update
	batchRoot, branchNodeUpdatesTwo, err := trieTwo.ReviewKeys(plainKeys, hashedKeys)
	require.NoError(t, err)
	renderUpdates(branchNodeUpdatesTwo)

	fmt.Printf("\n sequential roots:\n")
	for i, rh := range roots {
		fmt.Printf("%2d %+v\n", i, hex.EncodeToString(rh))
	}

	ms2.applyBranchNodeUpdates(branchNodeUpdatesTwo)

	require.EqualValues(t, batchRoot, roots[len(roots)-1],
		"expected equal roots, got sequential [%v] != batch [%v]", hex.EncodeToString(roots[len(roots)-1]), hex.EncodeToString(batchRoot))
	require.Lenf(t, batchRoot, 32, "root hash length should be equal to 32 bytes")
}
