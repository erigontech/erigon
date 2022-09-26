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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/length"
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
		Balance("f5", 4).
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

func Test_Sepolia(t *testing.T) {
	ms := NewMockState(t)

	type TestData struct {
		expectedRoot string
		balances     map[string][]byte
	}

	tests := []TestData{
		{
			"5eb6e371a698b8d68f665192350ffcecbbbf322916f4b51bd79bb6887da3f494",
			map[string][]byte{
				"a2a6d93439144ffe4d27c9e088dcd8b783946263": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"bc11295936aa79d594139de1b2e12629414f3bdb": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"7cf5b79bfe291a67ab02b393e456ccc4c266f753": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"aaec86394441f915bce3e6ab399977e9906f3b69": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"f47cae1cf79ca6758bfc787dbd21e6bdbe7112b8": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"d7eddb78ed295b3c9629240e8924fb8d8874ddd8": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"8b7f0977bb4f0fbe7076fa22bc24aca043583f5e": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"e2e2659028143784d557bcec6ff3a0721048880a": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"d9a5179f091d85051d3c982785efd1455cec8699": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"beef32ca5b9a198d27b4e02f4c70439fe60356cf": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"0000006916a87b82333f4245046623b23794c65c": {0x08, 0x45, 0x95, 0x16, 0x14, 0x01, 0x48, 0x4a, 0x00, 0x00, 0x00},
				"b21c33de1fab3fa15499c62b59fe0cc3250020d1": {0x52, 0xb7, 0xd2, 0xdc, 0xc8, 0x0c, 0xd2, 0xe4, 0x00, 0x00, 0x00},
				"10f5d45854e038071485ac9e402308cf80d2d2fe": {0x52, 0xb7, 0xd2, 0xdc, 0xc8, 0x0c, 0xd2, 0xe4, 0x00, 0x00, 0x00},
				"d7d76c58b3a519e9fa6cc4d22dc017259bc49f1e": {0x52, 0xb7, 0xd2, 0xdc, 0xc8, 0x0c, 0xd2, 0xe4, 0x00, 0x00, 0x00},
				"799d329e5f583419167cd722962485926e338f4a": {0x0d, 0xe0, 0xb6, 0xb3, 0xa7, 0x64, 0x00, 0x00},
			},
		},
		{
			"c91d4ecd59dce3067d340b3aadfc0542974b4fb4db98af39f980a91ea00db9dc",
			map[string][]byte{
				"2f14582947e292a2ecd20c430b46f2d27cfe213c": {0x1B, 0xC1, 0x6D, 0x67, 0x4E, 0xC8, 0x00, 0x00},
			},
		},
		{
			"c91d4ecd59dce3067d340b3aadfc0542974b4fb4db98af39f980a91ea00db9dc",
			map[string][]byte{},
		},
	}

	hph := NewHexPatriciaHashed(length.Addr, ms.branchFn, ms.accountFn, ms.storageFn)
	hph.SetTrace(true)

	for _, testData := range tests {
		builder := NewUpdateBuilder()

		for address, balance := range testData.balances {
			builder.IncrementBalance(address, balance)
		}
		plainKeys, hashedKeys, updates := builder.Build()

		if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
			t.Fatal(err)
		}

		rootHash, branchNodeUpdates, err := hph.ReviewKeys(plainKeys, hashedKeys)
		if err != nil {
			t.Fatal(err)
		}
		ms.applyBranchNodeUpdates(branchNodeUpdates)

		require.EqualValues(t, testData.expectedRoot, fmt.Sprintf("%x", rootHash))
	}
}

func Test_HexPatriciaHashed_StateEncode(t *testing.T) {
	//trie := NewHexPatriciaHashed(length.Hash, nil, nil, nil)
	var s state
	rnd := rand.New(rand.NewSource(42))
	n, err := rnd.Read(s.CurrentKey[:])
	require.NoError(t, err)
	require.EqualValues(t, 128, n)
	n, err = rnd.Read(s.RootHash[:])
	require.NoError(t, err)
	require.EqualValues(t, 32, n)
	s.RootPresent = true
	s.RootTouched = true
	s.RootChecked = true

	s.CurrentKeyLen = int8(rnd.Intn(129))
	for i := 0; i < len(s.Depths); i++ {
		s.Depths[i] = rnd.Int()
	}
	for i := 0; i < len(s.TouchMap); i++ {
		s.TouchMap[i] = uint16(rnd.Intn(1<<16 - 1))
	}
	for i := 0; i < len(s.AfterMap); i++ {
		s.AfterMap[i] = uint16(rnd.Intn(1<<16 - 1))
	}
	for i := 0; i < len(s.BranchBefore); i++ {
		if rnd.Intn(100) > 49 {
			s.BranchBefore[i] = true
		}
	}

	enc, err := s.Encode(nil)
	require.NoError(t, err)
	require.NotEmpty(t, enc)

	var s1 state
	err = s1.Decode(enc)
	require.NoError(t, err)

	require.EqualValues(t, s.RootHash[:], s1.RootHash[:])
	require.EqualValues(t, s.CurrentKey[:], s1.CurrentKey[:])
	require.EqualValues(t, s.AfterMap[:], s1.AfterMap[:])
	require.EqualValues(t, s.TouchMap[:], s1.TouchMap[:])
	require.EqualValues(t, s.BranchBefore[:], s1.BranchBefore[:])
	require.EqualValues(t, s.RootTouched, s1.RootTouched)
	require.EqualValues(t, s.RootPresent, s1.RootPresent)
	require.EqualValues(t, s.RootChecked, s1.RootChecked)
	require.EqualValues(t, s.CurrentKeyLen, s1.CurrentKeyLen)
}
