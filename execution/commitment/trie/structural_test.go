// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package trie

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/rlp"
)

// nibblizeKey expands each byte of k into two nibbles, appending the terminator
// nibble when term is set.
func nibblizeKey(k []byte, term bool) []byte {
	out := make([]byte, 0, 2*len(k)+1)
	for _, b := range k {
		out = append(out, b/16, b%16)
	}
	if term {
		out = append(out, 16)
	}
	return out
}

// retainAll and retainNone are the two constant retain decisions used below.
func retainAll(_ []byte) bool  { return true }
func retainNone(_ []byte) bool { return false }

// genStructStepsOver drives GenStructStep over consecutive (curr, succ) pairs of hexKeys,
// attaching data(i) to the step whose curr is hexKeys[i], and finishing the last key against
// finalSucc (empty when nothing follows).
func genStructStepsOver(t *testing.T, hb *HashBuilder, retain func([]byte) bool, hc HashCollector, hexKeys [][]byte, data func(i int) GenStructStepData, finalSucc []byte) {
	t.Helper()
	var groups, hasTree, hasHash []uint16
	var err error
	for i := 1; i < len(hexKeys); i++ {
		groups, hasTree, hasHash, err = GenStructStep(retain, hexKeys[i-1], hexKeys[i], hb, hc, data(i-1), groups, hasTree, hasHash, false)
		require.NoError(t, err, "GenStructStep at key %d", i-1)
	}
	last := len(hexKeys) - 1
	_, _, _, err = GenStructStep(retain, hexKeys[last], finalSucc, hb, hc, data(last), groups, hasTree, hasHash, false)
	require.NoError(t, err, "final GenStructStep")
}

// stepExpectation is one expected emitted step: the hex key and its hasHash/hasTree bitmasks.
type stepExpectation struct {
	keyHex  []byte
	hasHash uint16
	hasTree uint16
}

// expectingCollector returns a HashCollector that counts emitted steps (those with a non-zero
// hasHash or hasTree) into *count and asserts each against steps in order; extra steps beyond
// len(steps) are counted but not asserted, so callers keep their own final count check.
func expectingCollector(t *testing.T, count *int, steps []stepExpectation) HashCollector {
	return func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		if hasHash == 0 && hasTree == 0 {
			return nil
		}
		*count++
		if *count <= len(steps) {
			s := steps[*count-1]
			require.Equal(t, s.keyHex, keyHex)
			require.Equal(t, s.hasHash, hasHash)
			require.Equal(t, s.hasTree, hasTree)
			require.NotNil(t, hashes)
		}
		return nil
	}
}

// genHashedKeys produces n sorted 8-byte keys derived from keccak hashes of a counter.
func genHashedKeys(n uint32) []string {
	keys := make([]string, 0, n)
	for b := uint32(0); b < n; b++ {
		var preimage [4]byte
		binary.BigEndian.PutUint32(preimage[:], b)
		keys = append(keys, string(crypto.Keccak256(preimage[:])[:8]))
	}
	slices.Sort(keys)
	return keys
}

func TestV2HashBuilding(t *testing.T) {
	keys := genHashedKeys(100000)
	tr := newEmpty()
	valueLong := []byte("VALUE123985903485903489043859043859043859048590485904385903485940385439058934058439058439058439058940385904358904385438809348908345")
	valueShort := []byte("VAL")
	valueOf := func(i int) []byte {
		if i%2 == 0 {
			return valueLong
		}
		return valueShort
	}
	for i, key := range keys {
		tr.Update([]byte(key), valueOf(i))
	}
	trieHash := tr.Hash()

	hb := NewHashBuilder(false)
	hexKeys := make([][]byte, len(keys))
	for i, key := range keys {
		hexKeys[i] = nibblizeKey([]byte(key), true)
	}
	genStructStepsOver(t, hb, retainNone, nil, hexKeys, func(i int) GenStructStepData {
		return &GenStructStepLeafData{rlp.RlpSerializableBytes(valueOf(i))}
	}, nil)

	builtHash := hb.rootHash()
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
}

func TestV2Resolution(t *testing.T) {
	keys := genHashedKeys(100000)
	tr := newEmpty()
	value := []byte("VALUE123985903485903489043859043859043859048590485904385903485940385439058934058439058439058439058940385904358904385438809348908345")
	for _, key := range keys {
		tr.Update([]byte(key), value)
	}
	trieHash := tr.Hash()

	// Choose some keys to be resolved
	var rl RetainList
	// First, existing keys
	for i := 0; i < 1000; i += 200 {
		rl.AddKey([]byte(keys[i]))
	}
	// Next, some non-existing keys
	for i := 0; i < 1000; i++ {
		rl.AddKey(crypto.Keccak256([]byte(keys[i]))[:8])
	}

	hb := NewHashBuilder(false)
	hexKeys := make([][]byte, len(keys))
	for i, key := range keys {
		hexKeys[i] = nibblizeKey([]byte(key), true)
	}
	genStructStepsOver(t, hb, rl.Retain, nil, hexKeys, func(i int) GenStructStepData {
		return &GenStructStepLeafData{rlp.RlpSerializableBytes(value)}
	}, nil)

	tr1 := newEmpty()
	tr1.RootNode = hb.root()
	builtHash := hb.rootHash()
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
	// Check the availability of the resolved keys
	for _, hex := range rl.hexes {
		key := nibbles.HexToKeybytes(hex)
		_, found := tr1.Get(key)
		if !found {
			t.Errorf("Key %x was not resolved", hex)
		}
	}
}

// In this test, we try to combine both accounts and their storage items in the single
// hash builder by tricking the GenStructStep slightly.
// For storage items, we will be using the keys which are concatenation of the contract address hash,
// incarnation encoding, and the storage location hash.
// If we just allow it to be processed natually, then at the end of the processing of all storage
// items, we would have entension node which hasTree off at some point, but includes incarnation encoding
// in it, which we do not want. To cut it off, we will use the "trick". When we give the last
// storage item to the GenStructStep, instead of setting `succ` to the empty slice, indicating that
// nothing follows, we will set `succ` to a key which is the concatenation of the address hash,
// incarnation encoding, except that the last nibble of the incoding is arbitrarily modified
// This will cause the correct extension node to form.
// In order to prevent the branch node on top of the extension node, we will need to manipulate
// the `groups` array and truncate it to the level of the accounts
func TestEmbeddedStorage(t *testing.T) {
	var accountAddress = common.Address{3, 4, 5, 6}
	addrHash := crypto.Keccak256(accountAddress[:])
	var location1 = common.Hash{1}
	locationKey1 := append(append([]byte{}, addrHash...), crypto.Keccak256(location1[:])...)
	var location2 = common.Hash{2}
	locationKey2 := append(append([]byte{}, addrHash...), crypto.Keccak256(location2[:])...)
	var location3 = common.Hash{3}
	locationKey3 := append(append([]byte{}, addrHash...), crypto.Keccak256(location3[:])...)
	var keys = []string{string(locationKey1), string(locationKey2), string(locationKey3)}
	slices.Sort(keys)
	tr := newEmpty()
	valueShort := []byte("VAL")
	for _, key := range keys {
		tr.Update([]byte(key)[length.Hash:], valueShort)
	}
	trieHash := tr.Hash()

	hb := NewHashBuilder(true)
	hexKeys := make([][]byte, len(keys))
	for i, key := range keys {
		hexKeys[i] = nibblizeKey([]byte(key), true)
	}
	// Produce the final succ as a specially modified version of the last key's prefix
	// (only different in the last nibble before the cutoff)
	cutoff := 2 * length.Hash
	last := hexKeys[len(hexKeys)-1]
	finalSucc := append(common.Copy(last[:cutoff-1]), last[cutoff-1]+1)

	genStructStepsOver(t, hb, retainAll, nil, hexKeys, func(i int) GenStructStepData {
		return &GenStructStepLeafData{rlp.RlpSerializableBytes(valueShort)}
	}, finalSucc)

	builtHash := hb.rootHash()
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
}

// TestEmbeddedStorage11 feeds prefixed storage keys with an incarnation suffix through
// GenStructStep, checking only that every step succeeds (the trick is the same as in
// TestEmbeddedStorage, with a wider cutoff).
func TestEmbeddedStorage11(t *testing.T) {
	keys := []struct {
		k []byte
		v []byte
	}{

		{
			k: common.FromHex("fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae9800000000000000010d2f4a412d2809e00f42a7f8cb0e659bddf0b4f201d24eb1b2946493cbae334c"),
			v: common.FromHex("496e7374616e6365000000000000000000000000000000000000000000000000"),
		},
		{
			k: common.FromHex("fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae98000000000000000123a5384746519cbca71a22098063e5608768276f2dc212e71fd2c6c643c726c4"),
			v: common.FromHex("65eea643e9a9d6f5f2f7e13ccdff36cf45b46aab"),
		},
		{
			k: common.FromHex("fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae980000000000000001387a79e493fff57a9c96dc0a7efb356613eafd5c89ea9f2be54d8ecf96ce0d28"),
			v: common.FromHex("01"),
		},
		{
			k: common.FromHex("fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae980000000000000001a8dc6a21510692d70d47860a1bbd432c801d1860bfbbe6856756ad4c062ba601"),
			v: common.FromHex("53706f7265000000000000000000000000000000000000000000000000000000"),
		},
		{
			k: common.FromHex("fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae980000000000000001dee260551c74e3b37ed31b6e5f482a3ff9342f863a5880c9090db0cc9e002750"),
			v: common.FromHex("5067247f2214dca445bfb213277b5f19711e309f"),
		},
		{
			k: common.FromHex("fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae980000000000000001fe59747b95e3ddbc3fd7e47a8bdf2465d2d88a030c9bd19cc3c0b7a9860c0d5f"),
			v: common.FromHex("01"),
		},
	}
	hb := NewHashBuilder(true)
	hexKeys := make([][]byte, len(keys))
	for i, key := range keys {
		hexKeys[i] = nibblizeKey(key.k, true)
	}
	cutoff := 2 * (length.Hash + common.IncarnationLength)
	last := hexKeys[len(hexKeys)-1]
	finalSucc := append(common.Copy(last[:cutoff-1]), last[cutoff-1]+1)

	genStructStepsOver(t, hb, retainNone, nil, hexKeys, func(i int) GenStructStepData {
		return &GenStructStepLeafData{rlp.RlpSerializableBytes(keys[i].v)}
	}, finalSucc)
	require.NotEmpty(t, hb.rootHash())
}

func TestAccountsOnly(t *testing.T) {
	keys := []struct {
		k []byte
		v []byte
	}{
		{k: common.FromHex("10002a312d2809e00f42a7f8cb0e659bddf0b4f201d24eb1b2946493cbae334c"), v: common.FromHex("01")},
		{k: common.FromHex("10002a412d2809e00f42a7f8cb0e659bddf0b4f201d24eb1b2946493cbae334c"), v: common.FromHex("01")},
		{k: common.FromHex("10002b412d2809e00f42a7f8cb0e659bddf0b4f201d24eb1b2946493cbae334c"), v: common.FromHex("01")},
		{k: common.FromHex("10009384e46519cbc71a22098063e5608768276f2dc212e71fd2c6c643c726c4"), v: common.FromHex("01")},
		{k: common.FromHex("10009484e46519cbc71a22098063e5608768276f2dc212e71fd2c6c643c726c4"), v: common.FromHex("01")},
		{k: common.FromHex("1000a9e493fff57a9c96dc0a7efb356613eafd5c89ea9f2be54d8ecf96ce0d28"), v: common.FromHex("01")},
		{k: common.FromHex("110006a1510692d70d47860a1bbd432c801d1860bfbbe6856756ad4c062ba601"), v: common.FromHex("01")},
		{k: common.FromHex("120006a1510692d70d47860a1bbd432c801d1860bfbbe6856756ad4c062ba601"), v: common.FromHex("01")},
		{k: common.FromHex("121006a1510692d70d47860a1bbd432c801d1860bfbbe6856756ad4c062ba601"), v: common.FromHex("01")},
		{k: common.FromHex("200c6a21510692d70d47860a1bbd432c801d1860bfbbe6856756ad4c062ba601"), v: common.FromHex("01")},
	}
	i := 0
	hc := expectingCollector(t, &i, []stepExpectation{
		{common.FromHex("0100000002"), 0b10000000000, 0b000},
		{common.FromHex("01000000"), 0b1000000100, 0b100},
		{common.FromHex("01"), 0b100, 0b001},
		{common.FromHex(""), 0b10, 0b10},
	})

	hb := NewHashBuilder(false)
	hexKeys := make([][]byte, len(keys))
	for i, key := range keys {
		hexKeys[i] = nibblizeKey(key.k, true)
	}
	genStructStepsOver(t, hb, retainNone, hc, hexKeys, func(i int) GenStructStepData {
		return &GenStructStepLeafData{rlp.RlpSerializableBytes(keys[i].v)}
	}, []byte{})
	require.Equal(t, 4, i)
}

func TestBranchesOnly(t *testing.T) {
	keys := []struct {
		k       []byte
		hasTree bool
	}{
		{k: common.FromHex("0100000002000a03"), hasTree: false},
		{k: common.FromHex("0100000002000a04"), hasTree: true},
		{k: common.FromHex("01000000020b"), hasTree: false},
		{k: common.FromHex("010000000900000103"), hasTree: false},
		{k: common.FromHex("010000000901"), hasTree: false},
		{k: common.FromHex("010000000a"), hasTree: false},
		{k: common.FromHex("0101"), hasTree: false},
		{k: common.FromHex("010200000a"), hasTree: false},
		{k: common.FromHex("010200000b"), hasTree: false},
		{k: common.FromHex("0201"), hasTree: false},
	}
	i := 0
	hc := expectingCollector(t, &i, []stepExpectation{
		{common.FromHex("0100000002000a"), 0b11000, 0b1000},
		{common.FromHex("0100000002"), 0b100000000000, 0b1},
		{common.FromHex("0100000009"), 0b10, 0b0},
		{common.FromHex("01000000"), 0b11000000100, 0b01000000100},
		{common.FromHex("01020000"), 0b110000000000, 0b0},
		{common.FromHex("01"), 0b10, 0b101},
		{common.FromHex(""), 0b10, 0b10},
	})

	hb := NewHashBuilder(false)
	// keys are already nibblized; the hash data attached to the step whose curr is keys[i]
	// carries the hasTree flag of the following key (the final step gets hasTree=false),
	// matching how the caller feeds GenStructStep in production.
	hexKeys := make([][]byte, len(keys))
	for i, key := range keys {
		hexKeys[i] = key.k
	}
	genStructStepsOver(t, hb, retainNone, hc, hexKeys, func(i int) GenStructStepData {
		hasTree := false
		if i+1 < len(keys) {
			hasTree = keys[i+1].hasTree
		}
		return &GenStructStepHashData{common.Hash{}, hasTree}
	}, []byte{})
	require.Equal(t, 7, i)
}

func TestStorageOnly(t *testing.T) {
	keys := []struct {
		k []byte
		v []byte
	}{
		{
			k: common.FromHex("500020e493fff57a9c96dc0a7efb356613eafd5c89ea9f2be54d8ecf96ce0d28"), v: common.FromHex("01"),
		},
		{
			k: common.FromHex("500021e493fff57a9c96dc0a7efb356613eafd5c89ea9f2be54d8ecf96ce0d28"), v: common.FromHex("01"),
		},
		{
			k: common.FromHex("500027e493fff57a9c96dc0a7efb356613eafd5c89ea9f2be54d8ecf96ce0d28"), v: common.FromHex("01"),
		},
		{
			k: common.FromHex("5000979e93fff57a9c96dc0a7efb356613eafd5c89ea9f2be54d8ecf96ce0d28"), v: common.FromHex("01"),
		},
		{
			k: common.FromHex("5000a7e493fff57a9c96dc0a7efb356613eafd5c89ea9f2be54d8ecf96ce0d28"), v: common.FromHex("01"),
		},
		{
			k: common.FromHex("600a79e493fff57a9c96dc0a7efb356613eafd5c89ea9f2be54d8ecf96ce0d28"), v: common.FromHex("01"),
		},
	}
	i := 0
	hc := expectingCollector(t, &i, []stepExpectation{
		{common.FromHex("05000000"), 0b100, 0b0},
	})

	hb := NewHashBuilder(false)
	hexKeys := make([][]byte, len(keys))
	for i, key := range keys {
		hexKeys[i] = nibblizeKey(key.k, len(key.k) == 32 || len(key.k) == 72)
	}
	genStructStepsOver(t, hb, retainNone, hc, hexKeys, func(i int) GenStructStepData {
		return &GenStructStepLeafData{rlp.RlpSerializableBytes(keys[i].v)}
	}, []byte{})
	require.Equal(t, 2, i)
}

func TestStorageWithoutBranchNodeInRoot(t *testing.T) {
	keys := []struct {
		k       []byte
		hasTree bool
	}{
		{
			k:       common.FromHex("500020"),
			hasTree: true,
		},
		{
			k:       common.FromHex("500021"),
			hasTree: false,
		},
		{
			k:       common.FromHex("500027"),
			hasTree: false,
		},
	}
	var i int
	hc := expectingCollector(t, &i, []stepExpectation{
		{common.FromHex("0500000002"), 0b10000011, 0b1},
		{common.FromHex(""), 0b0, 0b100000},
	})

	hb := NewHashBuilder(false)
	hexKeys := make([][]byte, len(keys))
	for i, key := range keys {
		hexKeys[i] = nibblizeKey(key.k, false)
	}
	genStructStepsOver(t, hb, retainNone, hc, hexKeys, func(i int) GenStructStepData {
		return &GenStructStepHashData{common.Hash{}, keys[i].hasTree}
	}, []byte{})
	require.Equal(t, 2, i)
}

// Test2 drives GenStructStep with hash data over keys without terminators, checking that
// every step succeeds.
func Test2(t *testing.T) {
	keys := []struct {
		k []byte
		v []byte
	}{
		{
			k: common.FromHex("000000"),
			v: common.FromHex("0100000000000000000000000000000000000000000000000000000000000000"),
		},
		{
			k: common.FromHex("000001"),
			v: common.FromHex("0100000000000000000000000000000000000000000000000000000000000000"),
		},
		{
			k: common.FromHex("000009"),
			v: common.FromHex("0100000000000000000000000000000000000000000000000000000000000000"),
		},
		{
			k: common.FromHex("01"),
			v: common.FromHex("0100000000000000000000000000000000000000000000000000000000000000"),
		},
		{
			k: common.FromHex("02"),
			v: common.FromHex("0100000000000000000000000000000000000000000000000000000000000000"),
		},
	}
	noopHc := func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		return nil
	}
	hb := NewHashBuilder(false)
	hexKeys := make([][]byte, len(keys))
	for i, key := range keys {
		hexKeys[i] = nibblizeKey(key.k, false)
	}
	genStructStepsOver(t, hb, retainNone, noopHc, hexKeys, func(i int) GenStructStepData {
		return &GenStructStepHashData{Hash: common.BytesToHash(keys[i].v)}
	}, nil)
}
