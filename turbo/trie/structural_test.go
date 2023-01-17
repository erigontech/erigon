// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty off
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

// Experimental code for separating data and structural information

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/turbo/rlphacks"
)

func TestV2HashBuilding(t *testing.T) {
	var keys []string
	for b := uint32(0); b < 100000; b++ {
		var preimage [4]byte
		binary.BigEndian.PutUint32(preimage[:], b)
		key := crypto.Keccak256(preimage[:])[:8]
		keys = append(keys, string(key))
	}
	slices.Sort(keys)
	for i, key := range keys {
		if i > 0 && keys[i-1] == key {
			fmt.Printf("Duplicate!\n")
		}
	}
	tr := New(libcommon.Hash{})
	valueLong := []byte("VALUE123985903485903489043859043859043859048590485904385903485940385439058934058439058439058439058940385904358904385438809348908345")
	valueShort := []byte("VAL")
	for i, key := range keys {
		if i%2 == 0 {
			tr.Update([]byte(key), valueLong)
		} else {
			tr.Update([]byte(key), valueShort)
		}
	}
	trieHash := tr.Hash()

	hb := NewHashBuilder(false)
	var succ bytes.Buffer
	var curr bytes.Buffer
	var valueTape bytes.Buffer
	var groups, hasTree, hasHash []uint16
	for i, key := range keys {
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()
		keyBytes := []byte(key)
		for _, b := range keyBytes {
			succ.WriteByte(b / 16)
			succ.WriteByte(b % 16)
		}
		succ.WriteByte(16)
		if curr.Len() > 0 {
			var err error
			groups, hasTree, hasHash, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueTape.Bytes())}, groups, hasTree, hasHash, false)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
		valueTape.Reset()
		if i%2 == 0 {
			valueTape.Write(valueLong)
		} else {
			valueTape.Write(valueShort)
		}
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	if _, _, _, err := GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueTape.Bytes())}, groups, hasTree, hasHash, false); err != nil {
		t.Errorf("Could not execute step of structGen algorithm: %v", err)
	}
	builtHash := hb.rootHash()
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
}

func TestV2Resolution(t *testing.T) {
	var keys []string
	for b := uint32(0); b < 100000; b++ {
		var preimage [4]byte
		binary.BigEndian.PutUint32(preimage[:], b)
		key := crypto.Keccak256(preimage[:])[:8]
		keys = append(keys, string(key))
	}
	slices.Sort(keys)
	tr := New(libcommon.Hash{})
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
	var succ bytes.Buffer
	var curr bytes.Buffer
	var valueTape bytes.Buffer
	var groups, hasTree, hasHash []uint16
	for _, key := range keys {
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()
		keyBytes := []byte(key)
		for _, b := range keyBytes {
			succ.WriteByte(b / 16)
			succ.WriteByte(b % 16)
		}
		succ.WriteByte(16)
		if curr.Len() > 0 {
			var err error
			groups, hasTree, hasHash, err = GenStructStep(rl.Retain, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueTape.Bytes())}, groups, hasTree, hasHash, false)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
		valueTape.Reset()
		valueTape.Write(value)
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	if _, _, _, err := GenStructStep(rl.Retain, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueTape.Bytes())}, groups, hasTree, hasHash, false); err != nil {
		t.Errorf("Could not execute step of structGen algorithm: %v", err)
	}
	tr1 := New(libcommon.Hash{})
	tr1.root = hb.root()
	builtHash := hb.rootHash()
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
	// Check the availability of the resolved keys
	for _, hex := range rl.hexes {
		key := hexToKeybytes(hex)
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
	var accountAddress = libcommon.Address{3, 4, 5, 6}
	addrHash := crypto.Keccak256(accountAddress[:])
	incarnation := make([]byte, 8)
	binary.BigEndian.PutUint64(incarnation, uint64(2))
	var location1 = libcommon.Hash{1}
	locationKey1 := append(append([]byte{}, addrHash...), crypto.Keccak256(location1[:])...)
	var location2 = libcommon.Hash{2}
	locationKey2 := append(append([]byte{}, addrHash...), crypto.Keccak256(location2[:])...)
	var location3 = libcommon.Hash{3}
	locationKey3 := append(append([]byte{}, addrHash...), crypto.Keccak256(location3[:])...)
	var keys = []string{string(locationKey1), string(locationKey2), string(locationKey3)}
	slices.Sort(keys)
	tr := New(libcommon.Hash{})
	valueShort := []byte("VAL")
	for _, key := range keys {
		tr.Update([]byte(key)[length.Hash:], valueShort)
	}
	trieHash := tr.Hash()

	hb := NewHashBuilder(true)
	var succ bytes.Buffer
	var curr bytes.Buffer
	var groups, hasTree, hasHash []uint16
	var err error
	for _, key := range keys {
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()
		keyBytes := []byte(key)
		for _, b := range keyBytes {
			succ.WriteByte(b / 16)
			succ.WriteByte(b % 16)
		}
		succ.WriteByte(16)
		if curr.Len() > 0 {
			groups, hasTree, hasHash, err = GenStructStep(func(_ []byte) bool { return true }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueShort)}, groups, hasTree, hasHash, false)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	// Produce the key which is specially modified version of `curr` (only different in the last nibble)
	cutoff := 2 * length.Hash
	succ.Write(curr.Bytes()[:cutoff-1])
	succ.WriteByte(curr.Bytes()[cutoff-1] + 1)
	if _, _, _, err = GenStructStep(func(_ []byte) bool { return true }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueShort)}, groups, hasTree, hasHash, false); err != nil {
		t.Errorf("Could not execute step of structGen algorithm: %v", err)
	}
	builtHash := hb.rootHash()
	if trieHash != builtHash {
		fmt.Printf("Trie built: %s\n", hb.root().fstring(""))
		fmt.Printf("Trie expected: %s\n", tr.root.fstring(""))
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
	fmt.Printf("groups: %d\n", len(groups))
}

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
	var succ bytes.Buffer
	var curr bytes.Buffer
	var groups, hasTree, hasHash []uint16
	var err error
	for _, key := range keys {
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()
		keyBytes := key.k
		for _, b := range keyBytes {
			succ.WriteByte(b / 16)
			succ.WriteByte(b % 16)
		}
		succ.WriteByte(16)
		if curr.Len() > 0 {
			groups, hasTree, hasHash, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(key.v)}, groups, hasTree, hasHash, false)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	// Produce the key which is specially modified version of `curr` (only different in the last nibble)
	cutoff := 2 * (length.Hash + common.IncarnationLength)
	succ.Write(curr.Bytes()[:cutoff-1])
	succ.WriteByte(curr.Bytes()[cutoff-1] + 1)
	if _, _, _, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(keys[len(keys)-1].v)}, groups, hasTree, hasHash, false); err != nil {
		t.Errorf("Could not execute step of structGen algorithm: %v", err)
	}
	builtHash := hb.rootHash()
	fmt.Printf("%d, %x, %d, %x\n", cutoff, builtHash, len(hb.hashStack), hb.hashStack)
	//if trieHash != builtHash {
	//	fmt.Printf("Trie built: %s\n", hb.root().fstring(""))
	//	fmt.Printf("Trie expected: %s\n", tr.root.fstring(""))
	//	t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	//}
	fmt.Printf("groups: %d\n", len(groups))
}

func TestAccountsOnly(t *testing.T) {
	keys := []struct {
		k []byte
		v []byte
	}{
		{k: common.FromHex("10002a312d2809e00f42a7f8cb0e659bddf0b4f201d24eb1b2946493cbae334c"), v: common.FromHex("01")},
		{k: common.FromHex("10002a412d2809e00f42a7f8cb0e659bddf0b4f201d24eb1b2946493cbae334c"), v: common.FromHex("01")},
		{k: common.FromHex("10002b412d2809e00f42a7f8cb0e659bddf0b4f201d24eb1b2946493cbae334c"), v: common.FromHex("01")},
		{k: common.FromHex("10009384w46519cbc71a22098063e5608768276f2dc212e71fd2c6c643c726c4"), v: common.FromHex("01")},
		{k: common.FromHex("10009484w46519cbc71a22098063e5608768276f2dc212e71fd2c6c643c726c4"), v: common.FromHex("01")},
		{k: common.FromHex("1000a9e493fff57a9c96dc0a7efb356613eafd5c89ea9f2be54d8ecf96ce0d28"), v: common.FromHex("01")},
		{k: common.FromHex("110006a1510692d70d47860a1bbd432c801d1860bfbbe6856756ad4c062ba601"), v: common.FromHex("01")},
		{k: common.FromHex("120006a1510692d70d47860a1bbd432c801d1860bfbbe6856756ad4c062ba601"), v: common.FromHex("01")},
		{k: common.FromHex("121006a1510692d70d47860a1bbd432c801d1860bfbbe6856756ad4c062ba601"), v: common.FromHex("01")},
		{k: common.FromHex("200c6a21510692d70d47860a1bbd432c801d1860bfbbe6856756ad4c062ba601"), v: common.FromHex("01")},
	}
	hb := NewHashBuilder(false)
	var succ bytes.Buffer
	var curr bytes.Buffer
	var groups, hasTree, hasHash []uint16
	var err error
	i := 0
	hc := func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		if hasHash == 0 && hasTree == 0 {
			return nil
		}
		i++
		switch i {
		case 1:
			require.Equal(t, common.FromHex("0100000002"), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b10000000000)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b000)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 2:
			require.Equal(t, common.FromHex("01000000"), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b1000000100)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b100)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 3:
			require.Equal(t, common.FromHex("01"), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b100)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b001)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 4:
			require.Equal(t, common.FromHex(""), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b10)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b10)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		}

		return nil
	}

	for _, key := range keys {
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()
		keyBytes := key.k
		for _, b := range keyBytes {
			succ.WriteByte(b / 16)
			succ.WriteByte(b % 16)
		}
		succ.WriteByte(16)
		if curr.Len() > 0 {
			groups, hasTree, hasHash, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, hc /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(key.v)}, groups, hasTree, hasHash, false)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	// Produce the key which is specially modified version of `curr` (only different in the last nibble)
	if _, _, _, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), []byte{}, hb, hc /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(keys[len(keys)-1].v)}, groups, hasTree, hasHash, false); err != nil {
		t.Errorf("Could not execute step of structGen algorithm: %v", err)
	}
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
		//{k: common.FromHex("010000000900000104"), hasTree: false},
		//{k: common.FromHex("010000000900000203"), hasTree: false},
		//{k: common.FromHex("010000000900000204"), hasTree: false},
		{k: common.FromHex("010000000901"), hasTree: false},
		{k: common.FromHex("010000000a"), hasTree: false},
		{k: common.FromHex("0101"), hasTree: false},
		{k: common.FromHex("010200000a"), hasTree: false},
		{k: common.FromHex("010200000b"), hasTree: false},
		{k: common.FromHex("0201"), hasTree: false},
	}
	hb := NewHashBuilder(false)
	var succ, curr bytes.Buffer
	var groups, hasTree, hasHash []uint16
	var err error
	i := 0
	hc := func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		if hasHash == 0 && hasTree == 0 {
			return nil
		}
		i++
		switch i {
		case 1:
			require.Equal(t, common.FromHex("0100000002000a"), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b11000)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b1000)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 2:
			require.Equal(t, common.FromHex("0100000002"), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b100000000000)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b1)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 3:
			require.Equal(t, common.FromHex("0100000009"), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b10)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b0)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 4:
			require.Equal(t, common.FromHex("01000000"), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b11000000100)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b01000000100)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 5:
			require.Equal(t, common.FromHex("01020000"), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b110000000000)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b0)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 6:
			require.Equal(t, common.FromHex("01"), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b10)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b101)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 7:
			require.Equal(t, common.FromHex(""), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b10)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b10)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		}

		return nil
	}

	for _, key := range keys {
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()
		succ.Write(key.k)
		if curr.Len() > 0 {
			groups, hasTree, hasHash, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, hc /* hashCollector */, &GenStructStepHashData{libcommon.Hash{}, key.hasTree}, groups, hasTree, hasHash, false)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	// Produce the key which is specially modified version of `curr` (only different in the last nibble)
	if _, _, _, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), []byte{}, hb, hc /* hashCollector */, &GenStructStepHashData{libcommon.Hash{}, false}, groups, hasTree, hasHash, false); err != nil {
		t.Errorf("Could not execute step of structGen algorithm: %v", err)
	}
	require.Equal(t, 7, i)
}

func TestStorageOnly(t *testing.T) {
	//acc := common.FromHex("fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae980000000000000001")
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
	hb := NewHashBuilder(false)
	var succ bytes.Buffer
	var curr bytes.Buffer
	var groups, hasHash, hasTree []uint16
	var err error
	i := 0
	hc := func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		if hasHash == 0 && hasTree == 0 {
			return nil
		}
		i++
		switch i {
		case 1:
			require.Equal(t, common.FromHex("05000000"), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b100)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b0)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 3:
			require.Equal(t, common.FromHex("05000000"), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b100)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b000)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 4:
			require.Equal(t, common.FromHex(""), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b0)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b100000)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 5:
			require.NoError(t, fmt.Errorf("not expected"))
		}

		return nil
	}

	for _, key := range keys {
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()
		keyBytes := key.k
		for _, b := range keyBytes {
			succ.WriteByte(b / 16)
			succ.WriteByte(b % 16)
		}
		if len(key.k) == 32 || len(key.k) == 72 {
			succ.WriteByte(16)
		}
		if curr.Len() > 0 {
			groups, hasTree, hasHash, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, hc /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(key.v)}, groups, hasTree, hasHash, false)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()

	// Produce the key which is specially modified version of `curr` (only different in the last nibble)
	if _, _, _, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, hc /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(keys[len(keys)-1].v)}, groups, hasTree, hasHash, false); err != nil {
		t.Errorf("Could not execute step of structGen algorithm: %v", err)
	}
	require.Equal(t, 2, i)
}

func TestStorageWithoutBranchNodeInRoot(t *testing.T) {
	trace := false
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
	hc := func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		if hasHash == 0 && hasTree == 0 {
			return nil
		}
		i++
		switch i {
		case 1:
			require.Equal(t, common.FromHex("0500000002"), keyHex)
			//require.Equal(t, fmt.Sprintf("%b", uint16(0b10000011)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b10000011)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b1)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		case 2:
			require.Equal(t, common.FromHex(""), keyHex)
			require.Equal(t, fmt.Sprintf("%b", uint16(0b0)), fmt.Sprintf("%b", hasHash))
			require.Equal(t, fmt.Sprintf("%b", uint16(0b100000)), fmt.Sprintf("%b", hasTree))
			require.NotNil(t, hashes)
		}

		return nil
	}
	hb := NewHashBuilder(false)
	var curr, succ bytes.Buffer
	var currhasTree, succhasTree bool
	var groups, hasTree, hasHash []uint16
	var err error

	for _, key := range keys {
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()
		for _, b := range key.k {
			succ.WriteByte(b / 16)
			succ.WriteByte(b % 16)
		}
		currhasTree = succhasTree
		succhasTree = key.hasTree
		if curr.Len() > 0 {
			v := &GenStructStepHashData{libcommon.Hash{}, currhasTree}
			groups, hasTree, hasHash, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, hc /* hashCollector */, v, groups, hasTree, hasHash, trace)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	currhasTree = succhasTree
	v := &GenStructStepHashData{libcommon.Hash{}, currhasTree}
	// Produce the key which is specially modified version of `curr` (only different in the last nibble)
	if _, _, _, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), []byte{}, hb, hc /* hashCollector */, v, groups, hasTree, hasHash, trace); err != nil {
		t.Errorf("Could not execute step of structGen algorithm: %v", err)
	}
	require.Equal(t, 2, i)
}

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
		//{
		//	k: common.FromHex("000010"),
		//	v: common.FromHex("0100000000000000000000000000000000000000000000000000000000000000"),
		//},
		//{
		//	k: common.FromHex("000020"),
		//	v: common.FromHex("0100000000000000000000000000000000000000000000000000000000000000"),
		//},
		{
			k: common.FromHex("01"),
			v: common.FromHex("0100000000000000000000000000000000000000000000000000000000000000"),
		},
		{
			k: common.FromHex("02"),
			v: common.FromHex("0100000000000000000000000000000000000000000000000000000000000000"),
		},
	}
	hb := NewHashBuilder(false)
	var succ bytes.Buffer
	var curr bytes.Buffer
	var groups, hasTree, hasHash []uint16
	var err error
	for _, key := range keys {
		curr.Reset()
		curr.Write(succ.Bytes())
		succ.Reset()
		keyBytes := key.k
		for _, b := range keyBytes {
			succ.WriteByte(b / 16)
			succ.WriteByte(b % 16)
		}
		//succ.WriteByte(16)
		if curr.Len() > 0 {
			fmt.Printf("send: %x\n", succ.Bytes())
			groups, hasTree, hasHash, err = GenStructStep(func(_ []byte) bool { return false },
				curr.Bytes(), succ.Bytes(), hb,
				func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
					return nil
				}, /* hashCollector */ &GenStructStepHashData{Hash: libcommon.BytesToHash(key.v)}, groups, hasTree, hasHash, false)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	// Produce the key which is specially modified version of `curr` (only different in the last nibble)
	if _, _, _, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		return nil
	}, /* hashCollector */ &GenStructStepHashData{Hash: libcommon.BytesToHash(keys[len(keys)-1].v)}, groups, hasTree, hasHash, false); err != nil {
		t.Errorf("Could not execute step of structGen algorithm: %v", err)
	}
}
