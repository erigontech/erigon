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
	"sort"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/turbo/rlphacks"
)

func TestV2HashBuilding(t *testing.T) {
	var keys []string
	for b := uint32(0); b < 100000; b++ {
		var preimage [4]byte
		binary.BigEndian.PutUint32(preimage[:], b)
		key := crypto.Keccak256(preimage[:])[:8]
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	for i, key := range keys {
		if i > 0 && keys[i-1] == key {
			fmt.Printf("Duplicate!\n")
		}
	}
	tr := New(common.Hash{})
	valueLong := []byte("VALUE123985903485903489043859043859043859048590485904385903485940385439058934058439058439058439058940385904358904385438809348908345")
	valueShort := []byte("VAL")
	for i, key := range keys {
		if i%2 == 0 {
			tr.Update([]byte(key), valueNode(valueLong))
		} else {
			tr.Update([]byte(key), valueNode(valueShort))
		}
	}
	trieHash := tr.Hash()

	hb := NewHashBuilder(false)
	var succ bytes.Buffer
	var curr bytes.Buffer
	var valueTape bytes.Buffer
	var groups []uint16
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
			groups, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueTape.Bytes())}, groups, false)
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
	if _, err := GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueTape.Bytes())}, groups, false); err != nil {
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
	sort.Strings(keys)
	tr := New(common.Hash{})
	value := []byte("VALUE123985903485903489043859043859043859048590485904385903485940385439058934058439058439058439058940385904358904385438809348908345")
	for _, key := range keys {
		tr.Update([]byte(key), valueNode(value))
	}
	trieHash := tr.Hash()

	// Choose some keys to be resolved
	var rl RetainList
	// First, existing keys
	for i := 0; i < 1000; i += 200 {
		rl.AddKey([]byte(keys[i]))
	}
	// Next, some non-exsiting keys
	for i := 0; i < 1000; i++ {
		rl.AddKey(crypto.Keccak256([]byte(keys[i]))[:8])
	}

	hb := NewHashBuilder(false)
	var succ bytes.Buffer
	var curr bytes.Buffer
	var valueTape bytes.Buffer
	var groups []uint16
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
			groups, err = GenStructStep(rl.Retain, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueTape.Bytes())}, groups, false)
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
	if _, err := GenStructStep(rl.Retain, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueTape.Bytes())}, groups, false); err != nil {
		t.Errorf("Could not execute step of structGen algorithm: %v", err)
	}
	tr1 := New(common.Hash{})
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
// items, we would have entension node which branches off at some point, but includes incarnation encoding
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
	incarnation := make([]byte, 8)
	binary.BigEndian.PutUint64(incarnation, uint64(2))
	var location1 = common.Hash{1}
	locationKey1 := append(append([]byte{}, addrHash...), crypto.Keccak256(location1[:])...)
	var location2 = common.Hash{2}
	locationKey2 := append(append([]byte{}, addrHash...), crypto.Keccak256(location2[:])...)
	var location3 = common.Hash{3}
	locationKey3 := append(append([]byte{}, addrHash...), crypto.Keccak256(location3[:])...)
	var keys = []string{string(locationKey1), string(locationKey2), string(locationKey3)}
	sort.Strings(keys)
	tr := New(common.Hash{})
	valueShort := []byte("VAL")
	for _, key := range keys {
		tr.Update([]byte(key)[common.HashLength:], valueNode(valueShort))
	}
	trieHash := tr.Hash()

	hb := NewHashBuilder(true)
	var succ bytes.Buffer
	var curr bytes.Buffer
	var groups []uint16
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
			groups, err = GenStructStep(func(_ []byte) bool { return true }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueShort)}, groups, false)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	// Produce the key which is specially modified version of `curr` (only different in the last nibble)
	cutoff := 2 * common.HashLength
	succ.Write(curr.Bytes()[:cutoff-1])
	succ.WriteByte(curr.Bytes()[cutoff-1] + 1)
	if groups, err = GenStructStep(func(_ []byte) bool { return true }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueShort)}, groups, false); err != nil {
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
	var groups []uint16
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
			groups, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(key.v)}, groups, false)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	// Produce the key which is specially modified version of `curr` (only different in the last nibble)
	cutoff := 2 * (common.HashLength + common.IncarnationLength)
	succ.Write(curr.Bytes()[:cutoff-1])
	succ.WriteByte(curr.Bytes()[cutoff-1] + 1)
	if groups, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(keys[len(keys)-1].v)}, groups, false); err != nil {
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

func TestStorageOnly(t *testing.T) {
	// 0f0f0f090c010a0a050808040f010103000300010f06000f09080401090b090d040201070b0c040a0b06050a020907060b04010e090a00000b0b0c0e0a0e090800000000000000000000000000000001,53754832dfdb81e5ca770a3f13a6545ad8ac58a7d1fbbc8848eb785ac02c6876

	//fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae9800000000000000010d2f4a412d2809e00f42a7f8cb0e659bddf0b4f201d24eb1b2946493cbae334c,496e7374616e6365000000000000000000000000000000000000000000000000
	//fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae98000000000000000123a5384746519cbca71a22098063e5608768276f2dc212e71fd2c6c643c726c4,65eea643e9a9d6f5f2f7e13ccdff36cf45b46aab
	//fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae980000000000000001387a79e493fff57a9c96dc0a7efb356613eafd5c89ea9f2be54d8ecf96ce0d28,01
	//fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae980000000000000001a8dc6a21510692d70d47860a1bbd432c801d1860bfbbe6856756ad4c062ba601,53706f7265000000000000000000000000000000000000000000000000000000
	//fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae980000000000000001dee260551c74e3b37ed31b6e5f482a3ff9342f863a5880c9090db0cc9e002750,5067247f2214dca445bfb213277b5f19711e309f
	//fff9c1aa5884f1130301f60f98419b9d4217bc4ab65a2976b41e9a00bbceae980000000000000001fe59747b95e3ddbc3fd7e47a8bdf2465d2d88a030c9bd19cc3c0b7a9860c0d5f,01
	keys := []struct {
		k []byte
		v []byte
	}{
		{
			k: common.FromHex("0d2f4a412d2809e00f42a7f8cb0e659bddf0b4f201d24eb1b2946493cbae334c"),
			v: common.FromHex("496e7374616e6365000000000000000000000000000000000000000000000000"),
		},
		{
			k: common.FromHex("23a5384746519cbca71a22098063e5608768276f2dc212e71fd2c6c643c726c4"),
			v: common.FromHex("65eea643e9a9d6f5f2f7e13ccdff36cf45b46aab"),
		},
		{
			k: common.FromHex("387a79e493fff57a9c96dc0a7efb356613eafd5c89ea9f2be54d8ecf96ce0d28"),
			v: common.FromHex("01"),
		},
		{
			k: common.FromHex("a8dc6a21510692d70d47860a1bbd432c801d1860bfbbe6856756ad4c062ba601"),
			v: common.FromHex("53706f7265000000000000000000000000000000000000000000000000000000"),
		},
		{
			k: common.FromHex("dee260551c74e3b37ed31b6e5f482a3ff9342f863a5880c9090db0cc9e002750"),
			v: common.FromHex("5067247f2214dca445bfb213277b5f19711e309f"),
		},
		{
			k: common.FromHex("fe59747b95e3ddbc3fd7e47a8bdf2465d2d88a030c9bd19cc3c0b7a9860c0d5f"),
			v: common.FromHex("01"),
		},
	}
	hb := NewHashBuilder(true)
	var succ bytes.Buffer
	var curr bytes.Buffer
	var groups []uint16
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
			groups, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(key.v)}, groups, false)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	// Produce the key which is specially modified version of `curr` (only different in the last nibble)
	if groups, err = GenStructStep(func(_ []byte) bool { return false }, curr.Bytes(), []byte{}, hb, nil /* hashCollector */, &GenStructStepLeafData{rlphacks.RlpSerializableBytes(keys[len(keys)-1].v)}, groups, false); err != nil {
		t.Errorf("Could not execute step of structGen algorithm: %v", err)
	}
	builtHash := hb.rootHash()
	fmt.Printf("%x, %d, %x\n", builtHash, len(hb.hashStack), hb.hashStack)
	//if trieHash != builtHash {
	//	fmt.Printf("Trie built: %s\n", hb.root().fstring(""))
	//	fmt.Printf("Trie expected: %s\n", tr.root.fstring(""))
	//	t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	//}
	fmt.Printf("groups: %d\n", len(groups))
}
