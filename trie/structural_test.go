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
	"math/big"
	"sort"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
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
			tr.Update([]byte(key), valueNode(valueLong), 0)
		} else {
			tr.Update([]byte(key), valueNode(valueShort), 0)
		}
	}
	trieHash := tr.Hash()

	hb := NewHashBuilder(false)
	var succ bytes.Buffer
	var curr OneBytesTape
	var valueTape OneBytesTape
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
			groups, err = GenStructStep(func(_ []byte) bool { return true }, curr.Bytes(), succ.Bytes(), hb, GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueTape.Bytes())}, groups)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
		valueTape.Buffer.Reset()
		if i%2 == 0 {
			valueTape.Buffer.Write(valueLong)
		} else {
			valueTape.Buffer.Write(valueShort)
		}
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	if _, err := GenStructStep(func(_ []byte) bool { return true }, curr.Bytes(), succ.Bytes(), hb, GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueTape.Bytes())}, groups); err != nil {
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
		tr.Update([]byte(key), valueNode(value), 0)
	}
	trieHash := tr.Hash()

	// Choose some keys to be resolved
	var rs ResolveSet
	// First, existing keys
	for i := 0; i < 1000; i += 200 {
		rs.AddKey([]byte(keys[i]))
	}
	// Next, some non-exsiting keys
	for i := 0; i < 1000; i++ {
		rs.AddKey(crypto.Keccak256([]byte(keys[i]))[:8])
	}

	hb := NewHashBuilder(false)
	var succ bytes.Buffer
	var curr OneBytesTape
	var valueTape OneBytesTape
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
			groups, err = GenStructStep(rs.HashOnly, curr.Bytes(), succ.Bytes(), hb, GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueTape.Bytes())}, groups)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
		valueTape.Buffer.Reset()
		valueTape.Buffer.Write(value)
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	if _, err := GenStructStep(rs.HashOnly, curr.Bytes(), succ.Bytes(), hb, GenStructStepLeafData{rlphacks.RlpSerializableBytes(valueTape.Bytes())}, groups); err != nil {
		t.Errorf("Could not execute step of structGen algorithm: %v", err)
	}
	tr1 := New(common.Hash{})
	tr1.root = hb.root()
	builtHash := hb.rootHash()
	if trieHash != builtHash {
		t.Errorf("Expected hash %x, got %x", trieHash, builtHash)
	}
	// Check the availability of the resolved keys
	for _, hex := range rs.hexes {
		key := hexToKeybytes(hex)
		_, found := tr1.Get(key)
		if !found {
			t.Errorf("Key %x was not resolved", hex)
		}
	}
}

var streamTests = []struct {
	aHexKeys          []string
	aBalances         []int64
	sHexKeys          []string
	sHexValues        []string
	rsHex             []string
	hexesExpected     []string
	aBalancesExpected []int64
	sValuesExpected   []string
	hashesExpected    []string
}{
	{
		aHexKeys:          []string{"0x00000000"},
		aBalances:         []int64{13},
		sHexKeys:          []string{},
		sHexValues:        []string{},
		rsHex:             []string{},
		hexesExpected:     []string{"0x000000000000000010"},
		aBalancesExpected: []int64{13},
		sValuesExpected:   []string{},
		hashesExpected:    []string{},
	},
	{
		aHexKeys:          []string{"0x0000000000000000"},
		aBalances:         []int64{13},
		sHexKeys:          []string{"0x00000000000000000100000000000001", "0x00000000000000000020000000000002"},
		sHexValues:        []string{"0x01", "0x02"},
		rsHex:             []string{},
		hexesExpected:     []string{"0x0000000000000000000000000000000010"},
		aBalancesExpected: []int64{13},
		sValuesExpected:   []string{},
		hashesExpected:    []string{},
	},
	{
		aHexKeys:          []string{"0x0000000000000000", "0x000f000000000000"},
		aBalances:         []int64{13, 567},
		sHexKeys:          []string{"0x00000000000000000100000000000001", "0x00000000000000000020000000000002"},
		sHexValues:        []string{"0x01", "0x02"},
		rsHex:             []string{"0x0000000000000000", "0x000f000000000000"},
		hexesExpected:     []string{"0x0000000000000000000000000000000010", "0000000f00000000000000000000000010"},
		aBalancesExpected: []int64{13, 567},
		sValuesExpected:   []string{},
		hashesExpected:    []string{},
	},
}

func TestToStream(t *testing.T) {
	trace := true
	for tn, streamTest := range streamTests {
		if trace {
			fmt.Printf("Test number %d\n", tn)
		}
		tr := New(common.Hash{})
		for i, balance := range streamTest.aBalances {
			account := &accounts.Account{Initialised: true, Balance: *big.NewInt(balance), CodeHash: emptyState}
			tr.UpdateAccount(common.FromHex(streamTest.aHexKeys[i]), account)
		}
		for i, sHexKey := range streamTest.sHexKeys {
			tr.Update(common.FromHex(sHexKey), common.FromHex(streamTest.sHexValues[i]), 0)
		}
		// Important to do the hash calculation here, so that the account nodes are updated
		// with the correct storage root values
		trieHash := tr.Hash()
		rs := NewResolveSet(0)
		for _, rsItem := range streamTest.rsHex {
			rs.AddKey(common.FromHex(rsItem))
		}
		s := ToStream(tr, rs, trace)
		if len(s.hexes) != len(streamTest.hexesExpected) {
			t.Errorf("length of hexes is %d, expected %d", len(s.hexes), len(streamTest.hexesExpected))
		}
		for i, hex := range s.hexes {
			if i < len(streamTest.hexesExpected) {
				hexExpected := common.FromHex(streamTest.hexesExpected[i])
				if !bytes.Equal(hex, hexExpected) {
					t.Errorf("hex[%d] = %x, expected %x", i, hex, hexExpected)
				}
			}
		}
		if len(s.aValues) != len(streamTest.aBalancesExpected) {
			t.Errorf("length of aValues is %d, expected %d", len(s.aValues), len(streamTest.aBalancesExpected))
		}
		for i, aValue := range s.aValues {
			if i < len(streamTest.aBalancesExpected) {
				balanceExpected := streamTest.aBalancesExpected[i]
				if aValue.Balance.Int64() != balanceExpected {
					t.Errorf("balance[%d] = %d, expected %d", i, aValue.Balance.Int64(), balanceExpected)
				}
			}
		}
		if len(s.sValues) != len(streamTest.sValuesExpected) {
			t.Errorf("length of sValues is %d, expected %d", len(s.sValues), len(streamTest.sValuesExpected))
		}
		for i, sValue := range s.sValues {
			if i < len(streamTest.sValuesExpected) {
				sValueExpected := common.FromHex(streamTest.sValuesExpected[i])
				if !bytes.Equal(sValue, sValueExpected) {
					t.Errorf("sValue[%d] = %x, expected %x", i, sValue, sValueExpected)
				}
			}
		}
		if len(s.hashes) != len(streamTest.hashesExpected) {
			t.Errorf("length of hashes is %d, expected %d", len(s.hashes), len(streamTest.hashesExpected))
		}
		for i, hash := range s.hashes {
			if i < len(streamTest.hashesExpected) {
				hashExpected := common.HexToHash(streamTest.hashesExpected[i])
				if hash != hashExpected {
					t.Errorf("hash[%d] = %x, expected %x", i, hash, hashExpected)
				}
			}
		}
		// Check that the hash of the stream is equal to the hash of the trie
		streamHash, err := StreamHash(s, 8, trace)
		if trace {
			fmt.Printf("want:\n%s\n", tr.root.fstring(""))
		}
		if err != nil {
			t.Errorf("unable to compute hash of the stream: %v", err)
		}
		if streamHash != trieHash {
			t.Errorf("stream hash %x != trie hash %x", streamHash, trieHash)
		}
	}
}
