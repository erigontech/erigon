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

	hb := NewHashBuilder()
	var prec, succ bytes.Buffer
	var curr OneBytesTape
	var valueTape OneBytesTape
	hb.SetKeyTape(&curr)
	hb.SetValueTape(&valueTape)
	var groups []uint16
	for i, key := range keys {
		prec.Reset()
		prec.Write(curr.Bytes())
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
			groups, err = GenStructStep(0, func(_ []byte) bool { return true }, false, prec.Bytes(), curr.Bytes(), succ.Bytes(), hb, groups)
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
	prec.Reset()
	prec.Write(curr.Bytes())
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	if _, err := GenStructStep(0, func(_ []byte) bool { return true }, false, prec.Bytes(), curr.Bytes(), succ.Bytes(), hb, groups); err != nil {
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

	hb := NewHashBuilder()
	var prec, succ bytes.Buffer
	var curr OneBytesTape
	var valueTape OneBytesTape
	hb.SetKeyTape(&curr)
	hb.SetValueTape(&valueTape)
	var groups []uint16
	for _, key := range keys {
		prec.Reset()
		prec.Write(curr.Bytes())
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
			groups, err = GenStructStep(0, rs.HashOnly, false, prec.Bytes(), curr.Bytes(), succ.Bytes(), hb, groups)
			if err != nil {
				t.Errorf("Could not execute step of structGen algorithm: %v", err)
			}
		}
		valueTape.Buffer.Reset()
		valueTape.Buffer.Write(value)
	}
	prec.Reset()
	prec.Write(curr.Bytes())
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	if _, err := GenStructStep(0, rs.HashOnly, false, prec.Bytes(), curr.Bytes(), succ.Bytes(), hb, groups); err != nil {
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
