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

// Pruning of the Merkle Patricia trees

package trie

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
)

func TestOnePerTimestamp(t *testing.T) {
	tp := NewTriePruning(0)
	tr := New(common.Hash{})
	tr.SetTouchFunc(func(hex []byte, del bool) {
		tp.Touch(hex, del)
	})
	var key [4]byte
	value := []byte("V")
	var timestamp uint64 = 0
	for n := uint32(0); n < uint32(100); n++ {
		tp.SetBlockNr(timestamp)
		binary.BigEndian.PutUint32(key[:], n)
		tr.Update(key[:], value, timestamp) // Each key is added within a new generation
		timestamp++
	}
	for n := uint32(50); n < uint32(60); n++ {
		tp.SetBlockNr(timestamp)
		binary.BigEndian.PutUint32(key[:], n)
		tr.Delete(key[:], timestamp) // Each key is added within a new generation
		timestamp++
	}
	for n := uint32(30); n < uint32(59); n++ {
		tp.SetBlockNr(timestamp)
		binary.BigEndian.PutUint32(key[:], n)
		tr.Get(key[:]) // Each key is added within a new generation
		timestamp++
	}
	prunableNodes := tr.CountPrunableNodes()
	fmt.Printf("Actual prunable nodes: %d, accounted: %d\n", prunableNodes, tp.NodeCount())
	if b := tp.PruneTo(tr, 4); !b {
		t.Fatal("Not pruned")
	}
	prunableNodes = tr.CountPrunableNodes()
	fmt.Printf("Actual prunable nodes: %d, accounted: %d\n", prunableNodes, tp.NodeCount())
}
