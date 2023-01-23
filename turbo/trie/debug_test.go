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

// Debugging utilities for Merkle Patricia trees

package trie

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

var debugTests = []struct {
	aHexKeys   []string
	aBalances  []uint64
	sHexKeys   []string
	sHexValues []string
}{
	{
		aHexKeys:   []string{"0x00000000"},
		aBalances:  []uint64{13},
		sHexKeys:   []string{},
		sHexValues: []string{},
	},
	{
		aHexKeys:   []string{"0x0000000000000000"},
		aBalances:  []uint64{13},
		sHexKeys:   []string{"0x00000000000000000100000000000001", "0x00000000000000000020000000000002"},
		sHexValues: []string{"0x01", "0x02"},
	},
	{
		aHexKeys:   []string{"0x0000000000000000", "0x000f000000000000"},
		aBalances:  []uint64{13, 567},
		sHexKeys:   []string{"0x00000000000000000100000000000001", "0x00000000000000000020000000000002"},
		sHexValues: []string{"0x01", "0x02"},
	},
}

func TestPrintLoad(t *testing.T) {
	trace := true
	for tn, debugTest := range debugTests {
		if trace {
			fmt.Printf("Test number %d\n", tn)
		}
		tr := New(libcommon.Hash{})
		for i, balance := range debugTest.aBalances {
			account := &accounts.Account{Initialised: true, Balance: *uint256.NewInt(balance), CodeHash: emptyState}
			tr.UpdateAccount(common.FromHex(debugTest.aHexKeys[i]), account)
		}
		for i, sHexKey := range debugTest.sHexKeys {
			tr.Update(common.FromHex(sHexKey), common.FromHex(debugTest.sHexValues[i]))
		}
		trieHash := tr.Hash()
		var b bytes.Buffer
		tr.Print(&b)
		if trace {
			fmt.Printf("Trie: %s\n", tr.root.fstring(""))
			fmt.Printf("Buffer: 0x%x\n", b.Bytes())
		}
		trLoaded, err := Load(&b)
		if err != nil {
			t.Errorf("could not load trie from the buffer: %v", err)
		}
		loadedHash := trLoaded.Hash()
		if trace {
			fmt.Printf("Loaded trie: %s\n", trLoaded.root.fstring(""))
		}
		if trieHash != loadedHash {
			t.Errorf("original trie hash %x != loaded trie hash %x", trieHash, loadedHash)
		}
	}
}
