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
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

func TestHashWithModificationsEmpty(t *testing.T) {
	tr := New(common.Hash{})
	// Populate the trie
	// Build the root
	var stream Stream
	var hb HashBuilder
	rootHash, err := HashWithModifications(
		tr,
		common.Hashes{}, []*accounts.Account{},
		common.StorageKeys{}, [][]byte{},
		8,
		&stream, // Streams that will be reused for old and new stream
		&hb,     // HashBuilder will be reused
		false,
	)
	if err != nil {
		t.Errorf("Could not compute hash with modification: %v", err)
	}
	if rootHash != EmptyRoot {
		t.Errorf("Expected empty root, got: %x", rootHash)
	}
}
