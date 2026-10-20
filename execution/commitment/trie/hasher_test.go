// Copyright 2024 The Erigon Authors
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
	"fmt"
	"testing"

	"github.com/erigontech/erigon/common"
)

func TestValue(t *testing.T) {
	h := newHasher(false)
	var hn common.Hash
	// A short value node is stored inline as its RLP encoding, not hashed.
	n, err := h.hash(ValueNode("BLAH"), false, hn[:])
	if err != nil {
		t.Fatal(err)
	}
	expected := "8584424c4148"
	if actual := fmt.Sprintf("%x", hn[:n]); actual != expected {
		t.Errorf("expected %s, got %s", expected, actual)
	}
}
