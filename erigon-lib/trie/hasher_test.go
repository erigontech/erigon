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

	"github.com/erigontech/erigon-lib/common"
)

func TestValue(t *testing.T) {
	t.Skip("should be restored. skipped for Erigon")

	h := newHasher(false)
	var hn common.Hash
	h.hash(ValueNode("BLAH"), false, hn[:])
	expected := "0x0"
	actual := fmt.Sprintf("0x%x", hn[:])
	if actual != expected {
		t.Errorf("Expected %s, got %x", expected, actual)
	}
}
