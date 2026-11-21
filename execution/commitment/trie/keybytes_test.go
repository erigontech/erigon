// Copyright 2014 The go-ethereum Authors
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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common"
)

func TestKeybytesToCompact(t *testing.T) {
	keybytes := Keybytes{common.FromHex("5a70"), true, true}
	compact := keybytes.ToCompact()
	assert.Equal(t, common.FromHex("35a7"), compact)

	keybytes = Keybytes{common.FromHex("5a70"), true, false}
	compact = keybytes.ToCompact()
	assert.Equal(t, common.FromHex("15a7"), compact)

	keybytes = Keybytes{common.FromHex("5a7c"), false, true}
	compact = keybytes.ToCompact()
	assert.Equal(t, common.FromHex("205a7c"), compact)

	keybytes = Keybytes{common.FromHex("5a7c"), false, false}
	compact = keybytes.ToCompact()
	assert.Equal(t, common.FromHex("005a7c"), compact)
}

func TestCompactToKeybytes(t *testing.T) {
	compact := common.FromHex("35a7")
	keybytes := CompactToKeybytes(compact)
	assert.Equal(t, Keybytes{common.FromHex("5a70"), true, true}, keybytes)

	compact = common.FromHex("15a7")
	keybytes = CompactToKeybytes(compact)
	assert.Equal(t, Keybytes{common.FromHex("5a70"), true, false}, keybytes)

	compact = common.FromHex("205a7c")
	keybytes = CompactToKeybytes(compact)
	assert.Equal(t, Keybytes{common.FromHex("5a7c"), false, true}, keybytes)

	compact = common.FromHex("005a7c")
	keybytes = CompactToKeybytes(compact)
	assert.Equal(t, Keybytes{common.FromHex("5a7c"), false, false}, keybytes)
}
