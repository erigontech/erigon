// Copyright 2026 The Erigon Authors
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

package commitment

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCollectedHashedKeysIncludeHashedTouches(t *testing.T) {
	u := NewUpdates(ModeCollect, t.TempDir(), KeyToHexNibbleHash)
	account := []byte("0123456789abcdefghij")
	other := []byte("jihgfedcba9876543210")
	u.TouchPlainKey(string(account), nil, func(*KeyUpdate, []byte) {})
	u.TouchPlainKeyDirect(string(account), &Update{Flags: NonceUpdate, Nonce: 1})
	u.TouchPlainKeyDirect(string(other), &Update{Flags: NonceUpdate, Nonce: 2})
	partial := []byte{0x3, 0x0, 0xa}
	u.TouchHashedKey(partial)
	u.TouchHashedKey(partial)

	want := [][]byte{partial, KeyToHexNibbleHash(account), KeyToHexNibbleHash(other)}
	slices.SortFunc(want, bytes.Compare)
	require.Equal(t, want, u.CollectedHashedKeys())
}
