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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCollectUniqueDrainsInOrderWithMappedKeys(t *testing.T) {
	u := NewUpdates(ModeCollect, t.TempDir(), KeyToHexNibbleHash)
	for i, key := range []string{"c", "a", "b"} {
		u.TouchPlainKeyUnique(key, &Update{Flags: NonceUpdate, Nonce: uint64(i)})
	}
	u.TouchPlainKeyDirect("m", &Update{Flags: NonceUpdate, Nonce: 9})
	require.Equal(t, uint64(4), u.Size())

	var keys []string
	var nonces []uint64
	require.NoError(t, u.Drain(func(key string, update *Update) error {
		keys = append(keys, key)
		nonces = append(nonces, update.Nonce)
		return nil
	}))
	require.Equal(t, []string{"c", "a", "b", "m"}, keys)
	require.Equal(t, []uint64{0, 1, 2, 9}, nonces)
	require.Zero(t, u.Size())

	u.TouchPlainKeyUnique("x", &Update{Flags: NonceUpdate})
	u.TouchPlainKeyDirect("y", &Update{Flags: NonceUpdate})
	u.Reset()
	require.Zero(t, u.Size())
}

func TestCollectUniqueFallsBackOutsideCollectMode(t *testing.T) {
	u := NewUpdates(ModeUpdate, t.TempDir(), KeyToHexNibbleHash)
	u.TouchPlainKeyUnique("a", &Update{Flags: NonceUpdate, Nonce: 1})
	u.TouchPlainKeyUnique("a", &Update{Flags: BalanceUpdate})
	require.Equal(t, uint64(1), u.Size())
}
