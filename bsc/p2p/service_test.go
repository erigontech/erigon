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

package bscp2p

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEthVersionFromCaps(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		caps    []string
		version uint
		ok      bool
	}{
		{caps: []string{"eth/68", "snap/1"}, version: 68, ok: true},
		{caps: []string{"eth/68", "eth/70", "bsc/1"}, version: 70, ok: true},
		{caps: []string{"eth/70", "eth/99"}, version: 70, ok: true},
		{caps: []string{"eth/66", "eth/x"}},
		{caps: nil},
	} {
		version, ok := ethVersionFromCaps(tc.caps)
		require.Equal(t, tc.ok, ok, tc.caps)
		require.Equal(t, tc.version, version, tc.caps)
	}
}
