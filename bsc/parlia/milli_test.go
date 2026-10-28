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

package parlia

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types"
)

func TestMilliTimestamp(t *testing.T) {
	t.Parallel()

	h := &types.Header{Time: 1762741500}
	require.Equal(t, uint64(1762741500000), MilliTimestamp(h))

	h.MixDigest[30], h.MixDigest[31] = 0x01, 0xc2
	require.Equal(t, uint64(1762741500450), MilliTimestamp(h))
}
