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

package p2p

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func TestValidateBALResponse(t *testing.T) {
	t.Parallel()
	bal := []byte{0xc2, 0x01, 0x02} // arbitrary non-sentinel payload
	balHash := crypto.Keccak256Hash(bal)
	h0 := common.BytesToHash([]byte{1})
	h1 := common.BytesToHash([]byte{2})

	t.Run("valid populated BAL is returned", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: balHash}}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal})
		require.NoError(t, err)
		require.False(t, bad)
		require.Equal(t, bal, out[h0])
	})

	t.Run("0x80 sentinel is a miss not an error", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: balHash}}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{{0x80}})
		require.NoError(t, err)
		require.False(t, bad)
		require.NotContains(t, out, h0)
	})

	t.Run("empty entry is a miss", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: balHash}}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{{}})
		require.NoError(t, err)
		require.False(t, bad)
		require.NotContains(t, out, h0)
	})

	t.Run("0xc0 accepted only for the empty-BAL hash", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: empty.BlockAccessListHash}}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{{0xc0}})
		require.NoError(t, err)
		require.False(t, bad)
		require.Equal(t, []byte{0xc0}, out[h0])
	})

	t.Run("0xc0 for a non-empty-BAL hash penalises the peer", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: balHash}}
		_, bad, err := validateBALResponse(reqs, []rlp.RawValue{{0xc0}})
		require.Error(t, err)
		require.True(t, bad)
	})

	t.Run("hash mismatch penalises the peer", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: common.BytesToHash([]byte{0xff})}}
		_, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal})
		require.Error(t, err)
		require.True(t, bad)
	})

	t.Run("over-long response penalises the peer", func(t *testing.T) {
		reqs := []BALRequest{{Hash: h0, Number: 1, ExpectedHash: balHash}}
		_, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal, bal})
		require.Error(t, err)
		require.True(t, bad)
	})

	t.Run("short response leaves trailing requests absent", func(t *testing.T) {
		reqs := []BALRequest{
			{Hash: h0, Number: 1, ExpectedHash: balHash},
			{Hash: h1, Number: 2, ExpectedHash: balHash},
		}
		out, bad, err := validateBALResponse(reqs, []rlp.RawValue{bal}) // only first answered
		require.NoError(t, err)
		require.False(t, bad)
		require.Equal(t, bal, out[h0])
		require.NotContains(t, out, h1)
	})
}

func TestBALRequestsForHeaders(t *testing.T) {
	t.Parallel()
	balHash := common.BytesToHash([]byte{0xaa})
	withBAL := &types.Header{Number: *uint256.NewInt(10), BlockAccessListHash: &balHash}
	preFork := &types.Header{Number: *uint256.NewInt(11)} // BlockAccessListHash == nil

	reqs := balRequestsForHeaders([]*types.Header{preFork, withBAL})
	require.Len(t, reqs, 1)
	require.Equal(t, withBAL.Hash(), reqs[0].Hash)
	require.Equal(t, balHash, reqs[0].ExpectedHash)
	require.Equal(t, uint64(10), reqs[0].Number)
}
