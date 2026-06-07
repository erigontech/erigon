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

package merge

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
)

// posHeader returns a header that passes the difficulty/nonce/time checks at the
// top of verifyHeader, so a test can mutate one field to exercise a later branch.
// These branches all run before verifyHeader first dereferences chain.Config(),
// so the nil-Config readerMock is sufficient.
func posHeader() *types.Header {
	return &types.Header{
		Difficulty: *ProofOfStakeDifficulty,
		Nonce:      ProofOfStakeNonce,
		Time:       1,
	}
}

func TestVerifyHeader_PreConfigChecks(t *testing.T) {
	t.Parallel()
	mergeEngine := New(nil)
	parent := &types.Header{}

	t.Run("extra-data too long", func(t *testing.T) {
		t.Parallel()
		h := posHeader()
		h.Extra = make([]byte, params.MaximumExtraDataSize+1)
		require.ErrorContains(t, mergeEngine.verifyHeader(readerMock{}, h, parent), "extra-data longer")
	})

	t.Run("older block time", func(t *testing.T) {
		t.Parallel()
		h := posHeader()
		h.Time = 5
		require.ErrorIs(t, mergeEngine.verifyHeader(readerMock{}, h, &types.Header{Time: 5}), errOlderBlockTime)
	})

	t.Run("gas limit above cap", func(t *testing.T) {
		t.Parallel()
		h := posHeader()
		h.GasLimit = params.MaxBlockGasLimit + 1
		require.ErrorContains(t, mergeEngine.verifyHeader(readerMock{}, h, parent), "invalid gasLimit")
	})

	t.Run("gas used above gas limit", func(t *testing.T) {
		t.Parallel()
		h := posHeader()
		h.GasLimit = 1000
		h.GasUsed = 2000
		require.ErrorContains(t, mergeEngine.verifyHeader(readerMock{}, h, parent), "invalid gasUsed")
	})

	t.Run("non-sequential block number", func(t *testing.T) {
		t.Parallel()
		h := posHeader() // Number 0, parent Number 0 -> diff != 1
		require.ErrorIs(t, mergeEngine.verifyHeader(readerMock{}, h, parent), rules.ErrInvalidNumber)
	})

	t.Run("invalid uncle hash", func(t *testing.T) {
		t.Parallel()
		h := posHeader()
		h.Number = *uint256.NewInt(1)
		h.UncleHash = common.Hash{} // not empty.UncleHash
		require.ErrorIs(t, mergeEngine.verifyHeader(readerMock{}, h, parent), errInvalidUncleHash)
	})
}

func TestMerge_TxDependencies(t *testing.T) {
	t.Parallel()
	require.Nil(t, New(nil).TxDependencies(&types.Header{}))
}
