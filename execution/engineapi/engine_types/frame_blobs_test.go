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

package engine_types

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/stretchr/testify/require"
)

func TestFrameBlobsBundle(t *testing.T) {
	tx := &types.FrameTxWrapper{FrameTransaction: &types.FrameTransaction{NonceKeys: []uint256.Int{{}}, BlobVersionedHashes: []common.Hash{{1}}}, WrapperVersion: 1, Blobs: types.Blobs{{}}, Commitments: types.BlobKzgs{{}}, Proofs: make(types.KZGProofs, 128)}
	bundle, err := BlobsBundleFromTransactions(types.Transactions{tx})
	require.NoError(t, err)
	require.Len(t, bundle.Blobs, 1)
	require.Len(t, bundle.Proofs, 128)
}
