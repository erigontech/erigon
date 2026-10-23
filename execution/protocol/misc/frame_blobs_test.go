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

package misc

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/stretchr/testify/require"
)

func TestFrameBlobHashes(t *testing.T) {
	hashes := []common.Hash{{1, 2}}
	txs := []types.Transaction{&types.FrameTransaction{NonceKeys: []uint256.Int{{}}, Frames: []types.Frame{{}}, BlobVersionedHashes: hashes}}
	require.NoError(t, ValidateBlobs(params.GasPerBlob, params.GasPerBlob, 1, hashes, &txs))
}
