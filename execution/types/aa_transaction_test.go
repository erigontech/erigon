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

package types

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

// TestAATxnAsMessageCarriesNoBlobHashes pins that an AA message is not mistaken
// for a blob-carrying one: EIP-4844 validation keys off a non-nil blob hash
// slice, and an AA txn has no blobs. Its To is nil, so a non-nil empty slice
// would trip the blob contract-creation rule.
func TestAATxnAsMessageCarriesNoBlobHashes(t *testing.T) {
	t.Parallel()

	txn := &AccountAbstractionTransaction{FeeCap: uint256.NewInt(1)}
	msg, err := txn.AsMessage(Signer{}, nil, nil)
	require.NoError(t, err)

	require.Nil(t, msg.BlobHashes())
	require.Zero(t, msg.BlobGas())
	require.True(t, msg.To().IsNil())
}
