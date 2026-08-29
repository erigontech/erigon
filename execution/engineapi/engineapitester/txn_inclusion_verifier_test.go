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

package engineapitester

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
)

// An expected inclusion at an index beyond the payload's txn list must be reported
// as missing instead of passing vacuously.
func TestVerifyTxnsOrderedInclusionReportsMissingTxnIndexes(t *testing.T) {
	verifier := NewTxnInclusionVerifier(nil)
	payload := &enginetypes.ExecutionPayload{}
	err := verifier.VerifyTxnsOrderedInclusion(t.Context(), payload, OrderedInclusion{
		TxnIndex: 0,
		TxnHash:  common.HexToHash("0x01"),
	})
	require.ErrorContains(t, err, "txns missing")
}

// A hash mismatch at an expected index is a missing inclusion, so the receipt of the
// unrelated txn occupying that index must not be fetched.
func TestVerifyTxnsOrderedInclusionDoesNotFetchReceiptOnHashMismatch(t *testing.T) {
	txn := types.NewTransaction(0, common.HexToAddress("0x02"), uint256.NewInt(1), 21_000, uint256.NewInt(1), nil)
	var buf bytes.Buffer
	require.NoError(t, txn.MarshalBinary(&buf))

	verifier := NewTxnInclusionVerifier(nil) // a receipt lookup would panic on the nil client
	payload := &enginetypes.ExecutionPayload{Transactions: []hexutil.Bytes{buf.Bytes()}}
	err := verifier.VerifyTxnsOrderedInclusion(t.Context(), payload, OrderedInclusion{
		TxnIndex: 0,
		TxnHash:  common.HexToHash("0x01"),
	})
	require.ErrorContains(t, err, "txns missing")
}
