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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
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
