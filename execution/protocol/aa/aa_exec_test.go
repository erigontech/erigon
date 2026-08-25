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

package aa

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func TestCreateAAReceiptStatus(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		executionStatus uint64
		want            uint64
	}{
		{"success", types.ExecutionStatusSuccess, types.ReceiptStatusSuccessful},
		{"execution failure", types.ExecutionStatusExecutionFailure, types.ReceiptStatusFailed},
		{"postOp failure", types.ExecutionStatusPostOpFailure, types.ReceiptStatusFailed},
		{"execution and postOp failure", types.ExecutionStatusExecutionAndPostOpFailure, types.ReceiptStatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := CreateAAReceipt(common.Hash{0x11}, tc.executionStatus, 21000, 21000, 1, 0, nil)
			require.Equal(t, tc.want, r.Status)
		})
	}
}
