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

package solid

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// Per the beacon-APIs spec, Uint64 (and Gwei) fields must be serialized as
// JSON strings. Regression test for erigon#20562.
func TestDepositRequestJSONUint64AsString(t *testing.T) {
	d := &DepositRequest{Amount: 32000000000, Index: 2457249}
	got, err := json.Marshal(d)
	require.NoError(t, err)
	require.Contains(t, string(got), `"amount":"32000000000"`)
	require.Contains(t, string(got), `"index":"2457249"`)
}

func TestPendingDepositJSONUint64AsString(t *testing.T) {
	d := &PendingDeposit{Amount: 32000000000, Slot: 13605852}
	got, err := json.Marshal(d)
	require.NoError(t, err)
	require.Contains(t, string(got), `"amount":"32000000000"`)
	require.Contains(t, string(got), `"slot":"13605852"`)
}
