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

package t8ntool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A signed transaction supplied via JSON (no secretKey) must keep its v, r, s
// signature values so the sender can be recovered.
func TestGetTransactionPreservesSignature(t *testing.T) {
	t.Parallel()
	input := []byte(`{
		"input":"0x",
		"gas":"0x5f5e100",
		"gasPrice":"0x1",
		"nonce":"0x0",
		"to":"0x095e7baea6a6c7c4c2dfeb977efac326af552d87",
		"value":"0x186a0",
		"v":"0x1b",
		"r":"0x88544c93a564b4c28d2ffac2074a0c55fdd4658fe0d215596ed2e32e3ef7f56b",
		"s":"0x7fb4075d54190f825d7c47bb820284757b34fd6293904a93cddb1d3aa961ac28"
	}`)

	var twk txWithKey
	require.NoError(t, twk.UnmarshalJSON(input))

	v, r, s := twk.tx.RawSignatureValues()
	require.EqualValues(t, 0x1b, v.Uint64(), "v must be preserved")
	require.False(t, r.IsZero(), "r must be preserved")
	require.False(t, s.IsZero(), "s must be preserved")
}
