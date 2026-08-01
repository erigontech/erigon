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

package testutil

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestTransactionTestGasLimitPriceOverflow(t *testing.T) {
	const fixture = `{
		"txbytes": "0xf87e80990100000000000000010000000000000001000000000000000288ffffffffffffffff94095e7baea6a6c7c4c2dfeb977efac326af552d8780801ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a01fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804",
		"result": {
			"Berlin": {"exception": "TransactionException.GASLIMIT_PRICE_PRODUCT_OVERFLOW"},
			"Byzantium": {"exception": "TransactionException.GASLIMIT_PRICE_PRODUCT_OVERFLOW"},
			"Constantinople": {"exception": "TransactionException.GASLIMIT_PRICE_PRODUCT_OVERFLOW"},
			"ConstantinopleFix": {"exception": "TransactionException.GASLIMIT_PRICE_PRODUCT_OVERFLOW"},
			"EIP150": {"exception": "TransactionException.GASLIMIT_PRICE_PRODUCT_OVERFLOW"},
			"EIP158": {"exception": "TransactionException.GASLIMIT_PRICE_PRODUCT_OVERFLOW"},
			"Frontier": {"exception": "TransactionException.GASLIMIT_PRICE_PRODUCT_OVERFLOW"},
			"Homestead": {"exception": "TransactionException.GASLIMIT_PRICE_PRODUCT_OVERFLOW"},
			"Istanbul": {"exception": "TransactionException.GASLIMIT_PRICE_PRODUCT_OVERFLOW"},
			"London": {"exception": "TransactionException.GASLIMIT_PRICE_PRODUCT_OVERFLOW"}
		}
	}`
	var test TransactionTest
	require.NoError(t, json.Unmarshal([]byte(fixture), &test))

	require.NoError(t, test.Run(uint256.NewInt(1)))
}
