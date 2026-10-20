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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunTransactionTest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transaction.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
		"gasLimitPriceOverflow": {
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
		}
	}`), 0o600))
	filter, err := compileTestFilter(".*", nil)
	require.NoError(t, err)

	results, err := runTransactionTest(path, filter)
	require.NoError(t, err)
	require.Equal(t, []testResult{{Name: "gasLimitPriceOverflow", Pass: true}}, results)
}

func TestRunTransactionTestEESTForks(t *testing.T) {
	tests := map[string]string{
		"Cancun": `{
			"nonceOverflow": {
				"txbytes": "0xf868890100000000000000000a82520894c0f6dc9e5836f54caadbf59cc69346c508e1992b80801ca037f68b9ea67aa96dcef806e691314bec49d0d6a7b0da43347fd8907a466944eda059b95678162573fc440c25b1c11339b7700153cdc2fe029760c7e5581122282e",
				"result": {
					"Cancun": {"intrinsicGas": "0x00", "exception": "TransactionException.NONCE_OVERFLOW"}
				}
			}
		}`,
		"Amsterdam": `{
			"emptyAuthorizationList": {
				"txbytes": "0x04f86401808007830186a09400000000000000000000000000000000000000008080c0c001a0884bf485199d5e86e675e79f17a710b754a36915a33a2838c24e1e53378699f1a074085ca12ad0f729b6bd6c37a7a23e925c7e1e87a557ad3100a9113786c98afd",
				"result": {
					"Amsterdam": {"intrinsicGas": "0x00", "exception": "TransactionException.TYPE_4_EMPTY_AUTHORIZATION_LIST"}
				}
			}
		}`,
	}
	filter, err := compileTestFilter(".*", nil)
	require.NoError(t, err)

	for fork, fixture := range tests {
		t.Run(fork, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "transaction.json")
			require.NoError(t, os.WriteFile(path, []byte(fixture), 0o600))

			results, err := runTransactionTest(path, filter)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.True(t, results[0].Pass, results[0].Error)
		})
	}
}
