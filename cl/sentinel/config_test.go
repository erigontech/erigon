// Copyright 2024 The Erigon Authors
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

package sentinel

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func MockPrivateKey(dec int64) *ecdsa.PrivateKey {
	pKey := new(ecdsa.PrivateKey)
	pKey.D = big.NewInt(dec)
	return pKey
}

func TestConvertToCryptoPrivkey(t *testing.T) {
	testCases := []struct {
		dec      int64
		expected string
	}{
		{
			dec:      1234567890123456,
			expected: "000000000000000000000000000000000000000000000000000462d53c8abac0",
		},
		{
			dec:      456723645272495,
			expected: "00000000000000000000000000000000000000000000000000019f6342a311af",
		},
		{
			dec:      238762543819574,
			expected: "0000000000000000000000000000000000000000000000000000d9273c9c2b36",
		},
	}

	for _, testCase := range testCases {
		pKey := MockPrivateKey(testCase.dec)

		cryptoPKey, err := convertToCryptoPrivkey(pKey)
		require.NoError(t, err)

		raw, err := cryptoPKey.Raw()
		require.NoError(t, err)

		rawString := fmt.Sprintf("%x", raw)
		require.EqualValues(t, testCase.expected, rawString)
	}
}

func TestMultiAddressBuilder(t *testing.T) {
	testCases := []struct {
		ipAddr      string
		port        uint
		expected    string
		shouldError bool
	}{
		{
			ipAddr:      "192.158.1.38",
			port:        80,
			expected:    "/ip4/192.158.1.38/tcp/80",
			shouldError: false,
		},
		{
			ipAddr:      "192.158..1.38",
			port:        80,
			expected:    "",
			shouldError: true,
		},
		{
			ipAddr:      "192.15.38",
			port:        45,
			expected:    "",
			shouldError: true,
		},
	}

	for _, testCase := range testCases {
		multiAddr, err := multiAddressBuilder(testCase.ipAddr, testCase.port)
		if testCase.shouldError {
			require.Error(t, err)
			require.Nil(t, multiAddr)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, testCase.expected, multiAddr.String())
	}
}
