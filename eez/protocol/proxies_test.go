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

package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

func TestDecodeProxy(t *testing.T) {
	p, ok := DecodeProxy(common.HexToHash("0x000000010203040506070811223344556677889900aabbccddeeff0011223301"))
	require.True(t, ok)
	require.Equal(t, Proxy{Address: common.HexToAddress("0x11223344556677889900aabbccddeeff00112233"), RollupID: 0x0102030405060708}, p)

	_, ok = DecodeProxy(common.Hash{})
	require.False(t, ok, "a zero word is no proxy")
	var unflagged common.Hash
	unflagged[3] = 1
	for i := 11; i < 31; i++ {
		unflagged[i] = 0xab
	}
	_, ok = DecodeProxy(unflagged)
	require.False(t, ok, "fields without the flag are no registration")
	unflagged[31] = 2
	_, ok = DecodeProxy(unflagged)
	require.False(t, ok, "the flag is a canonical true")
}

func TestProxyKey(t *testing.T) {
	proxy := common.HexToAddress("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd")
	var preimage [64]byte
	copy(preimage[12:32], proxy[:])
	require.Equal(t, crypto.Keccak256Hash(preimage[:]), ProxyKey(proxy))
	require.NotEqual(t, ProxyKey(proxy), ProxyKey(common.HexToAddress("0x11")))
}
