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

// TestProxyCalls: the manager's and the proxy's entrypoints, selectors
// pinned to the protocol's.
func TestProxyCalls(t *testing.T) {
	original := common.HexToAddress("0x277aF7Ba0CFBD9455Aa0401528f4BcEC683EA27E")
	args := "000000000000000000000000277af7ba0cfbd9455aa0401528f4bcec683ea27e" +
		"0000000000000000000000000000000000000000000000000000000000000001"
	require.Equal(t, common.FromHex("0xeb20c0aa"+args), EncodeComputeCrossChainProxyAddress(original, 1))
	require.Equal(t, common.FromHex("0xa7587c62"+args), EncodeCreateCrossChainProxy(original, 1))

	target := common.HexToAddress("0x2E983A1Ba5e8b38AAAeC4B440B9dDcFBf72E15d1")
	enc, err := EncodeExecuteOnBehalf(target, 0, []byte{0x55, 0x24, 0x10, 0x77, 0x01})
	require.NoError(t, err)
	require.Equal(t, common.FromHex("0x8205f3e1"+
		"0000000000000000000000002e983a1ba5e8b38aaaec4b440b9ddcfbf72e15d1"+
		"0000000000000000000000000000000000000000000000000000000000000000"+
		"0000000000000000000000000000000000000000000000000000000000000060"+
		"0000000000000000000000000000000000000000000000000000000000000005"+
		"5524107701000000000000000000000000000000000000000000000000000000"), enc)

	a, err := DecodeAddressReturn(common.FromHex("0x0000000000000000000000001fc99e43b3dbecfe7dfe20ae0acbd58deea30cf7"))
	require.NoError(t, err)
	require.Equal(t, common.HexToAddress("0x1Fc99E43B3DBECFe7dFe20Ae0ACbD58dEea30cf7"), a)
	_, err = DecodeAddressReturn([]byte{1})
	require.Error(t, err)
}
