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
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/abi"
)

// ProxiesSlot is the storage slot of authorizedProxies on both managers,
// EEZ on L1 and EEZL2.
const ProxiesSlot = 0

// Proxy is a manager's record of a cross-chain proxy: the contract it stands
// for and that contract's rollup.
type Proxy struct {
	Address  common.Address
	RollupID uint64
}

// ProxyKey is the storage key of authorizedProxies[proxy].
func ProxyKey(proxy common.Address) common.Hash {
	var preimage [64]byte
	copy(preimage[12:32], proxy[:])
	preimage[63] = ProxiesSlot
	return crypto.Keccak256Hash(preimage[:])
}

// DecodeProxy reads a word of authorizedProxies: bytes 3..11 the rollup id,
// big-endian, 11..31 the address, 31 the registration flag, 1. False for any
// word without the flag.
func DecodeProxy(word common.Hash) (Proxy, bool) {
	if word[31] != 1 {
		return Proxy{}, false
	}
	return Proxy{Address: common.BytesToAddress(word[11:31]), RollupID: binary.BigEndian.Uint64(word[3:11])}, true
}

var (
	proxyArgs = abi.Arguments{
		{Name: "originalAddress", Type: mustType("address", nil)},
		{Name: "originalRollupId", Type: mustType("uint64", nil)},
	}
	addressReturn = abi.Arguments{{Type: mustType("address", nil)}}

	computeCrossChainProxyAddressMethod = abi.NewMethod("computeCrossChainProxyAddress", "computeCrossChainProxyAddress", abi.Function, "view", false, false,
		proxyArgs, addressReturn)
	createCrossChainProxyMethod = abi.NewMethod("createCrossChainProxy", "createCrossChainProxy", abi.Function, "nonpayable", false, false,
		proxyArgs, addressReturn)
	// CrossChainProxy forwards a call of its manager to destination.
	executeOnBehalfMethod = abi.NewMethod("executeOnBehalf", "executeOnBehalf", abi.Function, "payable", false, true,
		abi.Arguments{
			{Name: "destination", Type: mustType("address", nil)},
			{Name: "callGas", Type: mustType("uint64", nil)},
			{Name: "data", Type: mustType("bytes", nil)},
		}, nil)
)

// ExecuteOnBehalfSelector is the selector of CrossChainProxy.executeOnBehalf.
var ExecuteOnBehalfSelector = [4]byte(executeOnBehalfMethod.ID)

// EncodeComputeCrossChainProxyAddress returns the input of
// computeCrossChainProxyAddress(original, rollupID).
func EncodeComputeCrossChainProxyAddress(original common.Address, rollupID uint64) []byte {
	return proxyCall(computeCrossChainProxyAddressMethod, original, rollupID)
}

// EncodeCreateCrossChainProxy returns the input of
// createCrossChainProxy(original, rollupID).
func EncodeCreateCrossChainProxy(original common.Address, rollupID uint64) []byte {
	return proxyCall(createCrossChainProxyMethod, original, rollupID)
}

func proxyCall(m abi.Method, original common.Address, rollupID uint64) []byte {
	var words [64]byte
	copy(words[12:32], original[:])
	binary.BigEndian.PutUint64(words[56:], rollupID)
	return append(slices.Clone(m.ID), words[:]...)
}

// EncodeExecuteOnBehalf returns the input of
// executeOnBehalf(destination, callGas, data).
func EncodeExecuteOnBehalf(destination common.Address, callGas uint64, data []byte) ([]byte, error) {
	return encodeCall(executeOnBehalfMethod, destination, callGas, data)
}

// DecodeAddressReturn decodes the output of a function returning an
// address.
func DecodeAddressReturn(out []byte) (common.Address, error) {
	values, err := addressReturn.Unpack(out)
	if err != nil {
		return common.Address{}, fmt.Errorf("address return: %w", err)
	}
	return values[0].(common.Address), nil
}
