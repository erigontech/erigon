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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
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
