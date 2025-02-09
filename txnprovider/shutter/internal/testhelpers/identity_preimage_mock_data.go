// Copyright 2025 The Erigon Authors
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

package testhelpers

import (
	"bytes"
	"encoding/binary"
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/txnprovider/shutter"
)

func MockIdentityPreimagesWithSlotIp(slot uint64, count uint64) []shutter.IdentityPreimage {
	return append([]shutter.IdentityPreimage{MakeSlotIdentityPreimage(slot)}, MockIdentityPreimages(count)...)
}

func MockIdentityPreimages(count uint64) []shutter.IdentityPreimage {
	ips := make([]shutter.IdentityPreimage, count)
	for i := uint64(0); i < count; i++ {
		ips[i] = Uint64ToIdentityPreimage(i)
	}
	return ips
}

func MakeSlotIdentityPreimage(slot uint64) shutter.IdentityPreimage {
	// 32 bytes of zeros plus the block number as 20 byte big endian (ie starting with lots of
	// zeros as well). This ensures the block identity preimage is always alphanumerically before
	// any transaction identity preimages, because sender addresses cannot be that small.
	var buf bytes.Buffer
	buf.Write(libcommon.BigToHash(libcommon.Big0).Bytes())
	buf.Write(libcommon.BigToHash(new(big.Int).SetUint64(slot)).Bytes()[12:])
	return buf.Bytes()
}

func Uint64ToIdentityPreimage(i uint64) shutter.IdentityPreimage {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}
