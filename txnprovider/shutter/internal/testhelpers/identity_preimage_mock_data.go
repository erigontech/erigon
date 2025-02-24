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
	"testing"

	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/txnprovider/shutter"
)

func MockIdentityPreimagesWithSlotIp(t *testing.T, slot uint64, count uint64) shutter.IdentityPreimages {
	return append(shutter.IdentityPreimages{MakeSlotIdentityPreimage(t, slot)}, MockIdentityPreimages(t, count)...)
}

func MockIdentityPreimages(t *testing.T, count uint64) shutter.IdentityPreimages {
	ips := make([]shutter.IdentityPreimage, count)
	for i := uint64(0); i < count; i++ {
		ips[i] = Uint64ToIdentityPreimage(t, i)
	}

	return ips
}

func MakeSlotIdentityPreimage(t *testing.T, slot uint64) shutter.IdentityPreimage {
	// 32 bytes of zeros plus the block number as 20 byte big endian (ie starting with lots of
	// zeros as well). This ensures the block identity preimage is always alphanumerically before
	// any transaction identity preimages, because sender addresses cannot be that small.
	var buf bytes.Buffer
	buf.Write(libcommon.BigToHash(libcommon.Big0).Bytes())
	buf.Write(libcommon.BigToHash(new(big.Int).SetUint64(slot)).Bytes()[12:])
	ip, err := shutter.IdentityPreimageFromBytes(buf.Bytes())
	require.NoError(t, err)
	return ip
}

func Uint64ToIdentityPreimage(t *testing.T, i uint64) shutter.IdentityPreimage {
	buf := make([]byte, 52)
	binary.BigEndian.PutUint64(buf[:8], i)
	ip, err := shutter.IdentityPreimageFromBytes(buf)
	require.NoError(t, err)
	return ip
}
