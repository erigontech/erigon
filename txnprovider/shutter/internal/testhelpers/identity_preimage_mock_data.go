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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/txnprovider/shutter"
)

func MockIdentityPreimagesWithSlotIp(slot uint64, count uint64) (shutter.IdentityPreimages, error) {
	slotIp, err := MakeSlotIdentityPreimage(slot)
	if err != nil {
		return nil, err
	}

	ips, err := MockIdentityPreimages(count)
	if err != nil {
		return nil, err
	}

	return append(shutter.IdentityPreimages{slotIp}, ips...), nil
}

func TestMustMockIdentityPreimages(t *testing.T, count uint64) shutter.IdentityPreimages {
	ips, err := MockIdentityPreimages(count)
	require.NoError(t, err)
	return ips
}

func MockIdentityPreimages(count uint64) (shutter.IdentityPreimages, error) {
	ips := make([]*shutter.IdentityPreimage, count)
	for i := uint64(0); i < count; i++ {
		var err error
		ips[i], err = Uint64ToIdentityPreimage(i)
		if err != nil {
			return nil, err
		}
	}

	return ips, nil
}

func MakeSlotIdentityPreimage(slot uint64) (*shutter.IdentityPreimage, error) {
	// 32 bytes of zeros plus the block number as 20 byte big endian (ie starting with lots of
	// zeros as well). This ensures the block identity preimage is always alphanumerically before
	// any transaction identity preimages, because sender addresses cannot be that small.
	var buf bytes.Buffer
	buf.Write(common.BigToHash(common.Big0).Bytes())
	buf.Write(common.BigToHash(new(big.Int).SetUint64(slot)).Bytes()[12:])
	return shutter.IdentityPreimageFromBytes(buf.Bytes())
}

func Uint64ToIdentityPreimage(i uint64) (*shutter.IdentityPreimage, error) {
	buf := make([]byte, 52)
	binary.BigEndian.PutUint64(buf[:8], i)
	return shutter.IdentityPreimageFromBytes(buf)
}
