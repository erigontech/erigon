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

package commitment

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"
)

func pbinTestKeccak(t *testing.T, parts ...[]byte) []byte {
	t.Helper()
	h := sha3.NewLegacyKeccak256()
	for _, part := range parts {
		_, err := h.Write(part)
		require.NoError(t, err)
	}
	return h.Sum(nil)
}

func pbinTestAddr(t *testing.T, value string) []byte {
	t.Helper()
	addr, err := hex.DecodeString(value)
	require.NoError(t, err)
	require.Len(t, addr, 20)
	return addr
}

func pbinTestAddress32(addr []byte) []byte {
	out := make([]byte, 32)
	copy(out[32-len(addr):], addr)
	return out
}

func pbinTestBE32(value uint64) []byte {
	out := make([]byte, 32)
	binary.BigEndian.PutUint64(out[24:], value)
	return out
}

func pbinTestSlot(value uint64) []byte { return pbinTestBE32(value) }

func pbinTestConcat(parts ...[]byte) []byte {
	var out []byte
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}
