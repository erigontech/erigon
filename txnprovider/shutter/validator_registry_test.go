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

package shutter

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
)

func TestRegistrationMessageMarshalRoundtrip(t *testing.T) {
	m := &LegacyRegistrationMessage{
		Version:                  1,
		ChainId:                  2,
		ValidatorRegistryAddress: common.HexToAddress("0x1234567890123456789012345678901234567890"),
		ValidatorIndex:           3,
		Nonce:                    4,
		IsRegistration:           true,
	}
	marshaled := m.Marshal()
	unmarshaled := new(LegacyRegistrationMessage)
	err := unmarshaled.Unmarshal(marshaled)
	require.NoError(t, err)
	require.Equal(t, m, unmarshaled)
}

func TestRegistrationMessageInvalidUnmarshal(t *testing.T) {
	base := bytes.Repeat([]byte{0}, 46)
	require.NoError(t, new(LegacyRegistrationMessage).Unmarshal(base))

	for _, b := range [][]byte{
		{},
		bytes.Repeat([]byte{0}, 45),
		bytes.Repeat([]byte{0}, 47),
		bytes.Repeat([]byte{0}, 92),
	} {
		err := new(LegacyRegistrationMessage).Unmarshal(b)
		assert.ErrorContains(t, err, "invalid registration message length")
	}

	for _, isRegistrationByte := range []byte{2, 3, 255} {
		b := bytes.Repeat([]byte{0}, 46)
		b[45] = isRegistrationByte
		err := new(LegacyRegistrationMessage).Unmarshal(b)
		assert.ErrorContains(t, err, "invalid registration message type byte")
	}
}
