package main

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
		ChainID:                  2,
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
