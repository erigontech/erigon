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

package crypto

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	blst "github.com/supranational/blst/bindings/go"

	"github.com/erigontech/erigon-lib/common"
)

func encryptedMessage() *EncryptedMessage {
	blocks := []Block{}
	for i := 0; i < 3; i++ {
		s := bytes.Repeat([]byte{byte(i)}, 32)
		var b Block
		copy(b[:], s)
		blocks = append(blocks, b)
	}

	return &EncryptedMessage{
		C1: makeTestG2(5),
		C2: blocks[0],
		C3: blocks[1:],
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	m1 := encryptedMessage()
	m2 := &EncryptedMessage{}
	err := m2.Unmarshal(m1.Marshal())
	require.NoError(t, err)
	assert.Equal(t, m1, m2)
}

func TestUnmarshalBroken(t *testing.T) {
	d := encryptedMessage().Marshal()
	m := EncryptedMessage{}

	err := m.Unmarshal(d[:16])
	assert.True(t, err != nil)

	err = m.Unmarshal(d[:32])
	assert.True(t, err != nil)

	err = m.Unmarshal(d[:65])
	assert.True(t, err != nil)

	err = m.Unmarshal(d[:len(d)-1])
	assert.True(t, err != nil)

	v := d[:]
	v[0]++
	err = m.Unmarshal(v)
	assert.True(t, err != nil)
}

func TestMarshal(t *testing.T) {
	ask := (*EonSecretKeyShare)(big.NewInt(123))
	ashM := ask.Marshal()
	askD := new(EonSecretKeyShare)
	require.NoError(t, askD.Unmarshal(ashM))
	assert.True(t, ask.Equal(askD))

	apks := (*EonPublicKeyShare)(makeTestG2(5))
	apksM := apks.Marshal()
	apksD := new(EonPublicKeyShare)
	require.NoError(t, apksD.Unmarshal(apksM))
	assert.True(t, apksD.Equal(apks))

	apk := (*EonPublicKey)(makeTestG2(6))
	apkM := apk.Marshal()
	apkD := new(EonPublicKey)
	require.NoError(t, apkD.Unmarshal(apkM))
	assert.True(t, apkD.Equal(apk))

	esks := (*EpochSecretKeyShare)(makeTestG1(7))
	esksM := esks.Marshal()
	esksD := new(EpochSecretKeyShare)
	require.NoError(t, esksD.Unmarshal(esksM))
	assert.True(t, esksD.Equal(esks))

	esk := (*EpochSecretKey)(makeTestG1(8))
	eskM := esk.Marshal()
	eskD := new(EpochSecretKey)
	require.NoError(t, eskD.Unmarshal(eskM))
	assert.True(t, eskD.Equal(esk))
}

func TestIdentifyVersion(t *testing.T) {
	d := encryptedMessage().Marshal()
	assert.True(t, IdentifyVersion(d) == VersionIdentifier)

	// legacy version
	assert.True(t, IdentifyVersion(d[1:]) != VersionIdentifier)
	assert.True(t, IdentifyVersion(d[1:]) == 0x00)
}

func TestMarshalGammasEmpty(t *testing.T) {
	g := &Gammas{}
	m := g.Marshal()
	assert.True(t, len(m) == 0)
}

func TestMarshalGammasError(t *testing.T) {
	validGammaEncoding := makeTestG2(1).Compress()
	inputs := [][]byte{
		{0x00},
		bytes.Repeat([]byte{0xaa}, 96),
		common.FromHex(
			"87f481803120be4e565dc88cdbb1ae4c1ddfa249bd34cdc43982d926278535e84ee584aa9ae3553f56d02d3aa842b3941058f0dcabacbc551dca1d04ba7647c806acf7f960809438993359338dc4858aadcbce50f9a370986c74053303ab4449",
		),
		validGammaEncoding[:len(validGammaEncoding)-1],
		append([]byte{0x00}, validGammaEncoding...),
	}
	for _, input := range inputs {
		g := &Gammas{}
		err := g.Unmarshal(input)
		assert.True(t, err != nil)
	}
}

func TestMarshalGammasRoundtrip(t *testing.T) {
	gammaValues := []*Gammas{
		{},
		{makeTestG2(1)},
		{makeTestG2(2), makeTestG2(2), makeTestG2(3), makeTestG2(4), makeTestG2(5), makeTestG2(6)},
	}
	for _, gammas := range gammaValues {
		m := gammas.Marshal()
		assert.True(t, len(m) == len([]*blst.P2Affine(*gammas))*96)
		g := &Gammas{}
		require.NoError(t, g.Unmarshal(m))
		assert.Equal(t, gammas, g)

		mText, err := gammas.MarshalText()
		require.NoError(t, err)
		gText := &Gammas{}
		require.NoError(t, gText.UnmarshalText(mText))
		assert.Equal(t, gammas, gText)
	}
}
