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
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	blst "github.com/supranational/blst/bindings/go"
)

func TestRandomSigma(t *testing.T) {
	firstByteAlways0 := true
	lastByteAlways0 := true
	for i := 0; i < 10; i++ {
		sigma, err := RandomSigma(rand.Reader)
		require.NoError(t, err)
		if sigma[0] != 0 {
			firstByteAlways0 = false
		}
		if sigma[BlockSize-1] != 0 {
			lastByteAlways0 = false
		}
	}
	assert.False(t, firstByteAlways0)
	assert.False(t, lastByteAlways0)
}

func TestPadding(t *testing.T) {
	testCases := []struct {
		mHex  string
		bsHex []string
	}{
		{
			"",
			[]string{
				strings.Repeat("20", 32),
			},
		},
		{
			"99",
			[]string{
				"99" + strings.Repeat("1f", 31),
			},
		},
		{
			"9999",
			[]string{
				"9999" + strings.Repeat("1e", 30),
			},
		},
		{
			strings.Repeat("99", 31),
			[]string{
				strings.Repeat("99", 31) + "01",
			},
		},
		{
			strings.Repeat("99", 32),
			[]string{
				strings.Repeat("99", 32),
				strings.Repeat("20", 32),
			},
		},
		{
			strings.Repeat("99", 33),
			[]string{
				strings.Repeat("99", 32),
				"99" + strings.Repeat("1f", 31),
			},
		},
	}

	for _, test := range testCases {
		m, err := hex.DecodeString(test.mHex)
		require.NoError(t, err)
		bs := []Block{}
		for _, bHex := range test.bsHex {
			bBytes, err := hex.DecodeString(bHex)
			require.NoError(t, err)
			assert.Len(t, bBytes, 32)
			var bByteArray [32]byte
			copy(bByteArray[:], bBytes)
			b := Block(bByteArray)
			bs = append(bs, b)
		}

		padded := PadMessage(m)
		assert.Equal(t, bs, padded)
	}
}

func TestUnpadding(t *testing.T) {
	invalidBs := [][]string{
		{
			strings.Repeat("00", 32),
		},
		{
			"aabbcc" + strings.Repeat("00", 29),
		},
		{
			strings.Repeat("21", 32),
		},
		{
			strings.Repeat("99", 32),
			strings.Repeat("00", 32),
		},
	}
	for _, bsHex := range invalidBs {
		bs := []Block{}
		for _, bHex := range bsHex {
			bBytes, err := hex.DecodeString(bHex)
			require.NoError(t, err)
			assert.Len(t, bBytes, 32)
			var bByteArray [32]byte
			copy(bByteArray[:], bBytes)
			b := Block(bByteArray)
			bs = append(bs, b)
		}

		_, err := UnpadMessage(bs)
		assert.NotEqual(t, err, nil)
	}

	testCases := []struct {
		bsHex []string
		mHex  string
	}{
		{
			[]string{
				strings.Repeat("20", 32),
			},
			"",
		},
		{
			[]string{
				"99" + strings.Repeat("1f", 31),
			},
			"99",
		},
		{
			[]string{
				strings.Repeat("99", 31) + "01",
			},
			strings.Repeat("99", 31),
		},
		{
			[]string{
				strings.Repeat("99", 32),
				strings.Repeat("20", 32),
			},
			strings.Repeat("99", 32),
		},
	}
	for _, test := range testCases {
		bs := []Block{}
		for _, bHex := range test.bsHex {
			bBytes, err := hex.DecodeString(bHex)
			require.NoError(t, err)
			assert.Len(t, bBytes, 32)
			var bByteArray [32]byte
			copy(bByteArray[:], bBytes)
			b := Block(bByteArray)
			bs = append(bs, b)
		}
		m, err := hex.DecodeString(test.mHex)
		require.NoError(t, err)

		unpadded, err := UnpadMessage(bs)
		require.NoError(t, err)
		assert.Equal(t, m, unpadded)
	}
}

func TestPaddingRoundtrip(t *testing.T) {
	ms := [][]byte{
		[]byte(""),
		[]byte("a"),
		bytes.Repeat([]byte("a"), 31),
		bytes.Repeat([]byte("a"), 32),
		bytes.Repeat([]byte("a"), 33),
	}
	for i := 0; i < 100; i++ {
		l, err := rand.Int(rand.Reader, big.NewInt(100))
		require.NoError(t, err)
		m := make([]byte, l.Int64())
		_, err = rand.Read(m)
		require.NoError(t, err)
		ms = append(ms, m)
	}
	for _, m := range ms {
		padded := PadMessage(m)
		unpadded, err := UnpadMessage(padded)
		require.NoError(t, err)
		assert.Equal(t, m, unpadded)
	}
}

func makeKeys(t *testing.T) (*EonPublicKey, *EpochSecretKey, *EpochID) {
	t.Helper()
	n := 3
	threshold := uint64(2)
	epochID := ComputeEpochID([]byte("epoch1"))

	ps := []*Polynomial{}
	gammas := []*Gammas{}
	for i := 0; i < n; i++ {
		p, err := RandomPolynomial(rand.Reader, threshold-1)
		require.NoError(t, err)
		ps = append(ps, p)
		gammas = append(gammas, p.Gammas())
	}

	eonSecretKeyShares := []*EonSecretKeyShare{}
	epochSecretKeyShares := []*EpochSecretKeyShare{}
	eonSecretKey := big.NewInt(0)
	for i := 0; i < n; i++ {
		eonSecretKey.Add(eonSecretKey, ps[i].Eval(big.NewInt(0)))
		eonSecretKey.Mod(eonSecretKey, order)

		ss := []*big.Int{}
		for j := 0; j < n; j++ {
			s := ps[j].EvalForKeyper(i)
			ss = append(ss, s)
		}
		eonSecretKeyShares = append(eonSecretKeyShares, ComputeEonSecretKeyShare(ss))
		_ = ComputeEonPublicKeyShare(i, gammas)
		epochSecretKeyShares = append(epochSecretKeyShares, ComputeEpochSecretKeyShare(eonSecretKeyShares[i], epochID))
	}
	eonPublicKey := ComputeEonPublicKey(gammas)
	eonPublicKeyExp := (*EonPublicKey)(generateP2(eonSecretKey))
	assert.True(t, eonPublicKey.Equal(eonPublicKeyExp))
	epochSecretKey, err := ComputeEpochSecretKey(
		[]int{0, 1},
		[]*EpochSecretKeyShare{epochSecretKeyShares[0], epochSecretKeyShares[1]},
		threshold)
	require.NoError(t, err)
	return eonPublicKey, epochSecretKey, epochID
}

func TestRoundTrip(t *testing.T) {
	eonPublicKey, epochSecretKey, epochID := makeKeys(t)

	m := []byte("hello")
	sigma, err := RandomSigma(rand.Reader)
	require.NoError(t, err)

	encM := Encrypt(m, eonPublicKey, epochID, sigma)
	decM, err := encM.Decrypt(epochSecretKey)
	require.NoError(t, err)
	assert.Equal(t, m, decM)
}

func TestC1Malleability(t *testing.T) {
	message := []byte("secret message")
	eonPublicKey, decryptionKey, epochIDPoint := makeKeys(t)
	originalSigma, err := RandomSigma(rand.Reader)
	assert.Equal(t, err, nil, "Could not get random sigma")
	encryptedMessage := Encrypt(
		message,
		eonPublicKey,
		epochIDPoint,
		originalSigma,
	)
	// we move C1 around, until we find a legal padding
	for i := 1; i <= 10000; i++ {
		newC1 := new(blst.P2)
		newC1.FromAffine(encryptedMessage.C1)
		newC1.AddAssign(encryptedMessage.C1)
		encryptedMessage.C1 = newC1.ToAffine()
		sigma := encryptedMessage.Sigma(decryptionKey)
		decryptedBlocks := DecryptBlocks(encryptedMessage.C3, sigma)
		_, err = UnpadMessage(decryptedBlocks)
		if err == nil {
			break
		}
	}
	msg, err := encryptedMessage.Decrypt(decryptionKey)
	assert.False(t, bytes.Equal(message, msg), "decryption successful, in spite of tampered C1")
	assert.NotEqual(t, err, nil, "decryption successful, in spite of tampered C1")
}

func TestMessageMalleability(t *testing.T) {
	messageBlock, err := RandomSigma(rand.Reader)
	assert.Equal(t, err, nil, "could not get random message")
	originalMessage := messageBlock[:]

	eonPublicKey, decryptionKey, epochIDPoint := makeKeys(t)
	sigma, err := RandomSigma(rand.Reader)
	assert.Equal(t, err, nil, "could not get random sigma")
	encryptedMessage := Encrypt(
		originalMessage,
		eonPublicKey,
		epochIDPoint,
		sigma,
	)

	// malleate message
	flipMask := 0b00000001
	encryptedB0 := int(encryptedMessage.C3[0][0])
	encryptedB0 ^= flipMask
	encryptedMessage.C3[0][0] = byte(encryptedB0)
	malleatedMessage := make([]byte, len(originalMessage))
	copy(malleatedMessage, originalMessage)
	plaintextB0 := int(malleatedMessage[0])
	plaintextB0 ^= flipMask
	malleatedMessage[0] = byte(plaintextB0)

	decryptedMessage, err := encryptedMessage.Decrypt(decryptionKey)
	assert.False(t, bytes.Equal(decryptedMessage, malleatedMessage), "message was successfully malleated")
	assert.NotEqual(t, err, nil, "decryption successful, in spite of tampered message")
}
