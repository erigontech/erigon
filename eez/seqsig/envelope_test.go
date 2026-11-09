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

package seqsig_test

import (
	"bytes"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/execution/types"
)

var secp256k1N = uint256.MustFromHex("0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141")

func TestVerifyAcceptsSignedHead(t *testing.T) {
	b := testBlock(7)
	for name, block := range map[string][]byte{"with block": encodeBlock(t, b), "without block": nil} {
		v, err := signed(t, headOf(b), block).Verify(domain, common.HexToAddress(signerHex))
		require.NoError(t, err, name)
		require.Equal(t, headOf(b), v.Head, name)
		require.Equal(t, common.HexToAddress(signerHex), v.Signer, name)
		if block == nil {
			require.Nil(t, v.Block, name)
			continue
		}
		require.NotNil(t, v.Block, name)
		require.Equal(t, b.Hash(), v.Block.Hash(), name)
	}
}

func TestVerifyRejects(t *testing.T) {
	b7, b8 := testBlock(7), testBlock(8)
	other := common.HexToHash("0x22")
	authorized := common.HexToAddress(signerHex)
	// b7's header with one ommer: the hash still matches the head, the body
	// does not match the header.
	withOmmer := encodeBlock(t, types.NewBlockFromNetwork(b7.Header(), &types.Body{Uncles: []*types.Header{{}}}, nil))

	for _, c := range []struct {
		name       string
		env        func() seqsig.Envelope
		domain     seqsig.Domain
		authorized common.Address
		want       error
	}{
		{
			name:       "unauthorized signer",
			env:        func() seqsig.Envelope { return signed(t, headOf(b7), nil) },
			authorized: common.HexToAddress("0x0909090909090909090909090909090909090909"),
			want:       seqsig.ErrUnauthorizedSigner,
		},
		{
			name: "wrong rollup",
			env:  func() seqsig.Envelope { return signed(t, headOf(b7), nil) },
			domain: seqsig.Domain{
				ChainID: domain.ChainID, Registry: domain.Registry, RollupID: 2,
			},
			want: seqsig.ErrWrongRollup,
		},
		{
			name: "other chain id",
			env:  func() seqsig.Envelope { return signed(t, headOf(b7), nil) },
			domain: seqsig.Domain{
				ChainID: 1338, Registry: domain.Registry, RollupID: domain.RollupID,
			},
			want: seqsig.ErrUnauthorizedSigner,
		},
		{
			name: "tampered number",
			env: func() seqsig.Envelope {
				e := signed(t, headOf(b7), nil)
				e.Head.Number++
				return e
			},
			want: seqsig.ErrUnauthorizedSigner,
		},
		{
			name: "tampered block hash",
			env: func() seqsig.Envelope {
				e := signed(t, headOf(b7), nil)
				e.Head.BlockHash = other
				return e
			},
			want: seqsig.ErrUnauthorizedSigner,
		},
		{
			name: "tampered parent hash",
			env: func() seqsig.Envelope {
				e := signed(t, headOf(b7), nil)
				e.Head.ParentHash = other
				return e
			},
			want: seqsig.ErrUnauthorizedSigner,
		},
		{
			name: "tampered epoch",
			env: func() seqsig.Envelope {
				e := signed(t, headOf(b7), nil)
				e.Head.EpochMs++
				return e
			},
			want: seqsig.ErrUnauthorizedSigner,
		},
		{
			name: "tampered counter",
			env: func() seqsig.Envelope {
				e := signed(t, headOf(b7), nil)
				e.Head.Counter++
				return e
			},
			want: seqsig.ErrUnauthorizedSigner,
		},
		{
			name: "short signature",
			env: func() seqsig.Envelope {
				e := signed(t, headOf(b7), nil)
				e.Signature = e.Signature[:64]
				return e
			},
			want: seqsig.ErrBadSignature,
		},
		{
			name: "long signature",
			env: func() seqsig.Envelope {
				e := signed(t, headOf(b7), nil)
				e.Signature = append(e.Signature, 0)
				return e
			},
			want: seqsig.ErrBadSignature,
		},
		{
			name: "wrong rollup, checked before the signature",
			env: func() seqsig.Envelope {
				e := signed(t, headOf(b7), nil)
				e.Head.RollupID = 2
				e.Signature = e.Signature[:64]
				return e
			},
			want: seqsig.ErrWrongRollup,
		},
		{
			name: "zero r",
			env: func() seqsig.Envelope {
				e := signed(t, headOf(b7), nil)
				clear(e.Signature[:32])
				return e
			},
			want: seqsig.ErrBadSignature,
		},
		{
			name: "s at the curve order",
			env: func() seqsig.Envelope {
				e := signed(t, headOf(b7), nil)
				secp256k1N.WriteToSlice(e.Signature[32:64])
				return e
			},
			want: seqsig.ErrBadSignature,
		},
		{
			name: "block of another number",
			env:  func() seqsig.Envelope { return signed(t, headOf(b7), encodeBlock(t, b8)) },
			want: seqsig.ErrBlockMismatch,
		},
		{
			name: "head with another parent",
			env: func() seqsig.Envelope {
				h := headOf(b7)
				h.ParentHash = other
				return signed(t, h, encodeBlock(t, b7))
			},
			want: seqsig.ErrBlockMismatch,
		},
		{
			name: "head with another number",
			env: func() seqsig.Envelope {
				h := headOf(b7)
				h.Number = 8
				return signed(t, h, encodeBlock(t, b7))
			},
			want: seqsig.ErrBlockMismatch,
		},
		{
			name: "body not matching its header",
			env:  func() seqsig.Envelope { return signed(t, headOf(b7), withOmmer) },
			want: seqsig.ErrBodyMismatch,
		},
		{
			name: "block with trailing bytes",
			env: func() seqsig.Envelope {
				return signed(t, headOf(b7), append(encodeBlock(t, b7), 0x80))
			},
			want: seqsig.ErrBlockDecode,
		},
		{
			name: "undecodable block",
			env:  func() seqsig.Envelope { return signed(t, headOf(b7), []byte{0xc0, 1, 2, 3}) },
			want: seqsig.ErrBlockDecode,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := domain
			if c.domain != (seqsig.Domain{}) {
				d = c.domain
			}
			a := authorized
			if c.authorized != (common.Address{}) {
				a = c.authorized
			}
			v, err := c.env().Verify(d, a)
			require.ErrorIs(t, err, c.want)
			require.Nil(t, v)
		})
	}
}

// TestRecoverEncodings pins the signature encodings that recover the signer:
// parity 0/1, 27/28 or EIP-155 (odd v parity 0, even v parity 1), and the
// high-s twin of a signature.
func TestRecoverEncodings(t *testing.T) {
	h := headOf(testBlock(7))
	sig := common.FromHex(signatureHex)
	parity := sig[64] - 27
	authorized := common.HexToAddress(signerHex)

	highS := slices.Clone(sig)
	s := new(uint256.Int).SetBytes(sig[32:64])
	new(uint256.Int).Sub(secp256k1N, s).WriteToSlice(highS[32:64])
	highS[64] = 27 + (parity ^ 1)
	require.Equal(t, byte(27), highS[64], "the twin has parity 0")

	const (
		signer = iota
		otherSigner
		badSignature
	)
	for _, c := range []struct {
		name string
		sig  []byte
		want int
	}{
		{"v 27/28", sig, signer},
		{"v 0/1", withV(sig, parity), signer},
		{"v EIP-155 35/36", withV(sig, 35+parity), signer},
		{"v EIP-155 37/38", withV(sig, 37+parity), signer},
		{"high s", highS, signer},
		{"high s, v 255", withV(highS, 255), signer},
		{"other parity", withV(sig, 27+(parity^1)), otherSigner},
		{"v 2", withV(sig, 2), badSignature},
		{"v 26", withV(sig, 26), badSignature},
		{"v 29", withV(sig, 29), badSignature},
		{"v 34", withV(sig, 34), badSignature},
	} {
		got, err := seqsig.Recover(h, domain, c.sig)
		switch c.want {
		case signer:
			require.NoError(t, err, c.name)
			require.Equal(t, authorized, got, c.name)
		case otherSigner:
			require.NoError(t, err, c.name)
			require.NotEqual(t, authorized, got, c.name)
		case badSignature:
			require.ErrorIs(t, err, seqsig.ErrBadSignature, c.name)
		}
	}
}

func withV(sig []byte, v byte) []byte {
	out := bytes.Clone(sig)
	out[64] = v
	return out
}

func TestSignProducesLowS(t *testing.T) {
	halfN := new(uint256.Int).Rsh(secp256k1N, 1)
	for counter := range uint64(32) {
		h := headOf(testBlock(7))
		h.Counter = counter + 1
		sig, err := seqsig.Sign(h, domain, key(t))
		require.NoError(t, err)
		require.Len(t, sig, seqsig.SignatureLength)
		require.Contains(t, []byte{27, 28}, sig[64])
		require.False(t, new(uint256.Int).SetBytes(sig[32:64]).Gt(halfN))
		digest := h.SigningHash(domain)
		pub, err := crypto.SigToPub(digest[:], withV(sig, sig[64]-27))
		require.NoError(t, err)
		require.Equal(t, common.HexToAddress(signerHex), crypto.PubkeyToAddress(*pub))
	}
}
