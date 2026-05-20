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

package snapshotauth

import (
	"crypto/ecdsa"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
)

func TestDelegation_RoundTrip(t *testing.T) {
	issuer := newKey(t)
	audience := newKey(t)

	d, err := New(
		&issuer.PublicKey, &audience.PublicKey,
		[]string{string(CapAdvertise), string(CapServe)},
		time.Time{},              // NotBefore zero
		time.Unix(1893456000, 0), // 2030-01-01
		3,
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, d.Sign(issuer))

	encoded, err := d.Encode()
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	got, err := Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, d.Version, got.Version)
	require.Equal(t, d.Issuer, got.Issuer)
	require.Equal(t, d.Audience, got.Audience)
	require.Equal(t, d.Capabilities, got.Capabilities)
	require.Equal(t, d.NotBefore, got.NotBefore)
	require.Equal(t, d.Expires, got.Expires)
	require.Equal(t, d.DepthCap, got.DepthCap)
	require.Equal(t, d.Signature, got.Signature)
	require.NoError(t, got.VerifySignature())
}

func TestDelegation_SigningKeyMustMatchIssuer(t *testing.T) {
	issuer := newKey(t)
	other := newKey(t)
	audience := newKey(t)

	d, err := New(&issuer.PublicKey, &audience.PublicKey,
		[]string{string(CapAdvertise)}, time.Time{}, time.Time{}, 1, nil)
	require.NoError(t, err)

	err = d.Sign(other)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match Issuer")
}

func TestDelegation_TamperedFieldsFailVerification(t *testing.T) {
	issuer := newKey(t)
	audience := newKey(t)

	d, err := New(&issuer.PublicKey, &audience.PublicKey,
		[]string{string(CapServe)}, time.Time{}, time.Time{}, 0, nil)
	require.NoError(t, err)
	require.NoError(t, d.Sign(issuer))

	// Sanity: untampered verifies.
	require.NoError(t, d.VerifySignature())

	// Tamper with capabilities. Must fail.
	tampered := *d
	tampered.Capabilities = []string{string(CapAdvertise), string(CapServe)}
	require.Error(t, tampered.VerifySignature())

	// Tamper with audience. Must fail.
	tampered2 := *d
	tampered2.Audience = make([]byte, PubKeyLen) // zero pubkey
	require.Error(t, tampered2.VerifySignature())

	// Tamper with expires. Must fail.
	tampered3 := *d
	tampered3.Expires = 9999999999
	require.Error(t, tampered3.VerifySignature())
}

func TestDelegation_CanonicalEncodingIsDeterministic(t *testing.T) {
	issuer := newKey(t)
	audience := newKey(t)

	// Capabilities supplied in different orders + with duplicates;
	// canonical form must collapse to the same bytes.
	a, err := New(&issuer.PublicKey, &audience.PublicKey,
		[]string{string(CapServe), string(CapAdvertise), string(CapServe)},
		time.Time{}, time.Time{}, 2, nil)
	require.NoError(t, err)
	require.NoError(t, a.Sign(issuer))

	b, err := New(&issuer.PublicKey, &audience.PublicKey,
		[]string{string(CapAdvertise), string(CapServe)},
		time.Time{}, time.Time{}, 2, nil)
	require.NoError(t, err)
	require.NoError(t, b.Sign(issuer))

	encA, err := a.Encode()
	require.NoError(t, err)
	encB, err := b.Encode()
	require.NoError(t, err)
	// Signatures over the same canonical payload are deterministic
	// only if the digest is identical — so encoded bytes must match.
	// (Note: ECDSA produces non-deterministic signatures by default,
	// so it's the digest that must match; we check the unsigned form.)
	a.Signature = nil
	b.Signature = nil
	encAUnsigned, err := a.Encode()
	require.NoError(t, err)
	encBUnsigned, err := b.Encode()
	require.NoError(t, err)
	require.Equal(t, encAUnsigned, encBUnsigned,
		"capabilities supplied in different orders must produce identical canonical encoding")
	// Both signed forms verify regardless of ECDSA signature variance.
	signedA, err := Decode(encA)
	require.NoError(t, err)
	require.NoError(t, signedA.VerifySignature())
	signedB, err := Decode(encB)
	require.NoError(t, err)
	require.NoError(t, signedB.VerifySignature())
}

func TestDelegation_ParentChainNested(t *testing.T) {
	root := newKey(t)
	mid := newKey(t)
	leaf := newKey(t)

	rootDel, err := New(&root.PublicKey, &mid.PublicKey,
		[]string{string(CapAdvertise), string(CapServe), string(CapDelegate)},
		time.Time{}, time.Time{}, 3, nil)
	require.NoError(t, err)
	require.NoError(t, rootDel.Sign(root))
	rootEnc, err := rootDel.Encode()
	require.NoError(t, err)

	midDel, err := New(&mid.PublicKey, &leaf.PublicKey,
		[]string{string(CapAdvertise)},
		time.Time{}, time.Time{}, 2, rootEnc)
	require.NoError(t, err)
	require.NoError(t, midDel.Sign(mid))
	midEnc, err := midDel.Encode()
	require.NoError(t, err)

	// Decoded mid carries the parent's hash; the parent itself is
	// resolved out-of-band and bound back via HashOf.
	decodedMid, err := Decode(midEnc)
	require.NoError(t, err)
	require.NoError(t, decodedMid.VerifySignature())
	require.NotEmpty(t, decodedMid.ParentHash)
	require.Equal(t, HashOf(rootEnc), decodedMid.ParentHash,
		"child ParentHash must be sha256 of the parent's canonical CBOR")

	decodedRoot, err := Decode(rootEnc)
	require.NoError(t, err)
	require.NoError(t, decodedRoot.VerifySignature())

	// Audience-of-parent matches Issuer-of-child — the chain link.
	require.Equal(t, decodedRoot.Audience, decodedMid.Issuer)
}

func TestDelegation_RejectsEmptyCapabilities(t *testing.T) {
	issuer := newKey(t)
	audience := newKey(t)

	_, err := New(&issuer.PublicKey, &audience.PublicKey,
		[]string{}, time.Time{}, time.Time{}, 0, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "at least one capability")
}

func TestParseCapabilities(t *testing.T) {
	cases := []struct {
		in      string
		want    []string
		wantErr bool
	}{
		{"snapshot:advertise", []string{"snapshot:advertise"}, false},
		{"snapshot:serve, snapshot:advertise", []string{"snapshot:advertise", "snapshot:serve"}, false},
		{"snapshot:advertise,snapshot:advertise", []string{"snapshot:advertise"}, false},
		{"", nil, true},
		{"   ", nil, true},
		{"snapshot:bogus", nil, true},
		{"snapshot:serve,bogus", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseCapabilities(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := crypto.GenerateKey()
	require.NoError(t, err)
	return k
}
