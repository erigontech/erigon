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

package snapshotsync

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"

	"github.com/erigontech/erigon/common/crypto"
)

// AdvertisementSignatureSize is the byte length of a sidecar
// signature: 64 bytes [R || S], no recovery byte.
//
// We use the no-recovery form because the verifier already has the
// public key from the peer's ENR — recovery is unnecessary, and
// omitting it makes the signature unambiguously verifiable against
// a specific known key rather than ambiguously recoverable to any
// of two candidates.
const AdvertisementSignatureSize = 64

// ErrAdvertisementSignatureInvalid is returned by VerifyAdvertisement
// when signature verification fails for any reason (bad length, wrong
// key, tampered content). Callers should treat this as wholesale
// rejection of the advertisement file per the three-layer model
// (docs/plans/20260515-three-layer-snapshot-distribution.md): the
// peer's manifest is suspect regardless of which specific entry would
// have validated.
var ErrAdvertisementSignatureInvalid = errors.New("advertisement signature invalid")

// SignAdvertisement returns a 64-byte secp256k1 ECDSA signature over
// SHA256(data) using the node's private key.
//
// The signature is the sidecar payload stored as chain.<enr>.sig
// next to the chain.<enr>.toml data file. Same secp256k1 key as the
// ENR uses for discv5 / sentry handshakes — no separate key
// management.
//
// Recovery byte is stripped: verifiers always have the public key
// from the peer's ENR, so the recovery info is unnecessary.
func SignAdvertisement(data []byte, priv *ecdsa.PrivateKey) ([]byte, error) {
	if priv == nil {
		return nil, errors.New("nil private key")
	}
	digest := sha256.Sum256(data)
	sig, err := crypto.Sign(digest[:], priv)
	if err != nil {
		return nil, err
	}
	// crypto.Sign returns 65 bytes [R || S || V]; strip recovery byte.
	return sig[:AdvertisementSignatureSize], nil
}

// VerifyAdvertisement checks that sig is a valid secp256k1 ECDSA
// signature over SHA256(data) for the public key in pubkey.
//
// pubkey accepts both compressed (33 bytes) and uncompressed (65
// bytes) secp256k1 encodings — see crypto.VerifySignature for the
// supported formats. ENRs carry the compressed form natively.
//
// Returns ErrAdvertisementSignatureInvalid on any failure (wrong
// length, wrong key, modified data). Per the three-layer model this
// failure must trigger wholesale rejection of the chain.<enr>.toml
// — partial trust isn't meaningful once authenticity is broken.
func VerifyAdvertisement(data, sig, pubkey []byte) error {
	if len(sig) != AdvertisementSignatureSize {
		return ErrAdvertisementSignatureInvalid
	}
	if len(pubkey) != 33 && len(pubkey) != 65 {
		return ErrAdvertisementSignatureInvalid
	}
	digest := sha256.Sum256(data)
	if !crypto.VerifySignature(pubkey, digest[:], sig) {
		return ErrAdvertisementSignatureInvalid
	}
	return nil
}
