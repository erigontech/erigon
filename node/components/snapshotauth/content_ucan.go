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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// ContentUCANValidity is how long a minted Content UCAN stays valid
// from the moment it is minted. The Content UCAN is short-lived
// relative to the months-long Authority UCAN: each Publish supersedes
// the previous generation's Content UCAN, and the consumer's per-peer
// trust cache TTL is bounded by min(Content.Expires, Authority.Expires)
// (docs/plans/20260520-chaintoml-ucan-flow-spec.md). 24h comfortably
// covers the gap between regenerations at chain tip while keeping a
// leaked attestation's replay window short.
const ContentUCANValidity = 24 * time.Hour

// MintContentUCAN builds and signs a Content UCAN attesting that
// tomlBytes is the V2 manifest published by operatorKey's node. The
// returned canonical CBOR is persisted as the
// chain.v2.<enr-fp>.<seq>.ucan sidecar.
//
// The Content UCAN is self-issued (issuer == audience == operator
// pubkey) with the single capability chain.v2:hash:<sha256(tomlBytes)>
// and ParentHash referencing authorityEncoded (the operator's
// Authority UCAN). It is a leaf (DepthCap 0 — not re-delegable) and
// valid for ContentUCANValidity from now.
//
// authorityEncoded is the canonical CBOR of the Authority UCAN; New
// stores sha256(authorityEncoded) as ParentHash. A consumer resolves
// the parent by that hash and checks Authority.Audience == this UCAN's
// Issuer.
func MintContentUCAN(tomlBytes []byte, operatorKey *ecdsa.PrivateKey, authorityEncoded []byte, now time.Time) ([]byte, error) {
	if operatorKey == nil {
		return nil, fmt.Errorf("MintContentUCAN: nil operator key")
	}
	sum := sha256.Sum256(tomlBytes)
	capability := ContentHashCapability(hex.EncodeToString(sum[:]))
	d, err := New(
		&operatorKey.PublicKey, &operatorKey.PublicKey,
		[]string{capability},
		now, now.Add(ContentUCANValidity), 0, authorityEncoded,
	)
	if err != nil {
		return nil, fmt.Errorf("construct content UCAN: %w", err)
	}
	if err := d.Sign(operatorKey); err != nil {
		return nil, fmt.Errorf("sign content UCAN: %w", err)
	}
	return d.Encode()
}
