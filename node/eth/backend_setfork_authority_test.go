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

package eth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/node/components/snapshotauth"
)

func mustKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := crypto.GenerateKey()
	require.NoError(t, err)
	return k
}

func compressedPub(k *ecdsa.PrivateKey) []byte {
	return elliptic.MarshalCompressed(k.PublicKey.Curve, k.PublicKey.X, k.PublicKey.Y)
}

func mintTransitionUCAN(t *testing.T, signingKey *ecdsa.PrivateKey, audience *ecdsa.PublicKey, targetChain string, notBefore, expires time.Time) string {
	t.Helper()
	cap, err := snapshotauth.ForkTransitionCapability(targetChain)
	require.NoError(t, err)
	d, err := snapshotauth.New(&signingKey.PublicKey, audience, []string{cap}, notBefore, expires, 0, nil)
	require.NoError(t, err)
	require.NoError(t, d.Sign(signingKey))
	enc, err := d.Encode()
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(enc)
}

func operatorRoots(t *testing.T, keys ...*ecdsa.PrivateKey) []snapshotauth.TrustRoot {
	t.Helper()
	out := make([]snapshotauth.TrustRoot, len(keys))
	for i, k := range keys {
		out[i] = snapshotauth.TrustRoot{Kind: snapshotauth.RootENR, Pubkey: compressedPub(k)}
	}
	return out
}

// TestVerifyForkTransition_SelfIssuedRootAccepted pins the happy path:
// a UCAN signed by an operator-accepted trust root, addressed to
// itself (self-issued), carrying the correct fork:transition:<name>
// capability, verifies cleanly.
func TestVerifyForkTransition_SelfIssuedRootAccepted(t *testing.T) {
	root := mustKey(t)
	now := time.Now()
	ucan := mintTransitionUCAN(t, root, &root.PublicKey, "hoodi-fork-42", now, now.Add(time.Hour))
	require.NoError(t, verifyForkTransition("hoodi-fork-42", ucan, operatorRoots(t, root), now))
}

// TestVerifyForkTransition_EmptyUCANRejected — the RPC edge check.
func TestVerifyForkTransition_EmptyUCANRejected(t *testing.T) {
	root := mustKey(t)
	err := verifyForkTransition("hoodi-fork-42", "", operatorRoots(t, root), time.Now())
	require.ErrorContains(t, err, "authority_rejected")
	require.ErrorContains(t, err, "required")
}

// TestVerifyForkTransition_EmptyTrustRootsRejected — an operator with
// no configured trust roots must not accept any transition UCAN.
func TestVerifyForkTransition_EmptyTrustRootsRejected(t *testing.T) {
	root := mustKey(t)
	now := time.Now()
	ucan := mintTransitionUCAN(t, root, &root.PublicKey, "hoodi-fork-42", now, now.Add(time.Hour))
	err := verifyForkTransition("hoodi-fork-42", ucan, nil, now)
	require.ErrorContains(t, err, "no snapshot trust roots configured")
}

// TestVerifyForkTransition_WrongRootRejected — UCAN signed by a key
// the operator does not accept.
func TestVerifyForkTransition_WrongRootRejected(t *testing.T) {
	attackerKey := mustKey(t)
	acceptedRoot := mustKey(t)
	now := time.Now()
	ucan := mintTransitionUCAN(t, attackerKey, &attackerKey.PublicKey, "hoodi-fork-42", now, now.Add(time.Hour))
	err := verifyForkTransition("hoodi-fork-42", ucan, operatorRoots(t, acceptedRoot), now)
	require.ErrorContains(t, err, "authority_rejected")
}

// TestVerifyForkTransition_WrongTargetChainRejected — a UCAN issued
// for fork A must not authorise a transition to fork B.
func TestVerifyForkTransition_WrongTargetChainRejected(t *testing.T) {
	root := mustKey(t)
	now := time.Now()
	ucanForA := mintTransitionUCAN(t, root, &root.PublicKey, "hoodi-fork-A", now, now.Add(time.Hour))
	err := verifyForkTransition("hoodi-fork-B", ucanForA, operatorRoots(t, root), now)
	require.ErrorContains(t, err, "authority_rejected")
	require.ErrorContains(t, err, "missing required capability")
}

// TestVerifyForkTransition_NotSelfIssuedRejected — a UCAN whose
// audience differs from its issuer fails the Phase 1 self-issue
// check even when signature + capability + trust root are all fine.
func TestVerifyForkTransition_NotSelfIssuedRejected(t *testing.T) {
	root := mustKey(t)
	otherAudience := mustKey(t)
	now := time.Now()
	ucan := mintTransitionUCAN(t, root, &otherAudience.PublicKey, "hoodi-fork-42", now, now.Add(time.Hour))
	err := verifyForkTransition("hoodi-fork-42", ucan, operatorRoots(t, root), now)
	require.ErrorContains(t, err, "self-issued")
}

// TestVerifyForkTransition_ExpiredUCANRejected — time-window
// enforcement is delegated to Verifier; pinning it here so future
// changes to the verifier don't silently loosen the RPC edge.
func TestVerifyForkTransition_ExpiredUCANRejected(t *testing.T) {
	root := mustKey(t)
	past := time.Now().Add(-2 * time.Hour)
	ucan := mintTransitionUCAN(t, root, &root.PublicKey, "hoodi-fork-42", past, past.Add(time.Hour))
	err := verifyForkTransition("hoodi-fork-42", ucan, operatorRoots(t, root), time.Now())
	require.ErrorContains(t, err, "authority_rejected")
}

// TestVerifyForkTransition_MalformedBase64Rejected — the wire is a
// base64 string; junk bytes should surface at the RPC edge, not
// deeper in the CBOR decoder.
func TestVerifyForkTransition_MalformedBase64Rejected(t *testing.T) {
	root := mustKey(t)
	err := verifyForkTransition("hoodi-fork-42", "not-valid-base64!!!", operatorRoots(t, root), time.Now())
	require.ErrorContains(t, err, "not valid base64")
}

// TestVerifyForkTransition_EmptyTargetChainRejected — belt-and-braces:
// the RPC handler should never reach here with an empty target, but
// the pure function must still refuse rather than accept a UCAN
// carrying "fork:transition:" (empty name) as a match.
func TestVerifyForkTransition_EmptyTargetChainRejected(t *testing.T) {
	root := mustKey(t)
	now := time.Now()
	ucan := mintTransitionUCAN(t, root, &root.PublicKey, "hoodi-fork-42", now, now.Add(time.Hour))
	err := verifyForkTransition("", ucan, operatorRoots(t, root), now)
	require.ErrorContains(t, err, "target chain name is required")
}
