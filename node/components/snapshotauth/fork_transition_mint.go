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
	"fmt"
	"time"
)

// ForkTransitionUCANValidity is the default lifetime of a fork-
// transition UCAN. Short — a transition cap authorises a specific
// operational action (invoke debug_setFork), not a long-lived
// publishing capability like ForkAuthorityUCANValidity. Operators
// mint one per transition batch; expired caps must be reissued.
const ForkTransitionUCANValidity = 24 * time.Hour

// MintForkTransitionUCAN issues a UCAN authorising debug_setFork to
// transition a node onto targetChainName. The bearer must present
// this UCAN over the wire; the target node's verifier accepts iff
// (a) trustRootKey's pubkey is in its --snapshot.trust-roots set,
// (b) the leaf is self-issued (Phase 1: no delegation cascade), and
// (c) the capability's <name> matches the RPC's target chain name.
//
// trustRootKey signs the delegation and is embedded as both Issuer
// and Audience — Phase 1 constrains fork-transition UCANs to be
// self-issued so audience-substitution attacks are structurally
// impossible until per-operator audience wiring lands in Phase 2.
//
// notBefore/expires typically pass time.Now() and time.Now().Add(
// ForkTransitionUCANValidity); callers can shrink for tighter
// windows.
func MintForkTransitionUCAN(
	trustRootKey *ecdsa.PrivateKey,
	targetChainName string,
	notBefore, expires time.Time,
) ([]byte, error) {
	if trustRootKey == nil {
		return nil, fmt.Errorf("MintForkTransitionUCAN: nil trust root key")
	}
	cap, err := ForkTransitionCapability(targetChainName)
	if err != nil {
		return nil, fmt.Errorf("MintForkTransitionUCAN: %w", err)
	}
	d, err := New(
		&trustRootKey.PublicKey,
		&trustRootKey.PublicKey,
		[]string{cap},
		notBefore, expires,
		0,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("construct fork-transition UCAN: %w", err)
	}
	if err := d.Sign(trustRootKey); err != nil {
		return nil, fmt.Errorf("sign fork-transition UCAN: %w", err)
	}
	return d.Encode()
}
