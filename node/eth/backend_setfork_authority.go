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
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/node/components/snapshotauth"
)

// verifyForkTransitionAuthority gates debug_setFork on a caller-
// presented UCAN. Method wrapper that resolves the operator trust
// roots from config, then delegates to the pure package-level
// verifyForkTransition (unit-tested directly).
func (s *Ethereum) verifyForkTransitionAuthority(targetChainName, authorityUCAN string) error {
	roots, err := s.resolveOperatorTrustRoots()
	if err != nil {
		return fmt.Errorf("authority_rejected: resolve trust roots: %w", err)
	}
	return verifyForkTransition(targetChainName, authorityUCAN, roots, time.Now())
}

// verifyForkTransition is the pure verification: Phase 1 constraints
// are that the leaf UCAN must be root-signed (no delegation cascade),
// self-issued (issuer == audience), carry fork:transition:<name>, and
// pass signature + time-window against `roots`. An empty root set is
// refused (there is deliberately no "any" opt-out: an operator who
// left the snapshot side wide open shouldn't have an unauthenticated
// fork-transition surface).
func verifyForkTransition(targetChainName, authorityUCAN string, roots []snapshotauth.TrustRoot, now time.Time) error {
	if strings.TrimSpace(authorityUCAN) == "" {
		return errors.New("authority_rejected: authority UCAN is required")
	}
	if strings.TrimSpace(targetChainName) == "" {
		return errors.New("authority_rejected: target chain name is required")
	}
	if len(roots) == 0 {
		return errors.New("authority_rejected: node has no snapshot trust roots configured; fork transitions require an operator-attested trust set")
	}

	leafCBOR, err := base64.StdEncoding.DecodeString(authorityUCAN)
	if err != nil {
		return fmt.Errorf("authority_rejected: authority UCAN is not valid base64: %w", err)
	}
	leaf, err := snapshotauth.Decode(leafCBOR)
	if err != nil {
		return fmt.Errorf("authority_rejected: decode leaf: %w", err)
	}
	if len(leaf.ParentHash) != 0 {
		return errors.New("authority_rejected: fork-transition UCAN must be root-signed (no delegation cascade in Phase 1)")
	}
	if !bytes.Equal(leaf.Issuer, leaf.Audience) {
		return errors.New("authority_rejected: fork-transition UCAN must be self-issued (issuer == audience)")
	}

	requiredCap, err := snapshotauth.ForkTransitionCapability(targetChainName)
	if err != nil {
		return fmt.Errorf("authority_rejected: build required capability: %w", err)
	}

	verifier := snapshotauth.NewVerifier(roots)
	if _, err := verifier.Verify(leafCBOR, leaf.Audience, []string{requiredCap}, now, nil); err != nil {
		return fmt.Errorf("authority_rejected: %w", err)
	}
	return nil
}

// resolveOperatorTrustRoots mirrors the parse block in Ethereum.New —
// compiled-in per-chain default, overridden by --snapshot.trust-roots.
// Called each SetFork invocation (rare) so no need to cache.
func (s *Ethereum) resolveOperatorTrustRoots() ([]snapshotauth.TrustRoot, error) {
	var chainName string
	if s.chainConfig != nil {
		chainName = s.chainConfig.ChainName
	}
	spec := snapcfg.GetEmbeddedTrustRoots(chainName)
	if s.config != nil {
		if override := strings.TrimSpace(s.config.Snapshot.TrustRoots); override != "" {
			spec = override
		}
	}
	if spec == "" || strings.EqualFold(spec, "any") {
		return nil, nil
	}
	return snapshotauth.ParseTrustRoots(spec)
}
