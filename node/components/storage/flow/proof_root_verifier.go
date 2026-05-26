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

package flow

import (
	"fmt"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// ProofRootVerifier re-verifies a downloaded snapshot file's content
// against the publisher's claimed ProofRoot. It runs after the file is
// fully downloaded and recorded as Local but BEFORE the orchestrator
// promotes the file to TrustVerified — a verification failure demotes
// the file back to TrustNone and suppresses the TrustPromoted event so
// downstream consumers never see a falsely-verified file.
//
// The interface accepts the freshly-downloaded FileEntry (which carries
// the manifest's advertised Anchors — ProofRoot, AtBlock, AtTxNum) and
// returns nil on a clean re-verification or an error describing the
// mismatch.
//
// The concrete verifier is wired by the storage component at startup;
// when no verifier is configured the orchestrator skips re-verification
// (existing behaviour, no regression).
type ProofRootVerifier interface {
	// VerifyProofRoot re-checks the file's content against entry.Anchors.
	// entry.Anchors.IsZero() is the caller's responsibility — the
	// orchestrator only calls Verify when the manifest carried an anchor
	// for this file.
	VerifyProofRoot(entry *snapshot.FileEntry) error
}

// HashProofVerifierStub is the Phase-1 placeholder verifier: it accepts
// every call and returns nil. Wired by default so the orchestrator hook
// is exercised end-to-end without falsely claiming we have a real
// proof scheme; the production segment-proof verifier (qmtree or PMT,
// undecided at Phase 1 close) replaces it once selected.
//
// The stub deliberately does NOT recompute a content hash and compare
// against ProofRoot — those values aren't bit-comparable (ProofRoot is
// a state-trie root, file content is a snapshot segment). A trivial
// "always pass" verifier is more honest about the gap than a
// content-hash check that would be cryptographically irrelevant to
// the state-trie claim. See plan
// .claude/plans/20260526-phase1-closing-plan.md § Group B item 4.
type HashProofVerifierStub struct{}

// VerifyProofRoot is the stub no-op.
func (HashProofVerifierStub) VerifyProofRoot(_ *snapshot.FileEntry) error { return nil }

// AlwaysFailVerifier is a test helper that rejects every verification;
// pairs with AlwaysPassVerifier in the orchestrator tests to pin the
// demotion path on mismatch.
type AlwaysFailVerifier struct{ Reason string }

// VerifyProofRoot always returns a mismatch error.
func (a AlwaysFailVerifier) VerifyProofRoot(e *snapshot.FileEntry) error {
	reason := a.Reason
	if reason == "" {
		reason = "test-injected mismatch"
	}
	if e == nil {
		return fmt.Errorf("proof-root verify failed: %s", reason)
	}
	return fmt.Errorf("proof-root verify failed for %s: %s", e.Name, reason)
}
