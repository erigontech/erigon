// Copyright 2024 The Erigon Authors
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

package consensus_tests

import (
	"fmt"
	"io/fs"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/spectest/spectest"
	"github.com/erigontech/erigon/cl/utils/bls"
)

// gossipMeta represents the meta.yaml structure for gossip networking tests.
type gossipMeta struct {
	Topic    string          `yaml:"topic"`
	Messages []gossipMessage `yaml:"messages"`
}

type gossipMessage struct {
	Message  string `yaml:"message"`
	Expected string `yaml:"expected"`
	Reason   string `yaml:"reason"`
}

func gossipAttesterSlashingHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	beaconState, err := spectest.ReadBeaconState(root, c.Version(), "state.ssz_snappy")
	if err != nil {
		return fmt.Errorf("failed to read beacon state: %w", err)
	}

	var meta gossipMeta
	if err := spectest.ReadMeta(root, "meta.yaml", &meta); err != nil {
		return fmt.Errorf("failed to read meta.yaml: %w", err)
	}

	// Track seen slashable validator index sets.
	seenIndices := make(map[uint64]bool)

	for i, msg := range meta.Messages {
		// Read the attester slashing SSZ file.
		slashing := cltypes.NewAttesterSlashing(c.Version())
		filename := msg.Message + ".ssz_snappy"
		if err := spectest.ReadSszOld(root, slashing, c.Version(), filename); err != nil {
			return fmt.Errorf("message %d: failed to read slashing %s: %w", i, filename, err)
		}

		result := validateAttesterSlashing(beaconState, slashing, seenIndices)

		if result != msg.Expected {
			return fmt.Errorf("message %d (%s): expected %q but got %q",
				i, msg.Message, msg.Expected, result)
		}

		// If valid, mark the slashable indices as seen.
		if result == "valid" {
			markAttesterSlashingSeen(beaconState, slashing, seenIndices)
		}
	}
	return nil
}

// validateAttesterSlashing performs gossip-level validation of an attester slashing.
// Returns "valid", "reject", or "ignore".
func validateAttesterSlashing(
	s *state.CachingBeaconState,
	slashing *cltypes.AttesterSlashing,
	seenIndices map[uint64]bool,
) string {
	att1 := slashing.Attestation_1
	att2 := slashing.Attestation_2

	// 1. Check that the attestation data is slashable.
	if !cltypes.IsSlashableAttestationData(att1.Data, att2.Data) {
		return "reject"
	}

	// 2. Compute the intersection of attesting indices (before full validation).
	// We use raw indices here; full validation happens later.
	intersection := solid.IntersectionOfSortedSets(
		solid.IterableSSZ[uint64](att1.AttestingIndices),
		solid.IterableSSZ[uint64](att2.AttestingIndices),
	)

	// 3. IGNORE if the intersection is empty or all intersection indices are
	// already in our seen cache (i.e., we've already processed a slashing
	// covering those indices).
	if len(intersection) == 0 {
		return "ignore"
	}
	allSeen := true
	for _, idx := range intersection {
		if !seenIndices[idx] {
			allSeen = false
			break
		}
	}
	if allSeen {
		return "ignore"
	}

	// 4. Full validation: indexed attestation validity (sorted, non-empty, BLS signatures).
	valid, err := state.IsValidIndexedAttestation(s, att1)
	if err != nil || !valid {
		return "reject"
	}
	valid, err = state.IsValidIndexedAttestation(s, att2)
	if err != nil || !valid {
		return "reject"
	}

	// 5. Check that process_attester_slashing would succeed:
	// at least one validator in the intersection must be slashable.
	currentEpoch := state.GetEpochAtSlot(s.BeaconConfig(), s.Slot())
	hasSlashable := false
	for _, idx := range intersection {
		validator, err := s.ValidatorForValidatorIndex(int(idx))
		if err != nil {
			return "reject"
		}
		if validator.IsSlashable(currentEpoch) {
			hasSlashable = true
			break
		}
	}
	if !hasSlashable {
		return "reject"
	}

	return "valid"
}

// markAttesterSlashingSeen adds the slashable indices from the intersection to the seen set.
func markAttesterSlashingSeen(
	s *state.CachingBeaconState,
	slashing *cltypes.AttesterSlashing,
	seenIndices map[uint64]bool,
) {
	intersection := solid.IntersectionOfSortedSets(
		solid.IterableSSZ[uint64](slashing.Attestation_1.AttestingIndices),
		solid.IterableSSZ[uint64](slashing.Attestation_2.AttestingIndices),
	)
	for _, idx := range intersection {
		seenIndices[idx] = true
	}
}

func gossipProposerSlashingHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	beaconState, err := spectest.ReadBeaconState(root, c.Version(), "state.ssz_snappy")
	if err != nil {
		return fmt.Errorf("failed to read beacon state: %w", err)
	}

	var meta gossipMeta
	if err := spectest.ReadMeta(root, "meta.yaml", &meta); err != nil {
		return fmt.Errorf("failed to read meta.yaml: %w", err)
	}

	// Track seen proposer indices.
	seenProposers := make(map[uint64]bool)

	for i, msg := range meta.Messages {
		slashing := &cltypes.ProposerSlashing{}
		filename := msg.Message + ".ssz_snappy"
		if err := spectest.ReadSszOld(root, slashing, c.Version(), filename); err != nil {
			return fmt.Errorf("message %d: failed to read slashing %s: %w", i, filename, err)
		}

		result := validateProposerSlashing(beaconState, slashing, seenProposers)

		if result != msg.Expected {
			return fmt.Errorf("message %d (%s): expected %q but got %q",
				i, msg.Message, msg.Expected, result)
		}

		// If valid, mark the proposer index as seen.
		if result == "valid" {
			seenProposers[slashing.Header1.Header.ProposerIndex] = true
		}
	}
	return nil
}

// validateProposerSlashing performs gossip-level validation of a proposer slashing.
// Returns "valid", "reject", or "ignore".
func validateProposerSlashing(
	s *state.CachingBeaconState,
	slashing *cltypes.ProposerSlashing,
	seenProposers map[uint64]bool,
) string {
	h1 := slashing.Header1.Header
	h2 := slashing.Header2.Header

	// 1. Check slots match.
	if h1.Slot != h2.Slot {
		return "reject"
	}

	// 2. Check proposer indices match.
	if h1.ProposerIndex != h2.ProposerIndex {
		return "reject"
	}

	// 3. Check headers are different.
	if *h1 == *h2 {
		return "reject"
	}

	// 4. Check if already seen.
	if seenProposers[h1.ProposerIndex] {
		return "ignore"
	}

	// 5. Check proposer exists and is slashable.
	proposer, err := s.ValidatorForValidatorIndex(int(h1.ProposerIndex))
	if err != nil {
		return "reject"
	}
	if !proposer.IsSlashable(state.Epoch(s)) {
		return "reject"
	}

	// 6. Verify both signatures.
	for _, signedHeader := range []*cltypes.SignedBeaconBlockHeader{slashing.Header1, slashing.Header2} {
		domain, err := s.GetDomain(
			s.BeaconConfig().DomainBeaconProposer,
			state.GetEpochAtSlot(s.BeaconConfig(), signedHeader.Header.Slot),
		)
		if err != nil {
			return "reject"
		}
		signingRoot, err := fork.ComputeSigningRoot(signedHeader.Header, domain)
		if err != nil {
			return "reject"
		}
		pk := proposer.PublicKey()
		valid, err := bls.Verify(signedHeader.Signature[:], signingRoot[:], pk[:])
		if err != nil || !valid {
			return "reject"
		}
	}

	return "valid"
}
