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
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"slices"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
	"github.com/erigontech/erigon/cl/spectest/spectest"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
)

// gossipMeta represents the meta.yaml structure for gossip networking tests.
type gossipMeta struct {
	Topic         string          `yaml:"topic"`
	CurrentTimeMS uint64          `yaml:"current_time_ms"`
	Messages      []gossipMessage `yaml:"messages"`
}

type gossipMessage struct {
	OffsetMS uint64 `yaml:"offset_ms"`
	SubnetID uint64 `yaml:"subnet_id"`
	Message  string `yaml:"message"`
	Expected string `yaml:"expected"`
	Reason   string `yaml:"reason"`
}

func gossipBLSToExecutionChangeHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	beaconState, meta, err := readGossipStateAndMeta(root, c)
	if err != nil {
		return err
	}
	seen := make(map[uint64]struct{})
	for i, message := range meta.Messages {
		change := &cltypes.SignedBLSToExecutionChange{}
		if err := spectest.ReadSszOld(root, change, c.Version(), message.Message+".ssz_snappy"); err != nil {
			return err
		}
		result := validateGossipBLSToExecutionChange(beaconState, change, seen)
		if result != message.Expected {
			return gossipResultError(i, message, result)
		}
		if result == "valid" {
			seen[change.Message.ValidatorIndex] = struct{}{}
		}
	}
	return nil
}

func validateGossipBLSToExecutionChange(beaconState *state.CachingBeaconState, signedChange *cltypes.SignedBLSToExecutionChange, seen map[uint64]struct{}) string {
	if signedChange == nil || signedChange.Message == nil {
		return "reject"
	}
	change := signedChange.Message
	if _, ok := seen[change.ValidatorIndex]; ok {
		return "ignore"
	}
	validator, err := beaconState.ValidatorForValidatorIndex(int(change.ValidatorIndex))
	if err != nil {
		return "reject"
	}
	withdrawalCredentials := validator.WithdrawalCredentials()
	if withdrawalCredentials[0] != byte(beaconState.BeaconConfig().BLSWithdrawalPrefixByte) {
		return "reject"
	}
	hashedFrom := utils.Sha256(change.From[:])
	if !bytes.Equal(withdrawalCredentials[1:], hashedFrom[1:]) {
		return "reject"
	}
	domain, err := fork.ComputeDomain(
		beaconState.BeaconConfig().DomainBLSToExecutionChange[:],
		utils.Uint32ToBytes4(uint32(beaconState.BeaconConfig().GenesisForkVersion)),
		beaconState.GenesisValidatorsRoot(),
	)
	if err != nil {
		return "reject"
	}
	signingRoot, err := fork.ComputeSigningRoot(change, domain)
	if err != nil {
		return "reject"
	}
	valid, err := bls.Verify(signedChange.Signature[:], signingRoot[:], change.From[:])
	if err != nil || !valid {
		return "reject"
	}
	return "valid"
}

type syncCommitteeMessageKey struct {
	slot           uint64
	validatorIndex uint64
	subnetID       uint64
}

func gossipSyncCommitteeMessageHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	beaconState, meta, err := readGossipStateAndMeta(root, c)
	if err != nil {
		return err
	}
	seen := make(map[syncCommitteeMessageKey]struct{})
	for i, message := range meta.Messages {
		syncMessage := &cltypes.SyncCommitteeMessage{}
		if err := spectest.ReadSszOld(root, syncMessage, c.Version(), message.Message+".ssz_snappy"); err != nil {
			return err
		}
		result := validateGossipSyncCommitteeMessage(beaconState, syncMessage, message.SubnetID, meta.CurrentTimeMS+message.OffsetMS, seen)
		if result != message.Expected {
			return gossipResultError(i, message, result)
		}
		if result == "valid" {
			seen[syncCommitteeMessageKey{syncMessage.Slot, syncMessage.ValidatorIndex, message.SubnetID}] = struct{}{}
		}
	}
	return nil
}

func validateGossipSyncCommitteeMessage(
	beaconState *state.CachingBeaconState,
	message *cltypes.SyncCommitteeMessage,
	subnetID uint64,
	currentTimeMS uint64,
	seen map[syncCommitteeMessageKey]struct{},
) string {
	if message == nil || !gossipSlotIsCurrent(beaconState, message.Slot, currentTimeMS) {
		return "ignore"
	}
	if message.ValidatorIndex >= uint64(beaconState.ValidatorLength()) {
		return "reject"
	}
	validSubnets, err := subnets.ComputeSubnetsForSyncCommittee(beaconState, message.ValidatorIndex)
	if err != nil || !slices.Contains(validSubnets, subnetID) {
		return "reject"
	}
	key := syncCommitteeMessageKey{message.Slot, message.ValidatorIndex, subnetID}
	if _, ok := seen[key]; ok {
		return "ignore"
	}
	publicKey, err := beaconState.ValidatorPublicKey(int(message.ValidatorIndex))
	if err != nil {
		return "reject"
	}
	domain, err := beaconState.GetDomain(beaconState.BeaconConfig().DomainSyncCommittee, message.Slot/beaconState.BeaconConfig().SlotsPerEpoch)
	if err != nil {
		return "reject"
	}
	signingRoot := utils.Sha256(message.BeaconBlockRoot[:], domain)
	valid, err := bls.Verify(message.Signature[:], signingRoot[:], publicKey[:])
	if err != nil || !valid {
		return "reject"
	}
	return "valid"
}

type syncContributionKey struct {
	slot              uint64
	beaconBlockRoot   common.Hash
	subcommitteeIndex uint64
}

type syncContributionAggregatorKey struct {
	aggregatorIndex   uint64
	slot              uint64
	subcommitteeIndex uint64
}

func gossipSyncContributionHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	beaconState, meta, err := readGossipStateAndMeta(root, c)
	if err != nil {
		return err
	}
	seenContributions := make(map[syncContributionKey][][]byte)
	seenAggregators := make(map[syncContributionAggregatorKey]struct{})
	for i, message := range meta.Messages {
		contribution := &cltypes.Contribution{}
		contribution.SetAggregationBitsSize(int(beaconState.BeaconConfig().SyncCommitteeSize / beaconState.BeaconConfig().SyncCommitteeSubnetCount / 8))
		signedContribution := &cltypes.SignedContributionAndProof{Message: &cltypes.ContributionAndProof{Contribution: contribution}}
		if err := spectest.ReadSszOld(root, signedContribution, c.Version(), message.Message+".ssz_snappy"); err != nil {
			return err
		}
		result := validateGossipSyncContribution(beaconState, signedContribution, meta.CurrentTimeMS+message.OffsetMS, seenContributions, seenAggregators)
		if result != message.Expected {
			return gossipResultError(i, message, result)
		}
		if result == "valid" {
			markGossipSyncContributionSeen(signedContribution.Message, seenContributions, seenAggregators)
		}
	}
	return nil
}

func validateGossipSyncContribution(
	beaconState *state.CachingBeaconState,
	signedContribution *cltypes.SignedContributionAndProof,
	currentTimeMS uint64,
	seenContributions map[syncContributionKey][][]byte,
	seenAggregators map[syncContributionAggregatorKey]struct{},
) string {
	if signedContribution == nil || signedContribution.Message == nil || signedContribution.Message.Contribution == nil {
		return "reject"
	}
	message := signedContribution.Message
	contribution := message.Contribution
	if !gossipSlotIsCurrent(beaconState, contribution.Slot, currentTimeMS) {
		return "ignore"
	}
	if contribution.SubcommitteeIndex >= beaconState.BeaconConfig().SyncCommitteeSubnetCount {
		return "reject"
	}
	if !gossipBitsHaveParticipants(contribution.AggregationBits) {
		return "reject"
	}
	modulo := max(uint64(1), beaconState.BeaconConfig().SyncCommitteeSize/beaconState.BeaconConfig().SyncCommitteeSubnetCount/beaconState.BeaconConfig().TargetAggregatorsPerSyncSubcommittee)
	selectionProofHash := utils.Sha256(message.SelectionProof[:])
	if binary.LittleEndian.Uint64(selectionProofHash[:8])%modulo != 0 {
		return "reject"
	}
	if message.AggregatorIndex >= uint64(beaconState.ValidatorLength()) {
		return "reject"
	}
	subcommitteePublicKeys := gossipSyncSubcommitteePublicKeys(beaconState, contribution.SubcommitteeIndex)
	aggregatorPublicKey, err := beaconState.ValidatorPublicKey(int(message.AggregatorIndex))
	if err != nil || !slices.Contains(subcommitteePublicKeys, aggregatorPublicKey) {
		return "reject"
	}
	contributionKey := syncContributionKey{contribution.Slot, contribution.BeaconBlockRoot, contribution.SubcommitteeIndex}
	for _, seenBits := range seenContributions[contributionKey] {
		if gossipBitsSuperset(seenBits, contribution.AggregationBits) {
			return "ignore"
		}
	}
	aggregatorKey := syncContributionAggregatorKey{message.AggregatorIndex, contribution.Slot, contribution.SubcommitteeIndex}
	if _, ok := seenAggregators[aggregatorKey]; ok {
		return "ignore"
	}
	selectionData := &cltypes.SyncAggregatorSelectionData{Slot: contribution.Slot, SubcommitteeIndex: contribution.SubcommitteeIndex}
	domain, err := beaconState.GetDomain(beaconState.BeaconConfig().DomainSyncCommitteeSelectionProof, contribution.Slot/beaconState.BeaconConfig().SlotsPerEpoch)
	if err != nil {
		return "reject"
	}
	signingRoot, err := fork.ComputeSigningRoot(selectionData, domain)
	if err != nil || !gossipSignatureValid(message.SelectionProof[:], signingRoot[:], aggregatorPublicKey[:]) {
		return "reject"
	}
	domain, err = beaconState.GetDomain(beaconState.BeaconConfig().DomainContributionAndProof, contribution.Slot/beaconState.BeaconConfig().SlotsPerEpoch)
	if err != nil {
		return "reject"
	}
	signingRoot, err = fork.ComputeSigningRoot(message, domain)
	if err != nil || !gossipSignatureValid(signedContribution.Signature[:], signingRoot[:], aggregatorPublicKey[:]) {
		return "reject"
	}
	participantPublicKeys := make([][]byte, 0, len(subcommitteePublicKeys))
	for index, publicKey := range subcommitteePublicKeys {
		if utils.IsBitOn(contribution.AggregationBits, index) {
			participantPublicKeys = append(participantPublicKeys, publicKey[:])
		}
	}
	domain, err = beaconState.GetDomain(beaconState.BeaconConfig().DomainSyncCommittee, contribution.Slot/beaconState.BeaconConfig().SlotsPerEpoch)
	if err != nil {
		return "reject"
	}
	signingRoot = utils.Sha256(contribution.BeaconBlockRoot[:], domain)
	valid, err := bls.VerifyAggregate(contribution.Signature[:], signingRoot[:], participantPublicKeys)
	if err != nil || !valid {
		return "reject"
	}
	return "valid"
}

func readGossipStateAndMeta(root fs.FS, c spectest.TestCase) (*state.CachingBeaconState, gossipMeta, error) {
	beaconState, err := spectest.ReadBeaconState(root, c.Version(), "state.ssz_snappy")
	if err != nil {
		return nil, gossipMeta{}, err
	}
	var meta gossipMeta
	if err := spectest.ReadMeta(root, "meta.yaml", &meta); err != nil {
		return nil, gossipMeta{}, err
	}
	return beaconState, meta, nil
}

func gossipSlotIsCurrent(beaconState *state.CachingBeaconState, slot uint64, currentTimeMS uint64) bool {
	genesisTimeMS := beaconState.GenesisTime() * 1000
	if currentTimeMS < genesisTimeMS {
		return false
	}
	return slot == (currentTimeMS-genesisTimeMS)/(beaconState.BeaconConfig().SecondsPerSlot*1000)
}

func gossipSyncSubcommitteePublicKeys(beaconState *state.CachingBeaconState, subcommitteeIndex uint64) []common.Bytes48 {
	committee := beaconState.CurrentSyncCommittee()
	if beaconState.BeaconConfig().SyncCommitteePeriod(beaconState.Slot()) != beaconState.BeaconConfig().SyncCommitteePeriod(beaconState.Slot()+1) {
		committee = beaconState.NextSyncCommittee()
	}
	subcommitteeSize := beaconState.BeaconConfig().SyncCommitteeSize / beaconState.BeaconConfig().SyncCommitteeSubnetCount
	start := subcommitteeIndex * subcommitteeSize
	return committee.GetCommittee()[start : start+subcommitteeSize]
}

func markGossipSyncContributionSeen(
	message *cltypes.ContributionAndProof,
	seenContributions map[syncContributionKey][][]byte,
	seenAggregators map[syncContributionAggregatorKey]struct{},
) {
	contribution := message.Contribution
	contributionKey := syncContributionKey{contribution.Slot, contribution.BeaconBlockRoot, contribution.SubcommitteeIndex}
	seenContributions[contributionKey] = append(seenContributions[contributionKey], bytes.Clone(contribution.AggregationBits))
	seenAggregators[syncContributionAggregatorKey{message.AggregatorIndex, contribution.Slot, contribution.SubcommitteeIndex}] = struct{}{}
}

func gossipBitsHaveParticipants(bits []byte) bool {
	for _, value := range bits {
		if value != 0 {
			return true
		}
	}
	return false
}

func gossipBitsSuperset(superset, subset []byte) bool {
	if len(superset) != len(subset) {
		return false
	}
	for index := range subset {
		if superset[index]&subset[index] != subset[index] {
			return false
		}
	}
	return true
}

func gossipSignatureValid(signature, signingRoot, publicKey []byte) bool {
	valid, err := bls.Verify(signature, signingRoot, publicKey)
	return err == nil && valid
}

func gossipResultError(index int, message gossipMessage, result string) error {
	return fmt.Errorf("message %d (%s): expected %q but got %q", index, message.Message, message.Expected, result)
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
