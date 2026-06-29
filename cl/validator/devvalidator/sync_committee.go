package devvalidator

import (
	"context"
	"fmt"
	"strconv"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

// buildSyncCommitteeMessage constructs the JSON body for a single
// SyncCommitteeMessage submitted to /eth/v1/beacon/pool/sync_committees.
func buildSyncCommitteeMessage(slot uint64, blockRoot common.Hash, validatorIndex uint64, sig common.Bytes96) map[string]interface{} {
	return map[string]interface{}{
		"slot":              strconv.FormatUint(slot, 10),
		"beacon_block_root": blockRoot.Hex(),
		"validator_index":   strconv.FormatUint(validatorIndex, 10),
		"signature":         hexutil.Encode(sig[:]),
	}
}

// maybeSyncCommittee submits sync committee messages and contributions for any
// of our validators that are members of the current sync committee. The
// messages alone are not enough to appear in a block's SyncAggregate — block
// production reads aggregated contributions — so we submit both.
func (s *Service) maybeSyncCommittee(ctx context.Context, slot uint64) {
	epoch := slot / s.cfg.SlotsPerEpoch

	type syncDuty struct {
		Pubkey                         string   `json:"pubkey"`
		ValidatorIndex                 string   `json:"validator_index"`
		ValidatorSyncCommitteeIndicies []string `json:"validator_sync_committee_indices"`
	}

	indices := make([]string, 0, len(s.keys))
	for _, k := range s.keys {
		indices = append(indices, strconv.FormatUint(k.ValidatorIndex, 10))
	}

	var duties []syncDuty
	path := fmt.Sprintf("/eth/v1/validator/duties/sync/%d", epoch)
	if err := s.client.postAndDecode(ctx, path, indices, &duties); err != nil {
		return
	}
	if len(duties) == 0 {
		return
	}

	var rootResp struct {
		Root common.Hash `json:"root"`
	}
	if err := s.client.get(ctx, "/eth/v1/beacon/blocks/head/root", &rootResp); err != nil {
		return
	}

	syncSubcommitteeSize := s.cfg.SyncCommitteeSize / s.cfg.SyncCommitteeSubnetCount

	msgs := make([]interface{}, 0, len(duties))
	contributions := make([]interface{}, 0, len(duties))
	for _, duty := range duties {
		validatorIndex, parseErr := strconv.ParseUint(duty.ValidatorIndex, 10, 64)
		if parseErr != nil {
			continue
		}
		pubBytes, err := hexutil.Decode(duty.Pubkey)
		if err != nil || len(pubBytes) != 48 {
			continue
		}
		var pub common.Bytes48
		copy(pub[:], pubBytes)
		key, ok := s.keysByPub[pub]
		if !ok {
			continue
		}
		sig, err := signSyncCommitteeMessage(key, rootResp.Root, slot, s.cfg, s.genesisValidatorsRoot)
		if err != nil {
			s.logger.Warn("[dev-validator] sync committee sign failed", "err", err)
			continue
		}
		msgs = append(msgs, buildSyncCommitteeMessage(slot, rootResp.Root, validatorIndex, sig))

		if c := s.buildContributions(slot, rootResp.Root, key, duty.ValidatorSyncCommitteeIndicies, syncSubcommitteeSize, sig); len(c) > 0 {
			contributions = append(contributions, c...)
		}
	}

	if len(msgs) > 0 {
		if err := s.client.post(ctx, "/eth/v1/beacon/pool/sync_committees", msgs); err != nil {
			s.logger.Debug("[dev-validator] sync committee submit failed", "slot", slot, "err", err)
		}
	}
	if len(contributions) > 0 {
		if err := s.client.post(ctx, "/eth/v1/validator/contribution_and_proofs", contributions); err != nil {
			s.logger.Debug("[dev-validator] sync contribution submit failed", "slot", slot, "err", err)
		}
	}
}

// buildContributions produces at most one signed sync committee contribution per
// subcommittee the validator participates in (the spec accepts only the first
// per aggregator+slot+subcommittee), each with the validator's single bit set.
func (s *Service) buildContributions(
	slot uint64,
	blockRoot common.Hash,
	key *ValidatorKey,
	committeeIndicesStr []string,
	syncSubcommitteeSize uint64,
	sig common.Bytes96,
) []interface{} {
	seenSubcommittees := map[uint64]struct{}{}
	out := make([]interface{}, 0, len(committeeIndicesStr))
	for _, posStr := range committeeIndicesStr {
		pos, err := strconv.ParseUint(posStr, 10, 64)
		if err != nil || syncSubcommitteeSize == 0 {
			continue
		}
		subcommitteeIndex := pos / syncSubcommitteeSize
		if _, done := seenSubcommittees[subcommitteeIndex]; done {
			continue
		}
		bitIndex := pos % syncSubcommitteeSize

		selectionProof, err := signSyncCommitteeSelectionProof(key, slot, subcommitteeIndex, s.cfg, s.genesisValidatorsRoot)
		if err != nil {
			s.logger.Warn("[dev-validator] sync selection proof sign failed", "err", err)
			continue
		}
		if !isSyncCommitteeAggregator(s.cfg, selectionProof) {
			continue
		}
		seenSubcommittees[subcommitteeIndex] = struct{}{}

		msg := &cltypes.ContributionAndProof{
			AggregatorIndex: key.ValidatorIndex,
			Contribution:    buildContribution(slot, blockRoot, subcommitteeIndex, bitIndex, sig, s.cfg),
			SelectionProof:  selectionProof,
		}
		contribSig, err := signContributionAndProof(key, msg, slot, s.cfg, s.genesisValidatorsRoot)
		if err != nil {
			s.logger.Warn("[dev-validator] sync contribution sign failed", "err", err)
			continue
		}
		out = append(out, &cltypes.SignedContributionAndProof{Message: msg, Signature: contribSig})
	}
	return out
}
