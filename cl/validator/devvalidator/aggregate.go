package devvalidator

import (
	"context"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

// buildAggregateAttestation wraps a validator's SingleAttestation into the
// Electra Attestation form (committee bits + aggregation bits) suitable for
// inclusion in an AggregateAndProof.
func buildAggregateAttestation(
	single *solid.SingleAttestation,
	validatorPosition, committeeLength uint64,
	cfg *clparams.BeaconChainConfig,
) *solid.Attestation {
	return single.ToAttestation(int(validatorPosition), int(committeeLength), int(cfg.MaxCommitteesPerSlot), cfg)
}

// submitAggregateAndProof builds and submits a signed aggregate-and-proof for
// the validator's attestation. Block production only includes attestations that
// reach the operations pool via this path, so without it the chain never
// finalizes. With dev's small committees every validator is an aggregator.
func (s *Service) submitAggregateAndProof(
	ctx context.Context,
	slot, committeeIndex uint64,
	key *ValidatorKey,
	attData *solid.AttestationData,
	sig common.Bytes96,
	committeeLength, validatorPosition uint64,
) {
	selectionProof, err := signSelectionProof(key, slot, s.cfg, s.genesisValidatorsRoot)
	if err != nil {
		s.logger.Warn("[dev-validator] selection proof sign failed", "err", err)
		return
	}
	if !state.IsAggregator(s.cfg, committeeLength, committeeIndex, selectionProof) {
		return
	}

	single := &solid.SingleAttestation{
		CommitteeIndex: committeeIndex,
		AttesterIndex:  key.ValidatorIndex,
		Data:           attData,
		Signature:      sig,
	}
	aggregate := buildAggregateAttestation(single, validatorPosition, committeeLength, s.cfg)

	msg := &cltypes.AggregateAndProof{
		AggregatorIndex: key.ValidatorIndex,
		Aggregate:       aggregate,
		SelectionProof:  selectionProof,
	}
	aggregatorSig, err := signAggregateAndProof(key, msg, slot, s.cfg, s.genesisValidatorsRoot)
	if err != nil {
		s.logger.Warn("[dev-validator] aggregate sign failed", "err", err)
		return
	}
	signed := &cltypes.SignedAggregateAndProof{
		Message:   msg,
		Signature: aggregatorSig,
	}
	if err := s.client.post(ctx, "/eth/v1/validator/aggregate_and_proofs", []any{signed}); err != nil {
		s.logger.Debug("[dev-validator] aggregate submit failed", "slot", slot, "err", err)
	}
}
