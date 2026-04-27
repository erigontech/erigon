package eth2

import (
	"fmt"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
)

// verifyExecutionPayloadEnvelopeSignature verifies the BLS signature of a signed execution payload envelope.
// If builder_index is BUILDER_INDEX_SELF_BUILD, the proposer's pubkey is used; otherwise the builder's pubkey.
// [New in Gloas:EIP7732]
func verifyExecutionPayloadEnvelopeSignature(s abstract.BeaconState, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
	builderIndex := signedEnvelope.Message.BuilderIndex

	// Skip BLS verification for locally-produced self-build envelopes that carry
	// InfiniteSignature. The CL node constructs these when the VC does not provide
	// a pre-signed envelope; the private key lives in the VC and is not available here.
	if builderIndex == clparams.BuilderIndexSelfBuild && signedEnvelope.Signature == common.Bytes96(bls.InfiniteSignature) {
		return true, nil
	}

	var pk [48]byte
	if builderIndex == clparams.BuilderIndexSelfBuild {
		// Self-build: use the proposer's pubkey
		proposerIndex := s.LatestBlockHeader().ProposerIndex
		validator, err := s.ValidatorForValidatorIndex(int(proposerIndex))
		if err != nil {
			return false, fmt.Errorf("verifyExecutionPayloadEnvelopeSignature: failed to get proposer validator: %w", err)
		}
		pk = validator.PublicKey()
	} else {
		// Builder: use the builder's pubkey
		builders := s.GetBuilders()
		if builders == nil || int(builderIndex) >= builders.Len() {
			return false, fmt.Errorf("verifyExecutionPayloadEnvelopeSignature: invalid builder index %d", builderIndex)
		}
		pk = builders.Get(int(builderIndex)).Pubkey
	}

	domain, err := s.GetDomain(s.BeaconConfig().DomainBeaconBuilder, state.Epoch(s))
	if err != nil {
		return false, fmt.Errorf("verifyExecutionPayloadEnvelopeSignature: failed to get domain: %w", err)
	}
	signingRoot, err := fork.ComputeSigningRoot(signedEnvelope.Message, domain)
	if err != nil {
		return false, fmt.Errorf("verifyExecutionPayloadEnvelopeSignature: failed to compute signing root: %w", err)
	}
	return bls.Verify(signedEnvelope.Signature[:], signingRoot[:], pk[:])
}
