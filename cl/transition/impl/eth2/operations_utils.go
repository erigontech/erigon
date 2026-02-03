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
	log "github.com/erigontech/erigon/common/log/v3"
)

// isBuilderPubkey returns true if the given pubkey belongs to any builder in the state.
// [New in Gloas:EIP7732]
func isBuilderPubkey(s abstract.BeaconState, pubkey common.Bytes48) bool {
	builders := s.GetBuilders()
	if builders == nil {
		return false
	}
	for i := 0; i < builders.Len(); i++ {
		if builders.Get(i).Pubkey == pubkey {
			return true
		}
	}
	return false
}

// getIndexForNewBuilder returns the first builder index that is reusable
// (withdrawable_epoch <= current_epoch and balance == 0), or len(state.builders)
// if no such slot exists.
// [New in Gloas:EIP7732]
func getIndexForNewBuilder(s abstract.BeaconState) cltypes.BuilderIndex {
	builders := s.GetBuilders()
	if builders == nil {
		return 0
	}
	epoch := state.Epoch(s)
	for i := 0; i < builders.Len(); i++ {
		builder := builders.Get(i)
		if builder.WithdrawableEpoch <= epoch && builder.Balance == 0 {
			return cltypes.BuilderIndex(i)
		}
	}
	return cltypes.BuilderIndex(builders.Len())
}

// addBuilderToRegistry adds a new builder to the registry, reusing a vacant slot if available,
// otherwise appending to the end of the builders list.
// [New in Gloas:EIP7732]
func addBuilderToRegistry(s abstract.BeaconState, pubkey common.Bytes48, withdrawalCredentials [32]byte, amount uint64, slot uint64) {
	index := getIndexForNewBuilder(s)
	builder := &cltypes.Builder{
		Pubkey:            pubkey,
		Version:           withdrawalCredentials[0],
		ExecutionAddress:  common.BytesToAddress(withdrawalCredentials[12:]),
		Balance:           amount,
		DepositEpoch:      state.GetEpochAtSlot(s.BeaconConfig(), slot),
		WithdrawableEpoch: s.BeaconConfig().FarFutureEpoch,
	}
	builders := s.GetBuilders()
	if int(index) < builders.Len() {
		builders.Set(int(index), builder)
	} else {
		builders.Append(builder)
	}
	s.SetBuilders(builders)
}

// applyDepositForBuilder processes a builder deposit: if the pubkey is new and the signature is valid,
// registers a new builder; if the pubkey already exists, increases the builder's balance.
// [New in Gloas:EIP7732]
func applyDepositForBuilder(s abstract.BeaconState, pubkey common.Bytes48, withdrawalCredentials [32]byte, amount uint64, signature common.Bytes96, slot uint64) {
	builders := s.GetBuilders()

	// Check if pubkey already exists in builders
	builderIndex := -1
	if builders != nil {
		for i := 0; i < builders.Len(); i++ {
			if builders.Get(i).Pubkey == pubkey {
				builderIndex = i
				break
			}
		}
	}

	if builderIndex == -1 {
		// New builder: verify deposit signature (proof of possession)
		depositData := &cltypes.DepositData{
			PubKey:                pubkey,
			WithdrawalCredentials: withdrawalCredentials,
			Amount:                amount,
			Signature:             signature,
		}
		valid, err := isValidDepositSignature(depositData, s.BeaconConfig())
		if err != nil {
			log.Warn("applyDepositForBuilder: error verifying deposit signature", "err", err)
			return
		}
		if valid {
			addBuilderToRegistry(s, pubkey, withdrawalCredentials, amount, slot)
		}
	} else {
		// Existing builder: increase balance
		builder := builders.Get(builderIndex)
		builder.Balance += amount
		builders.Set(builderIndex, builder)
		s.SetBuilders(builders)
	}
}

// verifyExecutionPayloadEnvelopeSignature verifies the BLS signature of a signed execution payload envelope.
// If builder_index is BUILDER_INDEX_SELF_BUILD, the proposer's pubkey is used; otherwise the builder's pubkey.
// [New in Gloas:EIP7732]
func verifyExecutionPayloadEnvelopeSignature(s abstract.BeaconState, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
	builderIndex := signedEnvelope.Message.BuilderIndex
	var pk [48]byte
	if builderIndex == cltypes.BuilderIndex(clparams.BuilderIndexSelfBuild) {
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
