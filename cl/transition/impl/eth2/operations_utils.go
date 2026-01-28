package eth2

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
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

// getBuilderFromDeposit creates a new Builder from deposit parameters.
// [New in Gloas:EIP7732]
func getBuilderFromDeposit(s abstract.BeaconState, pubkey common.Bytes48, withdrawalCredentials [32]byte, amount uint64) *cltypes.Builder {
	return &cltypes.Builder{
		Pubkey:            pubkey,
		Version:           withdrawalCredentials[0],
		ExecutionAddress:  common.BytesToAddress(withdrawalCredentials[12:]),
		Balance:           amount,
		DepositEpoch:      state.Epoch(s),
		WithdrawableEpoch: s.BeaconConfig().FarFutureEpoch,
	}
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
func addBuilderToRegistry(s abstract.BeaconState, pubkey common.Bytes48, withdrawalCredentials [32]byte, amount uint64) {
	index := getIndexForNewBuilder(s)
	builder := getBuilderFromDeposit(s, pubkey, withdrawalCredentials, amount)
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
func applyDepositForBuilder(s abstract.BeaconState, pubkey common.Bytes48, withdrawalCredentials [32]byte, amount uint64, signature common.Bytes96) {
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
			addBuilderToRegistry(s, pubkey, withdrawalCredentials, amount)
		}
	} else {
		// Existing builder: increase balance
		builder := builders.Get(builderIndex)
		builder.Balance += amount
		builders.Set(builderIndex, builder)
		s.SetBuilders(builders)
	}
}
