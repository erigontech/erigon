package state

import (
	"errors"
	"fmt"
	"math"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
)

// IsBuilderIndex returns true if the given validator index is actually a builder index.
// Builder indices have the BUILDER_INDEX_FLAG bit (bit 40) set.
func IsBuilderIndex(validatorIndex uint64) bool {
	return (validatorIndex & clparams.BuilderIndexFlag) != 0
}

// ConvertBuilderIndexToValidatorIndex converts a builder index to a validator index
// by setting the BUILDER_INDEX_FLAG bit.
func ConvertBuilderIndexToValidatorIndex(builderIndex uint64) uint64 {
	return builderIndex | clparams.BuilderIndexFlag
}

// ConvertValidatorIndexToBuilderIndex converts a validator index (with builder flag set)
// to a builder index by clearing the BUILDER_INDEX_FLAG bit.
func ConvertValidatorIndexToBuilderIndex(validatorIndex uint64) uint64 {
	return validatorIndex &^ clparams.BuilderIndexFlag
}

// IsActiveBuilder checks if the builder at builderIndex is active for the given state.
// A builder is considered active if:
// - Its deposit epoch is finalized (deposit_epoch < finalized_checkpoint.epoch)
// - It has not initiated an exit (withdrawable_epoch == FAR_FUTURE_EPOCH)
func IsActiveBuilder(state abstract.BeaconState, builderIndex uint64) bool {
	builders := state.GetBuilders()
	if builders == nil {
		log.Debug("builders is nil")
		return false
	}
	if builderIndex >= uint64(builders.Len()) {
		return false
	}
	builder := builders.Get(int(builderIndex))
	if builder == nil {
		log.Debug("builder is nil", "builderIndex", builderIndex)
		return false
	}
	finalizedEpoch := state.FinalizedCheckpoint().Epoch
	farFuture := state.BeaconConfig().FarFutureEpoch
	return builder.DepositEpoch < finalizedEpoch && builder.WithdrawableEpoch == farFuture
}

// IsBuilderWithdrawalCredential checks if the withdrawal credentials belong to a builder.
// Builder withdrawal credentials start with the configured builder prefix.
func IsBuilderWithdrawalCredential(withdrawalCredentials [32]byte, beaconConfig *clparams.BeaconChainConfig) bool {
	return withdrawalCredentials[0] == byte(beaconConfig.BuilderWithdrawalPrefix)
}

func IndexedPayloadAttestationSignatureSet(s abstract.BeaconState, attestation *cltypes.IndexedPayloadAttestation) (signature, signingRoot, aggregatePublicKey []byte, err error) {
	indices := attestation.AttestingIndices
	if indices.Length() == 0 || !solid.IsUint64Sorted(indices) {
		return nil, nil, nil, errors.New("indexedPayloadAttestationSignatureSet: attesting indices are empty or not sorted")
	}

	pks := make([][]byte, 0, indices.Length())
	for i := range indices.Length() {
		val, err := s.ValidatorForValidatorIndex(int(indices.Get(i)))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("indexedPayloadAttestationSignatureSet: failed to get validator public key: %w", err)
		}
		pks = append(pks, val.PublicKeyBytes())
	}

	epoch := GetEpochAtSlot(s.BeaconConfig(), attestation.Data.Slot)
	domain, err := s.GetDomain(s.BeaconConfig().DomainPtcAttester, epoch)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to get the domain: %w", err)
	}

	root, err := fork.ComputeSigningRoot(attestation.Data, domain)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to get signing root: %w", err)
	}

	aggregate, err := bls.AggregatePublickKeys(pks)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to aggregate public keys: %w", err)
	}

	return attestation.Signature[:], root[:], aggregate, nil
}

func ExecutionPayloadBidSignatureSet(s abstract.BeaconState, signedBid *cltypes.SignedExecutionPayloadBid) (signature, signingRoot, publicKey []byte, err error) {
	builders := s.GetBuilders()
	if builders == nil || signedBid.Message.BuilderIndex >= uint64(builders.Len()) {
		return nil, nil, nil, fmt.Errorf("builder index %d out of range", signedBid.Message.BuilderIndex)
	}
	builder := builders.Get(int(signedBid.Message.BuilderIndex))

	domain, err := s.GetDomain(s.BeaconConfig().DomainBeaconBuilder, Epoch(s))
	if err != nil {
		return nil, nil, nil, err
	}

	root, err := fork.ComputeSigningRoot(signedBid.Message, domain)
	if err != nil {
		return nil, nil, nil, err
	}

	return signedBid.Signature[:], root[:], builder.Pubkey[:], nil
}

// CanBuilderCoverBid returns true if the builder has enough balance to cover the bid amount
// after accounting for the minimum deposit and pending withdrawals.
func CanBuilderCoverBid(s abstract.BeaconState, builderIndex uint64, bidAmount uint64) bool {
	builders := s.GetBuilders()
	if builders == nil {
		log.Debug("builders is nil")
		return false
	}
	if builderIndex >= uint64(builders.Len()) {
		return false
	}
	builder := builders.Get(int(builderIndex))
	if builder == nil {
		log.Debug("builder is nil", "builderIndex", builderIndex)
		return false
	}

	minDepositAmount := s.BeaconConfig().MinDepositAmount
	pendingWithdrawals := GetPendingBalanceToWithdrawForBuilder(s, builderIndex)
	capacity, ok := BuilderBidCapacity(builder.Balance, minDepositAmount, pendingWithdrawals)
	return ok && bidAmount <= capacity
}

// BuilderBidCapacity returns the largest bid the builder can cover; ok is false when it cannot cover any bid.
func BuilderBidCapacity(balance, minDepositAmount, pendingWithdrawals uint64) (capacity uint64, ok bool) {
	if pendingWithdrawals > math.MaxUint64-minDepositAmount {
		return 0, false
	}
	minimumBalance := minDepositAmount + pendingWithdrawals
	if balance < minimumBalance {
		return 0, false
	}
	return balance - minimumBalance, true
}

func GetProposerDependentRoot(s abstract.BeaconState, epoch uint64) (common.Hash, error) {
	cfg := s.BeaconConfig()
	if epoch <= cfg.MinSeedLookahead {
		if s.Slot() == 0 {
			return s.BlockRoot()
		}
		return s.GetBlockRootAtSlot(0)
	}
	dependentSlot := (epoch-cfg.MinSeedLookahead)*cfg.SlotsPerEpoch - 1
	return s.GetBlockRootAtSlot(dependentSlot)
}

// ForEachPendingBuilderWithdrawal calls fn for every pending builder withdrawal and pending payment withdrawal.
func ForEachPendingBuilderWithdrawal(s abstract.BeaconState, fn func(builderIndex, amount uint64)) {
	pendingWithdrawals := s.GetBuilderPendingWithdrawals()
	if pendingWithdrawals != nil {
		pendingWithdrawals.Range(func(_ int, withdrawal *cltypes.BuilderPendingWithdrawal, _ int) bool {
			if withdrawal != nil {
				fn(withdrawal.BuilderIndex, withdrawal.Amount)
			}
			return true
		})
	} else {
		log.Debug("builder_pending_withdrawals is nil")
	}

	pendingPayments := s.GetBuilderPendingPayments()
	if pendingPayments != nil {
		pendingPayments.Range(func(_ int, payment *cltypes.BuilderPendingPayment, _ int) bool {
			if payment != nil && payment.Withdrawal != nil {
				fn(payment.Withdrawal.BuilderIndex, payment.Withdrawal.Amount)
			}
			return true
		})
	} else {
		log.Debug("builder_pending_payments is nil")
	}
}

// GetPendingBalanceToWithdrawForBuilder returns the total pending balance to withdraw for a builder.
func GetPendingBalanceToWithdrawForBuilder(s abstract.BeaconState, builderIndex uint64) uint64 {
	var total uint64
	ForEachPendingBuilderWithdrawal(s, func(index, amount uint64) {
		if index != builderIndex {
			return
		}
		if amount > math.MaxUint64-total {
			total = math.MaxUint64
			return
		}
		total += amount
	})

	return total
}

// IsPendingValidator returns true if any pending deposit in the list matches
// the given pubkey AND has a valid deposit signature.
// [New in Gloas:EIP7732]
func IsPendingValidator(cfg *clparams.BeaconChainConfig, pendingDeposits *solid.ListSSZ[*solid.PendingDeposit], pubkey common.Bytes48) bool {
	if pendingDeposits == nil {
		return false
	}
	for i := 0; i < pendingDeposits.Len(); i++ {
		d := pendingDeposits.Get(i)
		if d.PubKey != pubkey {
			continue
		}
		valid, err := IsValidDepositSignature(cfg, d.PubKey, d.WithdrawalCredentials, d.Amount, d.Signature)
		if err != nil {
			log.Debug("IsValidDepositSignature failed", "pubkey", d.PubKey, "err", err)
			continue
		}
		if valid {
			return true
		}
	}
	return false
}

// IsBuilderPubkey returns true if the given pubkey belongs to any builder in the state.
// [New in Gloas:EIP7732]
func IsBuilderPubkey(s abstract.BeaconState, pubkey common.Bytes48) bool {
	builders := s.GetBuilders()
	if builders == nil {
		return false
	}
	for i := 0; i < builders.Len(); i++ {
		builder := builders.Get(i)
		if builder != nil && builder.Pubkey == pubkey {
			return true
		}
	}
	return false
}

// IsValidDepositSignature validates a validator deposit signature.
func IsValidDepositSignature(cfg *clparams.BeaconChainConfig, pubkey common.Bytes48, withdrawalCredentials common.Hash, amount uint64, signature common.Bytes96) (bool, error) {
	return isValidDepositSignatureForDomain(cfg, cfg.DomainDeposit, pubkey, withdrawalCredentials, amount, signature)
}

func IsValidBuilderDepositSignature(cfg *clparams.BeaconChainConfig, request *solid.BuilderDepositRequest) (bool, error) {
	return isValidDepositSignatureForDomain(cfg, cfg.DomainBuilderDeposit, request.PubKey, request.WithdrawalCredentials, request.Amount, request.Signature)
}

func isValidDepositSignatureForDomain(cfg *clparams.BeaconChainConfig, domainType [4]byte, pubkey common.Bytes48, withdrawalCredentials common.Hash, amount uint64, signature common.Bytes96) (bool, error) {
	domain, err := fork.ComputeDomain(
		domainType[:],
		utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)),
		[32]byte{},
	)
	if err != nil {
		return false, err
	}
	depositData := &cltypes.DepositData{
		PubKey:                pubkey,
		WithdrawalCredentials: withdrawalCredentials,
		Amount:                amount,
		Signature:             signature,
	}
	depositMessageRoot, err := depositData.MessageHash()
	if err != nil {
		return false, err
	}
	signedRoot := crypto.Sha256(depositMessageRoot[:], domain)
	valid, err := bls.Verify(signature[:], signedRoot[:], pubkey[:])
	if err != nil {
		return false, err
	}
	return valid, nil
}

// GetIndexForNewBuilder returns the first builder index that is reusable
// (withdrawable_epoch <= current_epoch and balance == 0), or len(state.builders)
// if no such slot exists.
// [New in Gloas:EIP7732]
func GetIndexForNewBuilder(s abstract.BeaconState) uint64 {
	builders := s.GetBuilders()
	if builders == nil {
		return 0
	}
	epoch := GetEpochAtSlot(s.BeaconConfig(), s.Slot())
	for i := 0; i < builders.Len(); i++ {
		builder := builders.Get(i)
		if builder != nil && builder.WithdrawableEpoch <= epoch && builder.Balance == 0 {
			return uint64(i)
		}
	}
	return uint64(builders.Len())
}

// AddBuilderToRegistry adds a new builder to the registry.
func AddBuilderToRegistry(s abstract.BeaconState, pubkey common.Bytes48, version uint8, executionAddress common.Address, amount uint64, slot uint64) error {
	cfg := s.BeaconConfig()
	builders := s.GetBuilders()
	if builders == nil {
		return errors.New("builder registry unavailable")
	}
	index := GetIndexForNewBuilder(s)
	if index >= cfg.BuilderRegistryLimit {
		return fmt.Errorf("builder registry full: index %d >= limit %d", index, cfg.BuilderRegistryLimit)
	}

	builder := &cltypes.Builder{
		Pubkey:            pubkey,
		Version:           version,
		ExecutionAddress:  executionAddress,
		Balance:           amount,
		DepositEpoch:      GetEpochAtSlot(cfg, slot),
		WithdrawableEpoch: cfg.FarFutureEpoch,
	}

	if int(index) < builders.Len() {
		builders.Set(int(index), builder)
	} else {
		builders.Append(builder)
	}
	s.SetBuilders(builders)
	return nil
}

// ApplyDepositForBuilder processes a fork-onboarding builder deposit.
// If the pubkey is new and signature is valid, registers a new builder.
// If the pubkey already exists, increases the builder's balance.
func ApplyDepositForBuilder(s abstract.BeaconState, pubkey common.Bytes48, withdrawalCredentials common.Hash, amount uint64, signature common.Bytes96, slot uint64) error {
	builders := s.GetBuilders()

	// Check if pubkey already exists in builders
	builderIndex := -1
	if builders != nil {
		for i := 0; i < builders.Len(); i++ {
			builder := builders.Get(i)
			if builder != nil && builder.Pubkey == pubkey {
				builderIndex = i
				break
			}
		}
	}

	if builderIndex == -1 {
		// New builder: verify deposit signature (proof of possession)
		valid, err := IsValidDepositSignature(s.BeaconConfig(), pubkey, withdrawalCredentials, amount, signature)
		if err != nil {
			log.Debug("invalid builder deposit signature", "err", err)
			return nil
		}
		if valid {
			return AddBuilderToRegistry(
				s,
				pubkey,
				s.BeaconConfig().PayloadBuilderVersion,
				common.BytesToAddress(withdrawalCredentials[12:]),
				amount,
				slot,
			)
		}
	} else {
		builder := builders.Get(builderIndex)
		if builder == nil {
			return nil
		}
		newBuilder := *builder
		if amount > math.MaxUint64-newBuilder.Balance {
			return fmt.Errorf("builder balance overflow: %d + %d", newBuilder.Balance, amount)
		}
		newBuilder.Balance += amount
		builders.Set(builderIndex, &newBuilder)
		s.SetBuilders(builders)
	}
	return nil
}

func ApplyBuilderDepositRequest(s abstract.BeaconState, request *solid.BuilderDepositRequest) error {
	if !IsBuilderWithdrawalCredential(request.WithdrawalCredentials, s.BeaconConfig()) {
		return nil
	}
	builders := s.GetBuilders()
	builderIndex := -1
	if builders != nil {
		for i := 0; i < builders.Len(); i++ {
			builder := builders.Get(i)
			if builder != nil && builder.Pubkey == request.PubKey {
				builderIndex = i
				break
			}
		}
	}

	if builderIndex == -1 {
		valid, err := IsValidBuilderDepositSignature(s.BeaconConfig(), request)
		if err != nil || !valid {
			return nil
		}
		return AddBuilderToRegistry(
			s,
			request.PubKey,
			s.BeaconConfig().PayloadBuilderVersion,
			common.BytesToAddress(request.WithdrawalCredentials[12:]),
			request.Amount,
			s.Slot(),
		)
	}

	builder := builders.Get(builderIndex)
	if builder == nil {
		return nil
	}
	newBuilder := *builder
	if newBuilder.WithdrawableEpoch != s.BeaconConfig().FarFutureEpoch && newBuilder.Balance == 0 {
		newBuilder.WithdrawableEpoch = GetEpochAtSlot(s.BeaconConfig(), s.Slot()) + s.BeaconConfig().MinBuilderWithdrawabilityDelay
	}
	if request.Amount > math.MaxUint64-newBuilder.Balance {
		return fmt.Errorf("builder balance overflow: %d + %d", newBuilder.Balance, request.Amount)
	}
	newBuilder.Balance += request.Amount
	builders.Set(builderIndex, &newBuilder)
	s.SetBuilders(builders)
	return nil
}
