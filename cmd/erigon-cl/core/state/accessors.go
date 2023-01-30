package state

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// GetActiveValidatorsIndices returns the list of validator indices active for the given epoch.
func (b *BeaconState) GetActiveValidatorsIndices(epoch uint64) (indicies []uint64) {
	for i, validator := range b.validators {
		if !validator.Active(epoch) {
			continue
		}
		indicies = append(indicies, uint64(i))
	}
	return
}

// GetEpochAtSlot gives the epoch for a certain slot
func (b *BeaconState) GetEpochAtSlot(slot uint64) uint64 {
	return slot / b.beaconConfig.SlotsPerEpoch
}

// Epoch returns current epoch.
func (b *BeaconState) Epoch() uint64 {
	return b.GetEpochAtSlot(b.slot)
}

// PreviousEpoch returns previous epoch.
func (b *BeaconState) PreviousEpoch() uint64 {
	epoch := b.Epoch()
	if epoch == 0 {
		return epoch
	}
	return epoch - 1
}

// getUnslashedParticipatingIndices returns set of currently unslashed participating indexes
func (b *BeaconState) GetUnslashedParticipatingIndices(flagIndex int, epoch uint64) (validatorSet []uint64, err error) {
	var participation cltypes.ParticipationFlagsList
	// Must be either previous or current epoch
	switch epoch {
	case b.Epoch():
		participation = b.currentEpochParticipation
	case b.PreviousEpoch():
		participation = b.previousEpochParticipation
	default:
		return nil, fmt.Errorf("getUnslashedParticipatingIndices: only epoch and previous epoch can be used")
	}
	// Iterate over all validators and include the active ones that have flag_index enabled and are not slashed.
	for i, validator := range b.Validators() {
		if !validator.Active(epoch) ||
			!participation[i].HasFlag(flagIndex) ||
			validator.Slashed {
			continue
		}
		validatorSet = append(validatorSet, uint64(i))
	}
	return
}

// GetTotalBalance return the sum of all balances within the given validator set.
func (b *BeaconState) GetTotalBalance(validatorSet []uint64) (uint64, error) {
	var (
		total          uint64
		validatorsSize = uint64(len(b.validators))
	)
	for _, validatorIndex := range validatorSet {
		// Should be in bounds.
		if validatorIndex >= validatorsSize {
			return 0, fmt.Errorf("GetTotalBalance: out of bounds validator index")
		}
		total += b.validators[validatorIndex].EffectiveBalance
	}
	// Always minimum set to EffectiveBalanceIncrement
	if total < b.beaconConfig.EffectiveBalanceIncrement {
		total = b.beaconConfig.EffectiveBalanceIncrement
	}
	return total, nil
}

// GetTotalActiveBalance return the sum of all balances within active validators.
func (b *BeaconState) GetTotalActiveBalance() (uint64, error) {
	return b.GetTotalBalance(b.GetActiveValidatorsIndices(b.Epoch()))
}

// GetTotalSlashingAmount return the sum of all slashings.
func (b *BeaconState) GetTotalSlashingAmount() (t uint64) {
	for _, slash := range &b.slashings {
		t += slash
	}
	return
}

// GetBlockRoot returns blook root at start of a given epoch
func (b *BeaconState) GetBlockRoot(epoch uint64) (libcommon.Hash, error) {
	return b.GetBlockRootAtSlot(epoch * b.beaconConfig.SlotsPerEpoch)
}

// GetBlockRootAtSlot returns the block root at a given slot
func (b *BeaconState) GetBlockRootAtSlot(slot uint64) (libcommon.Hash, error) {
	if slot >= b.slot {
		return libcommon.Hash{}, fmt.Errorf("GetBlockRootAtSlot: slot in the future")
	}
	if b.slot > slot+b.beaconConfig.SlotsPerHistoricalRoot {
		return libcommon.Hash{}, fmt.Errorf("GetBlockRootAtSlot: slot too much far behind")
	}
	return b.blockRoots[slot%b.beaconConfig.SlotsPerHistoricalRoot], nil
}

func (b *BeaconState) GetDomain(domainType [4]byte, epoch uint64) ([]byte, error) {
	if epoch == 0 {
		epoch = b.Epoch()
	}
	var forkVersion [4]byte
	if epoch < b.fork.Epoch {
		forkVersion = b.fork.PreviousVersion
	} else {
		forkVersion = b.fork.CurrentVersion
	}
	return fork.ComputeDomain(domainType[:], forkVersion, b.genesisValidatorsRoot)
}

func (b *BeaconState) ComputeShuffledIndex(ind, ind_count uint64, seed [32]byte) (uint64, error) {
	if ind >= ind_count {
		return 0, fmt.Errorf("index=%d must be less than the index count=%d", ind, ind_count)
	}

	for i := uint64(0); i < b.beaconConfig.ShuffleRoundCount; i++ {
		// Construct first hash input.
		input := append(seed[:], byte(i))
		hashedInput := utils.Keccak256(input)

		// Read hash value.
		hashValue := binary.LittleEndian.Uint64(hashedInput[:8])

		// Caclulate pivot and flip.
		pivot := hashValue % ind_count
		flip := (pivot + ind_count - ind) % ind_count

		// No uint64 max function in go standard library.
		position := ind
		if flip > ind {
			position = flip
		}

		// Construct the second hash input.
		positionByteArray := make([]byte, 4)
		binary.LittleEndian.PutUint32(positionByteArray, uint32(position>>8))
		input2 := append(seed[:], byte(i))
		input2 = append(input2, positionByteArray...)

		hashedInput2 := utils.Keccak256(input2)
		// Read hash value.
		byteVal := hashedInput2[(position%256)/8]
		bitVal := (byteVal >> (position % 8)) % 2
		if bitVal == 1 {
			ind = flip
		}
	}
	return ind, nil
}

func (b *BeaconState) ComputeProposerIndex(indices []uint64, seed [32]byte) (uint64, error) {
	if len(indices) == 0 {
		return 0, fmt.Errorf("must have >0 indices")
	}
	maxRandomByte := uint64(1<<8 - 1)
	i := uint64(0)
	total := uint64(len(indices))
	buf := make([]byte, 8)
	for {
		shuffled, err := b.ComputeShuffledIndex(i%total, total, seed)
		if err != nil {
			return 0, err
		}
		candidateIndex := indices[shuffled]
		if candidateIndex >= uint64(len(b.validators)) {
			return 0, fmt.Errorf("candidate index out of range: %d for validator set of length: %d", candidateIndex, len(b.validators))
		}
		binary.LittleEndian.PutUint64(buf, i/32)
		input := append(seed[:], buf...)
		randomByte := uint64(utils.Keccak256(input)[i%32])
		effectiveBalance := b.validators[candidateIndex].EffectiveBalance
		if effectiveBalance*maxRandomByte >= clparams.MainnetBeaconConfig.MaxEffectiveBalance*randomByte {
			return candidateIndex, nil
		}
		i += 1
	}
}

func (b *BeaconState) GetRandaoMixes(epoch uint64) [32]byte {
	return b.randaoMixes[epoch%b.beaconConfig.EpochsPerHistoricalVector]
}

func (b *BeaconState) GetBeaconProposerIndex() (uint64, error) {
	epoch := b.Epoch()

	hash := sha256.New()
	// Input for the seed hash.
	input := b.GetSeed(epoch, clparams.MainnetBeaconConfig.DomainBeaconProposer)
	slotByteArray := make([]byte, 8)
	binary.LittleEndian.PutUint64(slotByteArray, b.Slot())

	// Add slot to the end of the input.
	inputWithSlot := append(input, slotByteArray...)

	// Calculate the hash.
	hash.Write(inputWithSlot)
	seed := hash.Sum(nil)

	indices := b.GetActiveValidatorsIndices(epoch)

	// Write the seed to an array.
	seedArray := [32]byte{}
	copy(seedArray[:], seed)

	return b.ComputeProposerIndex(indices, seedArray)
}

func (b *BeaconState) GetSeed(epoch uint64, domain [4]byte) []byte {
	mix := b.GetRandaoMixes(epoch + b.beaconConfig.EpochsPerHistoricalVector - b.beaconConfig.MinSeedLookahead - 1)
	epochByteArray := make([]byte, 8)
	binary.LittleEndian.PutUint64(epochByteArray, epoch)
	input := append(domain[:], epochByteArray...)
	input = append(input, mix[:]...)
	hash := sha256.New()
	hash.Write(input)
	return hash.Sum(nil)
}

// BaseRewardPerIncrement return base rewards for processing sync committee and duties.
func (b *BeaconState) baseRewardPerIncrement(totalActiveBalance uint64) uint64 {
	return b.beaconConfig.EffectiveBalanceIncrement * b.beaconConfig.BaseRewardFactor / utils.IntegerSquareRoot(totalActiveBalance)
}

// SyncRewards returns the proposer reward and the sync participant reward given the total active balance in state.
func (b *BeaconState) SyncRewards() (proposerReward, participantReward uint64, err error) {
	activeBalance, err := b.GetTotalActiveBalance()
	if err != nil {
		return 0, 0, err
	}
	totalActiveIncrements := activeBalance / b.beaconConfig.EffectiveBalanceIncrement
	baseRewardPerInc := b.baseRewardPerIncrement(activeBalance)
	totalBaseRewards := baseRewardPerInc * totalActiveIncrements
	maxParticipantRewards := totalBaseRewards * b.beaconConfig.SyncRewardWeight / b.beaconConfig.WeightDenominator / b.beaconConfig.SlotsPerEpoch
	participantReward = maxParticipantRewards / b.beaconConfig.SyncCommitteeSize
	proposerReward = participantReward * b.beaconConfig.ProposerWeight / (b.beaconConfig.WeightDenominator - b.beaconConfig.ProposerWeight)
	return
}

func (b *BeaconState) ValidatorFromDeposit(deposit *cltypes.Deposit) *cltypes.Validator {
	amount := deposit.Data.Amount
	effectiveBalance := amount - amount%b.beaconConfig.EffectiveBalanceIncrement
	if effectiveBalance > b.beaconConfig.EffectiveBalanceIncrement {
		effectiveBalance = b.beaconConfig.EffectiveBalanceIncrement
	}

	return &cltypes.Validator{
		PublicKey:                  deposit.Data.PubKey,
		WithdrawalCredentials:      deposit.Data.WithdrawalCredentials,
		ActivationEligibilityEpoch: b.beaconConfig.FarFutureEpoch,
		ActivationEpoch:            b.beaconConfig.FarFutureEpoch,
		ExitEpoch:                  b.beaconConfig.FarFutureEpoch,
		WithdrawableEpoch:          b.beaconConfig.FarFutureEpoch,
		EffectiveBalance:           effectiveBalance,
	}
}
