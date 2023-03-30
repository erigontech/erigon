package state

import (
	"encoding/binary"

	"github.com/minio/sha256-simd"

	lru2 "github.com/hashicorp/golang-lru/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

type HashFunc func([]byte) ([32]byte, error)

const (
	blockRootsLength = 8192
	stateRootsLength = 8192
	randoMixesLength = 65536
	slashingsLength  = 8192
)

type BeaconState struct {
	// State fields
	genesisTime                uint64
	genesisValidatorsRoot      libcommon.Hash
	slot                       uint64
	fork                       *cltypes.Fork
	latestBlockHeader          *cltypes.BeaconBlockHeader
	blockRoots                 [blockRootsLength]libcommon.Hash
	stateRoots                 [stateRootsLength]libcommon.Hash
	historicalRoots            []libcommon.Hash
	eth1Data                   *cltypes.Eth1Data
	eth1DataVotes              []*cltypes.Eth1Data
	eth1DepositIndex           uint64
	validators                 []*cltypes.Validator
	balances                   []uint64
	randaoMixes                [randoMixesLength]libcommon.Hash
	slashings                  [slashingsLength]uint64
	previousEpochParticipation cltypes.ParticipationFlagsList
	currentEpochParticipation  cltypes.ParticipationFlagsList
	justificationBits          cltypes.JustificationBits
	// Altair
	previousJustifiedCheckpoint *cltypes.Checkpoint
	currentJustifiedCheckpoint  *cltypes.Checkpoint
	finalizedCheckpoint         *cltypes.Checkpoint
	inactivityScores            []uint64
	currentSyncCommittee        *cltypes.SyncCommittee
	nextSyncCommittee           *cltypes.SyncCommittee
	// Bellatrix
	latestExecutionPayloadHeader *cltypes.Eth1Header
	// Capella
	nextWithdrawalIndex          uint64
	nextWithdrawalValidatorIndex uint64
	historicalSummaries          []*cltypes.HistoricalSummary
	// Phase0: genesis fork. these 2 fields replace participation bits.
	previousEpochAttestations []*cltypes.PendingAttestation
	currentEpochAttestations  []*cltypes.PendingAttestation
	// Internals
	version           clparams.StateVersion   // State version
	leaves            [32][32]byte            // Pre-computed leaves.
	touchedLeaves     map[StateLeafIndex]bool // Maps each leaf to whether they were touched or not.
	publicKeyIndicies map[[48]byte]uint64
	// Caches
	activeValidatorsCache       *lru2.Cache[uint64, []uint64]
	committeeCache              *lru2.Cache[[16]byte, []uint64]
	shuffledSetsCache           *lru2.Cache[libcommon.Hash, []uint64]
	totalActiveBalanceCache     *uint64
	totalActiveBalanceRootCache uint64
	proposerIndex               *uint64
	previousStateRoot           libcommon.Hash
	// Configs
	beaconConfig *clparams.BeaconChainConfig
}

func New(cfg *clparams.BeaconChainConfig) *BeaconState {
	state := &BeaconState{
		beaconConfig: cfg,
	}
	state.initBeaconState()
	return state
}

func preparateRootsForHashing(roots []libcommon.Hash) [][32]byte {
	ret := make([][32]byte, len(roots))
	for i := range roots {
		copy(ret[i][:], roots[i][:])
	}
	return ret
}

// MarshallSSZTo retrieve the SSZ encoded length of the state.
func (b *BeaconState) DecodeSSZ(buf []byte) error {
	panic("not implemented")
}

// BlockRoot computes the block root for the state.
func (b *BeaconState) BlockRoot() ([32]byte, error) {
	stateRoot, err := b.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	return (&cltypes.BeaconBlockHeader{
		Slot:          b.latestBlockHeader.Slot,
		ProposerIndex: b.latestBlockHeader.ProposerIndex,
		BodyRoot:      b.latestBlockHeader.BodyRoot,
		ParentRoot:    b.latestBlockHeader.ParentRoot,
		Root:          stateRoot,
	}).HashSSZ()
}

func (b *BeaconState) _refreshActiveBalances() {
	epoch := b.Epoch()
	b.totalActiveBalanceCache = new(uint64)
	*b.totalActiveBalanceCache = 0
	for _, validator := range b.validators {
		if validator.Active(epoch) {
			*b.totalActiveBalanceCache += validator.EffectiveBalance
		}
	}
	*b.totalActiveBalanceCache = utils.Max64(b.beaconConfig.EffectiveBalanceIncrement, *b.totalActiveBalanceCache)
	b.totalActiveBalanceRootCache = utils.IntegerSquareRoot(*b.totalActiveBalanceCache)
}

func (b *BeaconState) _updateProposerIndex() (err error) {
	epoch := b.Epoch()

	hash := sha256.New()
	// Input for the seed hash.
	input := b.GetSeed(epoch, clparams.MainnetBeaconConfig.DomainBeaconProposer)
	slotByteArray := make([]byte, 8)
	binary.LittleEndian.PutUint64(slotByteArray, b.slot)

	// Add slot to the end of the input.
	inputWithSlot := append(input[:], slotByteArray...)

	// Calculate the hash.
	hash.Write(inputWithSlot)
	seed := hash.Sum(nil)

	indices := b.GetActiveValidatorsIndices(epoch)

	// Write the seed to an array.
	seedArray := [32]byte{}
	copy(seedArray[:], seed)
	b.proposerIndex = new(uint64)
	*b.proposerIndex, err = b.ComputeProposerIndex(indices, seedArray)
	return
}

// _initializeValidatorsPhase0 initializes the validators matching flags based on previous/current attestations
func (b *BeaconState) _initializeValidatorsPhase0() error {
	// Previous Pending attestations
	if b.slot == 0 {
		return nil
	}
	previousEpochRoot, err := b.GetBlockRoot(b.PreviousEpoch())
	if err != nil {
		return err
	}
	for _, attestation := range b.previousEpochAttestations {
		slotRoot, err := b.GetBlockRootAtSlot(attestation.Data.Slot)
		if err != nil {
			return err
		}
		indicies, err := b.GetAttestingIndicies(attestation.Data, attestation.AggregationBits, false)
		if err != nil {
			return err
		}
		for _, index := range indicies {
			if b.validators[index].MinPreviousInclusionDelayAttestation == nil || b.validators[index].MinPreviousInclusionDelayAttestation.InclusionDelay > attestation.InclusionDelay {
				b.validators[index].MinPreviousInclusionDelayAttestation = attestation
			}
			b.validators[index].IsPreviousMatchingSourceAttester = true
			if attestation.Data.Target.Root != previousEpochRoot {
				continue
			}
			b.validators[index].IsPreviousMatchingTargetAttester = true

			if attestation.Data.BeaconBlockHash == slotRoot {
				b.validators[index].IsPreviousMatchingHeadAttester = true
			}
		}
	}

	// Current Pending attestations
	if len(b.currentEpochAttestations) == 0 {
		return nil
	}
	currentEpochRoot, err := b.GetBlockRoot(b.Epoch())
	if err != nil {
		return err
	}
	for _, attestation := range b.currentEpochAttestations {
		slotRoot, err := b.GetBlockRootAtSlot(attestation.Data.Slot)
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}
		indicies, err := b.GetAttestingIndicies(attestation.Data, attestation.AggregationBits, false)
		if err != nil {
			return err
		}
		for _, index := range indicies {
			if b.validators[index].MinCurrentInclusionDelayAttestation == nil || b.validators[index].MinCurrentInclusionDelayAttestation.InclusionDelay > attestation.InclusionDelay {
				b.validators[index].MinCurrentInclusionDelayAttestation = attestation
			}
			b.validators[index].IsCurrentMatchingSourceAttester = true
			if attestation.Data.Target.Root == currentEpochRoot {
				b.validators[index].IsCurrentMatchingTargetAttester = true
			}
			if attestation.Data.BeaconBlockHash == slotRoot {
				b.validators[index].IsCurrentMatchingHeadAttester = true
			}
		}
	}
	return nil
}

func (b *BeaconState) initBeaconState() error {

	if b.touchedLeaves == nil {
		b.touchedLeaves = make(map[StateLeafIndex]bool)
	}

	b.publicKeyIndicies = make(map[[48]byte]uint64)
	b._refreshActiveBalances()
	for i, validator := range b.validators {
		b.publicKeyIndicies[validator.PublicKey] = uint64(i)
	}
	var err error
	if b.activeValidatorsCache, err = lru2.New[uint64, []uint64](5); err != nil {
		return err
	}
	if b.shuffledSetsCache, err = lru2.New[libcommon.Hash, []uint64](25); err != nil {
		return err
	}
	if b.committeeCache, err = lru2.New[[16]byte, []uint64](256); err != nil {
		return err
	}
	if err := b._updateProposerIndex(); err != nil {
		return err
	}
	if b.version >= clparams.Phase0Version {
		return b._initializeValidatorsPhase0()
	}
	return nil
}
