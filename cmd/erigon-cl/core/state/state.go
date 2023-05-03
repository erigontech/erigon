package state

import (
	"crypto/sha256"
	"encoding/binary"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/raw"
)

type HashFunc func([]byte) ([32]byte, error)

type BeaconState struct {
	*raw.BeaconState

	// Internals
	publicKeyIndicies map[[48]byte]uint64
	// Caches
	activeValidatorsCache       *lru.Cache[uint64, []uint64]
	committeeCache              *lru.Cache[[16]byte, []uint64]
	shuffledSetsCache           *lru.Cache[common.Hash, []uint64]
	totalActiveBalanceCache     *uint64
	totalActiveBalanceRootCache uint64
	proposerIndex               *uint64
	previousStateRoot           common.Hash
	// Configs
}

func New(cfg *clparams.BeaconChainConfig) *BeaconState {
	state := &BeaconState{
		BeaconState: raw.New(cfg),
	}
	state.initBeaconState()
	return state
}

func NewFromRaw(r *raw.BeaconState) *BeaconState {
	state := &BeaconState{
		BeaconState: r,
	}
	state.initBeaconState()
	return state
}

func (b *BeaconState) SetPreviousStateRoot(root libcommon.Hash) {
	b.previousStateRoot = root
}

// MarshallSSZTo retrieve the SSZ encoded length of the state.
func (b *BeaconState) DecodeSSZ(buf []byte) error {
	panic("not implemented")
}

func (b *BeaconState) _updateProposerIndex() (err error) {
	epoch := b.Epoch()

	hash := sha256.New()
	// Input for the seed hash.
	randao := b.RandaoMixes()
	input := GetSeed(b.BeaconConfig(), randao[:], epoch, b.BeaconConfig().DomainBeaconProposer)
	slotByteArray := make([]byte, 8)
	binary.LittleEndian.PutUint64(slotByteArray, b.Slot())

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
	if b.Slot() == 0 {
		return nil
	}
	previousEpochRoot, err := b.GetBlockRoot(b.PreviousEpoch())
	if err != nil {
		return err
	}
	for _, attestation := range b.PreviousEpochAttestations() {
		slotRoot, err := b.GetBlockRootAtSlot(attestation.Data.Slot)
		if err != nil {
			return err
		}
		indicies, err := b.GetAttestingIndicies(attestation.Data, attestation.AggregationBits, false)
		if err != nil {
			return err
		}
		for _, index := range indicies {
			previousMinAttestationDelay, err := b.ValidatorMinPreviousInclusionDelayAttestation(int(index))
			if err != nil {
				return err
			}
			if previousMinAttestationDelay == nil || previousMinAttestationDelay.InclusionDelay > attestation.InclusionDelay {
				if err := b.SetValidatorMinPreviousInclusionDelayAttestation(int(index), attestation); err != nil {
					return err
				}
			}
			if err := b.SetValidatorIsPreviousMatchingSourceAttester(int(index), true); err != nil {
				return err
			}
			if attestation.Data.Target.Root != previousEpochRoot {
				continue
			}
			if err := b.SetValidatorIsPreviousMatchingTargetAttester(int(index), true); err != nil {
				return err
			}
			if attestation.Data.BeaconBlockHash == slotRoot {
				if err := b.SetValidatorIsPreviousMatchingHeadAttester(int(index), true); err != nil {
					return err
				}
			}
		}
	}
	// Current Pending attestations
	if b.CurrentEpochAttestationsLength() == 0 {
		return nil
	}
	currentEpochRoot, err := b.GetBlockRoot(b.Epoch())
	if err != nil {
		return err
	}
	for _, attestation := range b.CurrentEpochAttestations() {
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
			currentMinAttestationDelay, err := b.ValidatorMinCurrentInclusionDelayAttestation(int(index))
			if err != nil {
				return err
			}
			if currentMinAttestationDelay == nil || currentMinAttestationDelay.InclusionDelay > attestation.InclusionDelay {
				if err := b.SetValidatorMinCurrentInclusionDelayAttestation(int(index), attestation); err != nil {
					return err
				}
			}
			if err := b.SetValidatorIsCurrentMatchingSourceAttester(int(index), true); err != nil {
				return err
			}
			if attestation.Data.Target.Root == currentEpochRoot {
				if err := b.SetValidatorIsCurrentMatchingTargetAttester(int(index), true); err != nil {
					return err
				}
			}
			if attestation.Data.BeaconBlockHash == slotRoot {
				if err := b.SetValidatorIsCurrentMatchingHeadAttester(int(index), true); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (b *BeaconState) _refreshActiveBalances() {
	epoch := b.Epoch()
	b.totalActiveBalanceCache = new(uint64)
	*b.totalActiveBalanceCache = 0
	b.ForEachValidator(func(validator *cltypes.Validator, idx, total int) bool {
		if validator.Active(epoch) {
			*b.totalActiveBalanceCache += validator.EffectiveBalance
		}
		return true
	})
	*b.totalActiveBalanceCache = utils.Max64(b.BeaconConfig().EffectiveBalanceIncrement, *b.totalActiveBalanceCache)
	b.totalActiveBalanceRootCache = utils.IntegerSquareRoot(*b.totalActiveBalanceCache)
}

func (b *BeaconState) initCaches() error {
	var err error
	if b.activeValidatorsCache, err = lru.New[uint64, []uint64](5); err != nil {
		return err
	}
	if b.shuffledSetsCache, err = lru.New[common.Hash, []uint64](5); err != nil {
		return err
	}
	if b.committeeCache, err = lru.New[[16]byte, []uint64](256); err != nil {
		return err
	}
	return nil
}

func (b *BeaconState) initBeaconState() error {
	b.publicKeyIndicies = make(map[[48]byte]uint64)
	b._refreshActiveBalances()
	b.initCaches()
	b.ForEachValidator(func(validator *cltypes.Validator, i, total int) bool {
		b.publicKeyIndicies[validator.PublicKey] = uint64(i)
		return true
	})
	if err := b._updateProposerIndex(); err != nil {
		return err
	}
	if b.Version() >= clparams.Phase0Version {
		return b._initializeValidatorsPhase0()
	}

	return nil
}

func (b *BeaconState) Copy() (*BeaconState, error) {
	copied := New(b.BeaconConfig())

	copied.BeaconState = raw.New(b.BeaconConfig())
	// Fill all the fields with copies

	copied.publicKeyIndicies = make(map[[48]byte]uint64)
	for pk, index := range b.publicKeyIndicies {
		copied.publicKeyIndicies[pk] = index
	}
	// Sync caches
	if err := copied.initCaches(); err != nil {
		return nil, err
	}
	for _, epoch := range b.activeValidatorsCache.Keys() {
		val, has := b.activeValidatorsCache.Get(epoch)
		if !has {
			continue
		}
		copied.activeValidatorsCache.Add(epoch, val)
	}
	for _, key := range b.shuffledSetsCache.Keys() {
		val, has := b.shuffledSetsCache.Get(key)
		if !has {
			continue
		}
		copied.shuffledSetsCache.Add(key, val)
	}
	for _, key := range b.committeeCache.Keys() {
		val, has := b.committeeCache.Get(key)
		if !has {
			continue
		}
		copied.committeeCache.Add(key, val)
	}
	if b.totalActiveBalanceCache != nil {
		copied.totalActiveBalanceCache = new(uint64)
		*copied.totalActiveBalanceCache = *b.totalActiveBalanceCache
		copied.totalActiveBalanceRootCache = b.totalActiveBalanceRootCache
	}

	return copied, nil
}
