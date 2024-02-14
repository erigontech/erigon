package state

import (
	"crypto/sha256"
	"encoding/binary"
	"io"
	"math"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/raw"
	shuffling2 "github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
)

const (
	shuffledSetsCacheSize     = 5
	activeValidatorsCacheSize = 5
)

type HashFunc func([]byte) ([32]byte, error)

// CachingBeaconState is a cached wrapper around a raw CachingBeaconState provider
type CachingBeaconState struct {
	// embedded BeaconState
	*raw.BeaconState

	// Internals
	publicKeyIndicies map[[48]byte]uint64
	// Caches
	activeValidatorsCache *lru.Cache[uint64, []uint64]
	shuffledSetsCache     *lru.Cache[common.Hash, []uint64]

	totalActiveBalanceCache     *uint64
	totalActiveBalanceRootCache uint64
	proposerIndex               *uint64
	previousStateRoot           common.Hash
}

func New(cfg *clparams.BeaconChainConfig) *CachingBeaconState {
	state := &CachingBeaconState{
		BeaconState: raw.New(cfg),
	}
	state.InitBeaconState()
	return state
}

func NewFromRaw(r *raw.BeaconState) *CachingBeaconState {
	state := &CachingBeaconState{
		BeaconState: r,
	}
	state.InitBeaconState()
	return state
}

func (b *CachingBeaconState) SetPreviousStateRoot(root libcommon.Hash) {
	b.previousStateRoot = root
}

func (b *CachingBeaconState) _updateProposerIndex() (err error) {
	epoch := Epoch(b)

	hash := sha256.New()
	beaconConfig := b.BeaconConfig()
	mixPosition := (epoch + beaconConfig.EpochsPerHistoricalVector - beaconConfig.MinSeedLookahead - 1) %
		beaconConfig.EpochsPerHistoricalVector
	// Input for the seed hash.
	mix := b.GetRandaoMix(int(mixPosition))
	input := shuffling2.GetSeed(b.BeaconConfig(), mix, epoch, b.BeaconConfig().DomainBeaconProposer)
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
	*b.proposerIndex, err = shuffling2.ComputeProposerIndex(b.BeaconState, indices, seedArray)
	return
}

// _initializeValidatorsPhase0 initializes the validators matching flags based on previous/current attestations
func (b *CachingBeaconState) _initializeValidatorsPhase0() error {
	// Previous Pending attestations
	if b.Slot() == 0 {
		return nil
	}

	previousEpochRoot, err := GetBlockRoot(b, PreviousEpoch(b))
	if err != nil {
		return err
	}

	if err := solid.RangeErr[*solid.PendingAttestation](b.PreviousEpochAttestations(), func(i1 int, pa *solid.PendingAttestation, _ int) error {
		attestationData := pa.AttestantionData()
		slotRoot, err := b.GetBlockRootAtSlot(attestationData.Slot())
		if err != nil {
			return err
		}
		indicies, err := b.GetAttestingIndicies(attestationData, pa.AggregationBits(), false)
		if err != nil {
			return err
		}
		for _, index := range indicies {
			previousMinAttestationDelay, err := b.ValidatorMinPreviousInclusionDelayAttestation(int(index))
			if err != nil {
				return err
			}
			if previousMinAttestationDelay == nil || previousMinAttestationDelay.InclusionDelay() > pa.InclusionDelay() {
				if err := b.SetValidatorMinPreviousInclusionDelayAttestation(int(index), pa); err != nil {
					return err
				}
			}
			if err := b.SetValidatorIsPreviousMatchingSourceAttester(int(index), true); err != nil {
				return err
			}
			if attestationData.Target().BlockRoot() != previousEpochRoot {
				continue
			}
			if err := b.SetValidatorIsPreviousMatchingTargetAttester(int(index), true); err != nil {
				return err
			}
			if attestationData.BeaconBlockRoot() == slotRoot {
				if err := b.SetValidatorIsPreviousMatchingHeadAttester(int(index), true); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	// Current Pending attestations
	if b.CurrentEpochAttestationsLength() == 0 {
		return nil
	}
	currentEpochRoot, err := GetBlockRoot(b, Epoch(b))
	if err != nil {
		return err
	}
	return solid.RangeErr[*solid.PendingAttestation](b.CurrentEpochAttestations(), func(i1 int, pa *solid.PendingAttestation, _ int) error {
		attestationData := pa.AttestantionData()
		slotRoot, err := b.GetBlockRootAtSlot(attestationData.Slot())
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}
		indicies, err := b.GetAttestingIndicies(attestationData, pa.AggregationBits(), false)
		if err != nil {
			return err
		}
		for _, index := range indicies {
			currentMinAttestationDelay, err := b.ValidatorMinCurrentInclusionDelayAttestation(int(index))
			if err != nil {
				return err
			}
			if currentMinAttestationDelay == nil || currentMinAttestationDelay.InclusionDelay() > pa.InclusionDelay() {
				if err := b.SetValidatorMinCurrentInclusionDelayAttestation(int(index), pa); err != nil {
					return err
				}
			}
			if err := b.SetValidatorIsCurrentMatchingSourceAttester(int(index), true); err != nil {
				return err
			}
			if attestationData.Target().BlockRoot() == currentEpochRoot {
				if err := b.SetValidatorIsCurrentMatchingTargetAttester(int(index), true); err != nil {
					return err
				}
			}
			if attestationData.BeaconBlockRoot() == slotRoot {
				if err := b.SetValidatorIsCurrentMatchingHeadAttester(int(index), true); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (b *CachingBeaconState) _refreshActiveBalances() {
	epoch := Epoch(b)
	b.totalActiveBalanceCache = new(uint64)
	*b.totalActiveBalanceCache = 0
	b.ForEachValidator(func(validator solid.Validator, idx, total int) bool {
		if validator.Active(epoch) {
			*b.totalActiveBalanceCache += validator.EffectiveBalance()
		}
		return true
	})
	*b.totalActiveBalanceCache = utils.Max64(b.BeaconConfig().EffectiveBalanceIncrement, *b.totalActiveBalanceCache)
	b.totalActiveBalanceRootCache = utils.IntegerSquareRoot(*b.totalActiveBalanceCache)
}

func (b *CachingBeaconState) initCaches() error {
	var err error
	if b.activeValidatorsCache, err = lru.New[uint64, []uint64]("beacon_active_validators_cache", activeValidatorsCacheSize); err != nil {
		return err
	}
	if b.shuffledSetsCache, err = lru.New[common.Hash, []uint64]("beacon_shuffled_sets_cache", shuffledSetsCacheSize); err != nil {
		return err
	}

	return nil
}

func (b *CachingBeaconState) InitBeaconState() error {
	b._refreshActiveBalances()

	b.publicKeyIndicies = make(map[[48]byte]uint64)

	b.ForEachValidator(func(validator solid.Validator, i, total int) bool {
		b.publicKeyIndicies[validator.PublicKey()] = uint64(i)

		return true
	})

	b.initCaches()
	if err := b._updateProposerIndex(); err != nil {
		return err
	}

	if b.Version() >= clparams.Phase0Version {
		return b._initializeValidatorsPhase0()
	}

	return nil
}

// EncodeCaches, encodes the beacon state caches into a byte slice
func (b *CachingBeaconState) EncodeCaches(w io.Writer) error {
	num := make([]byte, 8)
	// activeValidatorsCaches
	if err := b.encodeActiveValidatorsCache(w, num); err != nil {
		return err
	}
	// shuffledSetsCache
	if err := b.encodeShuffledSetsCache(w, num); err != nil {
		return err
	}
	// Now do the extra caches
	if b.totalActiveBalanceCache == nil {
		if err := binary.Write(w, binary.BigEndian, uint64(math.MaxUint64)); err != nil {
			return err
		}
	} else {
		if err := binary.Write(w, binary.BigEndian, *b.totalActiveBalanceCache); err != nil {
			return err
		}
	}
	if err := binary.Write(w, binary.BigEndian, b.totalActiveBalanceRootCache); err != nil {
		return err
	}
	if b.proposerIndex == nil {
		if err := binary.Write(w, binary.BigEndian, uint64(math.MaxUint64)); err != nil {
			return err
		}
	} else {
		if err := binary.Write(w, binary.BigEndian, *b.proposerIndex); err != nil {
			return err
		}
	}
	if _, err := w.Write(b.previousStateRoot[:]); err != nil {
		return err
	}
	return nil
}

func (b *CachingBeaconState) DecodeCaches(r io.Reader) error {
	num := make([]byte, 8)
	// activeValidatorsCaches
	if err := b.decodeActiveValidatorsCache(r, num); err != nil {
		return err
	}
	// shuffledSetsCache
	if err := b.decodeShuffledSetsCache(r, num); err != nil {
		return err
	}
	// Now do the extra caches
	var totalActiveBalanceCache uint64
	if err := binary.Read(r, binary.BigEndian, &totalActiveBalanceCache); err != nil {
		return err
	}
	if totalActiveBalanceCache == math.MaxUint64 {
		b.totalActiveBalanceCache = nil
	} else {
		b.totalActiveBalanceCache = &totalActiveBalanceCache
	}
	if err := binary.Read(r, binary.BigEndian, &b.totalActiveBalanceRootCache); err != nil {
		return err
	}
	var proposerIndex uint64
	if err := binary.Read(r, binary.BigEndian, &proposerIndex); err != nil {
		return err
	}
	if proposerIndex == math.MaxUint64 {
		b.proposerIndex = nil
	} else {
		b.proposerIndex = &proposerIndex
	}
	if _, err := r.Read(b.previousStateRoot[:]); err != nil {
		return err
	}
	return nil
}

func writeUint64WithBuffer(w io.Writer, num uint64, buf []byte) error {
	binary.BigEndian.PutUint64(buf, num)
	if _, err := w.Write(buf); err != nil {
		return err
	}
	return nil
}

func readUint64WithBuffer(r io.Reader, buf []byte, out *uint64) error {
	if _, err := r.Read(buf); err != nil {
		return err
	}
	*out = binary.BigEndian.Uint64(buf)
	return nil
}

// internal encoding/decoding algos
func (b *CachingBeaconState) encodeActiveValidatorsCache(w io.Writer, num []byte) error {
	keysA := b.activeValidatorsCache.Keys()
	keys := make([]uint64, 0, len(keysA))
	lists := make([][]uint64, 0, len(keys))
	for _, key := range keysA {
		l, ok := b.activeValidatorsCache.Get(key)
		if !ok || len(l) == 0 {
			continue
		}
		keys = append(keys, key)
		lists = append(lists, l)
	}
	// Write the total length
	if err := writeUint64WithBuffer(w, uint64(len(keys)), num); err != nil {
		return err
	}

	for i, key := range keys {
		if err := writeUint64WithBuffer(w, uint64(len(lists[i])), num); err != nil {
			return err
		}
		if err := writeUint64WithBuffer(w, key, num); err != nil {
			return err
		}
		for _, v := range lists[i] {
			if err := writeUint64WithBuffer(w, v, num); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *CachingBeaconState) decodeActiveValidatorsCache(r io.Reader, num []byte) error {
	var err error
	b.activeValidatorsCache, err = lru.New[uint64, []uint64]("beacon_active_validators_cache", activeValidatorsCacheSize)
	if err != nil {
		return err
	}
	var l uint64

	if err := readUint64WithBuffer(r, num, &l); err != nil {
		return err
	}
	for i := 0; i < int(l); i++ {
		var l uint64

		if err := readUint64WithBuffer(r, num, &l); err != nil {
			return err
		}
		var key uint64
		if err := readUint64WithBuffer(r, num, &key); err != nil {
			return err
		}
		list := make([]uint64, l)
		for i := 0; i < int(l); i++ {
			if err := readUint64WithBuffer(r, num, &list[i]); err != nil {
				return err
			}
		}
		b.activeValidatorsCache.Add(key, list)
	}
	return nil
}

// internal encoding/decoding algos
func (b *CachingBeaconState) encodeShuffledSetsCache(w io.Writer, num []byte) error {
	keysA := b.shuffledSetsCache.Keys()
	keys := make([]common.Hash, 0, len(keysA))
	lists := make([][]uint64, 0, len(keys))

	for _, key := range keysA {
		l, ok := b.shuffledSetsCache.Get(key)
		if !ok || len(l) == 0 {
			continue
		}
		keys = append(keys, key)
		lists = append(lists, l)
	}
	// Write the total length
	if err := writeUint64WithBuffer(w, uint64(len(keys)), num); err != nil {
		return err
	}
	for i, key := range keys {
		if err := writeUint64WithBuffer(w, uint64(len(lists[i])), num); err != nil {
			return err
		}
		if _, err := w.Write(key[:]); err != nil {
			return err
		}
		for _, v := range lists[i] {
			if err := writeUint64WithBuffer(w, v, num); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *CachingBeaconState) decodeShuffledSetsCache(r io.Reader, num []byte) error {
	var err error
	b.shuffledSetsCache, err = lru.New[common.Hash, []uint64]("beacon_shuffled_sets_cache", shuffledSetsCacheSize)
	if err != nil {
		return err
	}

	var l uint64
	if err := readUint64WithBuffer(r, num, &l); err != nil {
		return err
	}
	for i := 0; i < int(l); i++ {
		var l uint64
		if err := readUint64WithBuffer(r, num, &l); err != nil {
			return err
		}
		var key common.Hash
		if _, err := r.Read(key[:]); err != nil {
			return err
		}
		list := make([]uint64, l)
		for i := 0; i < int(l); i++ {
			if err := readUint64WithBuffer(r, num, &list[i]); err != nil {
				return err
			}
		}
		b.shuffledSetsCache.Add(key, list)
	}

	return nil
}
