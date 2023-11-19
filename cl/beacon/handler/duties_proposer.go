package handler

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	shuffling2 "github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/transition"
)

type proposerDuties struct {
	Pubkey         libcommon.Bytes48 `json:"pubkey"`
	ValidatorIndex uint64            `json:"validator_index"`
	Slot           uint64            `json:"slot"`
}

type proposerKnightCacheEntry struct {
	epoch     uint64
	blockRoot libcommon.Hash
}

// The proposer knight respects its duties and hands over which proposer should be proposing in which slot.
type proposerKnight struct {
	// The proposer knight's duties.
	dutiesCache *lru.Cache[proposerKnightCacheEntry, []proposerDuties]
}

func (a *ApiHandler) getDutiesProposer(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	if a.dutiesCache == nil {
		a.dutiesCache, err = lru.New[proposerKnightCacheEntry, []proposerDuties]("proposerKnight", 32)
	}
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	var epoch uint64
	var tx kv.Tx

	// decode {epoch} url param to uint64
	epoch, err = epochFromRequest(r)
	if err != nil {
		httpStatus = http.StatusBadRequest
		return
	}
	if epoch >= (a.forkchoiceStore.Slot()/a.beaconChainCfg.SlotsPerEpoch)+1 {
		err = fmt.Errorf("invalid epoch")
		httpStatus = http.StatusBadRequest
		return
	}
	startingSlot := epoch * a.beaconChainCfg.SlotsPerEpoch
	// There are 2 cases: finalized epoch and non-finalized epoch
	if a.forkchoiceStore.FinalizedCheckpoint().Epoch() >= epoch {
		err = fmt.Errorf("invalid epoch")
		httpStatus = http.StatusNotImplemented
		return
	}
	// We are looking at a non-finalized epoch
	var blockRoot libcommon.Hash
	var state *state.CachingBeaconState
	countdown := 256
	tx, err = a.indiciesDB.BeginRo(r.Context())
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	defer tx.Rollback()
	// We need to find the starting slot to fetch the state from
	for blockRoot == (libcommon.Hash{}) {
		blockRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, startingSlot)
		if err != nil {
			httpStatus = http.StatusInternalServerError
			return
		}
		countdown--
		if countdown == 0 {
			httpStatus = http.StatusNotFound
			return
		}
		startingSlot--
	}
	// lets see if our knight has our duties
	cacheEntry := proposerKnightCacheEntry{
		epoch:     epoch,
		blockRoot: blockRoot,
	}
	duties, ok := a.dutiesCache.Get(cacheEntry)
	if ok {
		data = duties
		finalized = new(bool)
		*finalized = false
		httpStatus = http.StatusAccepted
		return
	}
	// We need to compute our duties
	state, err = a.forkchoiceStore.GetFullState(blockRoot, true)
	if err != nil {
		httpStatus = http.StatusNotFound
		return
	}

	expectedSlot := epoch * a.beaconChainCfg.SlotsPerEpoch
	if expectedSlot > state.Slot() {
		if err = transition.DefaultMachine.ProcessSlots(state, expectedSlot); err != nil {
			httpStatus = http.StatusInternalServerError
			return
		}
	}
	duties = make([]proposerDuties, 0, a.beaconChainCfg.SlotsPerEpoch)
	for slot := expectedSlot; slot < expectedSlot+a.beaconChainCfg.SlotsPerEpoch; slot++ {
		var proposerIndex uint64
		// Lets do proposer index computation
		mixPosition := (epoch + a.beaconChainCfg.EpochsPerHistoricalVector - a.beaconChainCfg.MinSeedLookahead - 1) %
			a.beaconChainCfg.EpochsPerHistoricalVector
		// Input for the seed hash.
		mix := state.GetRandaoMix(int(mixPosition))
		input := shuffling2.GetSeed(a.beaconChainCfg, mix, epoch, a.beaconChainCfg.DomainBeaconProposer)
		slotByteArray := make([]byte, 8)
		binary.LittleEndian.PutUint64(slotByteArray, slot)

		// Add slot to the end of the input.
		inputWithSlot := append(input[:], slotByteArray...)
		hash := sha256.New()

		// Calculate the hash.
		hash.Write(inputWithSlot)
		seed := hash.Sum(nil)

		indices := state.GetActiveValidatorsIndices(epoch)

		// Write the seed to an array.
		seedArray := [32]byte{}
		copy(seedArray[:], seed)
		proposerIndex, err = shuffling2.ComputeProposerIndex(state.BeaconState, indices, seedArray)
		if err != nil {
			httpStatus = http.StatusInternalServerError
			return
		}
		var pk libcommon.Bytes48
		pk, err = state.ValidatorPublicKey(int(proposerIndex))
		if err != nil {
			httpStatus = http.StatusInternalServerError
			return
		}
		duties = append(duties, proposerDuties{
			Pubkey:         pk,
			ValidatorIndex: proposerIndex,
			Slot:           slot,
		})
	}
	a.dutiesCache.Add(cacheEntry, duties)
	data = duties
	finalized = new(bool)
	*finalized = false
	httpStatus = http.StatusAccepted
	return

}
