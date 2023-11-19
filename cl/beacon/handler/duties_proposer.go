package handler

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/http"
	"sync"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	shuffling2 "github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
)

type proposerDuties struct {
	Pubkey         libcommon.Bytes48 `json:"pubkey"`
	ValidatorIndex uint64            `json:"validator_index"`
	Slot           uint64            `json:"slot"`
}

// The proposer knight respects its duties and hands over which proposer should be proposing in which slot.
type proposerKnight struct {
	// The proposer knight's duties.
	dutiesCache *lru.Cache[uint64, []proposerDuties]
}

func (a *ApiHandler) getDutiesProposer(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	if a.dutiesCache == nil {
		a.dutiesCache, err = lru.New[uint64, []proposerDuties]("proposerKnight", 32)
	}
	if err != nil {
		httpStatus = http.StatusInternalServerError
		return
	}
	var epoch uint64

	epoch, err = epochFromRequest(r)
	if err != nil {
		httpStatus = http.StatusBadRequest
		return
	}

	if epoch < a.forkchoiceStore.FinalizedCheckpoint().Epoch() {
		err = fmt.Errorf("invalid epoch")
		httpStatus = http.StatusBadRequest
		return
	}

	// We need to compute our duties
	state, cancel := a.syncedData.HeadState()
	defer cancel()
	if state == nil {
		err = fmt.Errorf("node is syncing")
		httpStatus = http.StatusInternalServerError
		return
	}

	expectedSlot := epoch * a.beaconChainCfg.SlotsPerEpoch

	duties := make([]proposerDuties, a.beaconChainCfg.SlotsPerEpoch)
	wg := sync.WaitGroup{}

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
		wg.Add(1)

		// Do it in parallel
		go func(i, slot uint64, indicies []uint64, seedArray [32]byte) {
			defer wg.Done()
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
			duties[i] = proposerDuties{
				Pubkey:         pk,
				ValidatorIndex: proposerIndex,
				Slot:           slot,
			}
		}(slot-expectedSlot, slot, indices, seedArray)
	}
	wg.Wait()
	a.dutiesCache.Add(epoch, duties)
	data = duties
	finalized = new(bool)
	*finalized = false
	httpStatus = http.StatusAccepted
	return

}
