package handler

import (
	"crypto/sha256"
	"encoding/binary"
	"net/http"
	"sync"

	shuffling2 "github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

type proposerDuties struct {
	Pubkey         libcommon.Bytes48 `json:"pubkey"`
	ValidatorIndex uint64            `json:"validator_index"`
	Slot           uint64            `json:"slot"`
}

func (a *ApiHandler) getDutiesProposer(r *http.Request) *beaconResponse {

	epoch, err := epochFromRequest(r)
	if err != nil {
		return newApiErrorResponse(http.StatusBadRequest, err.Error())
	}

	if epoch < a.forkchoiceStore.FinalizedCheckpoint().Epoch() {
		return newApiErrorResponse(http.StatusBadRequest, "invalid epoch")
	}

	// We need to compute our duties
	state, cancel := a.syncedData.HeadState()
	defer cancel()
	if state == nil {
		return newApiErrorResponse(http.StatusInternalServerError, "beacon node is syncing")

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
				panic(err)
			}
			var pk libcommon.Bytes48
			pk, err = state.ValidatorPublicKey(int(proposerIndex))
			if err != nil {
				panic(err)
			}
			duties[i] = proposerDuties{
				Pubkey:         pk,
				ValidatorIndex: proposerIndex,
				Slot:           slot,
			}
		}(slot-expectedSlot, slot, indices, seedArray)
	}
	wg.Wait()

	return newBeaconResponse(duties).withFinalized(false).withVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch))

}
