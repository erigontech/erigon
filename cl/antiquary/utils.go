package antiquary

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"
)

func getProposerDutiesValue(s *state.CachingBeaconState) []byte {
	epoch := state.Epoch(s)
	var wg sync.WaitGroup
	list := make([]byte, s.BeaconConfig().SlotsPerEpoch*4)
	for slot := s.Slot(); slot < s.Slot()+s.BeaconConfig().SlotsPerEpoch; slot++ {
		var proposerIndex uint64
		// Lets do proposer index computation
		mixPosition := (epoch + s.BeaconConfig().EpochsPerHistoricalVector - s.BeaconConfig().MinSeedLookahead - 1) %
			s.BeaconConfig().EpochsPerHistoricalVector
		// Input for the seed hash.
		mix := s.GetRandaoMix(int(mixPosition))
		input := shuffling.GetSeed(s.BeaconConfig(), mix, epoch, s.BeaconConfig().DomainBeaconProposer)
		slotByteArray := make([]byte, 8)
		binary.LittleEndian.PutUint64(slotByteArray, slot)

		// Add slot to the end of the input.
		inputWithSlot := append(input[:], slotByteArray...)
		hash := sha256.New()

		// Calculate the hash.
		hash.Write(inputWithSlot)
		seed := hash.Sum(nil)

		indices := s.GetActiveValidatorsIndices(epoch)

		// Write the seed to an array.
		seedArray := [32]byte{}
		copy(seedArray[:], seed)
		wg.Add(1)

		// Do it in parallel
		go func(i, slot uint64, indicies []uint64, seedArray [32]byte) {
			defer wg.Done()
			var err error
			proposerIndex, err = shuffling.ComputeProposerIndex(s.BeaconState, indices, seedArray)
			if err != nil {
				panic(err)
			}
			binary.BigEndian.PutUint32(list[i*4:(i+1)*4], uint32(proposerIndex))
		}(slot-s.Slot(), slot, indices, seedArray)
	}
	wg.Wait()
	return list
}

// func (s *Antiquary) dumpFullBeaconState() {
// 	b, err := s.currentState.EncodeSSZ(nil)
// 	if err != nil {
// 		s.logger.Error("Failed to encode full beacon state", "err", err)
// 		return
// 	}
// 	// just dump it in a.txt like an idiot without afero
// 	if err := os.WriteFile("bab.txt", b, 0644); err != nil {
// 		s.logger.Error("Failed to write full beacon state", "err", err)
// 	}
// }

func findNearestSlotBackwards(tx kv.Tx, cfg *clparams.BeaconChainConfig, slot uint64) (uint64, error) {
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
	if err != nil {
		return 0, err
	}
	for (canonicalRoot == (common.Hash{}) && slot > 0) || slot%cfg.SlotsPerEpoch != 0 {
		slot--
		canonicalRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return 0, err
		}
	}
	return slot, nil
}
