package monitor

import (
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

type validatorMonitorImpl struct {
	syncedData       *synced_data.SyncedDataManager
	ethClock         eth_clock.EthereumClock
	beaconCfg        *clparams.BeaconChainConfig
	vaidatorStatuses *validatorStatuses // map validatorID -> epoch -> validatorStatus
}

func NewValidatorMonitor(
	enableMonitor bool,
	ethClock eth_clock.EthereumClock,
	beaconConfig *clparams.BeaconChainConfig,
	syncedData *synced_data.SyncedDataManager,
) ValidatorMonitor {
	if !enableMonitor {
		return &dummyValdatorMonitor{}
	}

	m := &validatorMonitorImpl{
		ethClock:         ethClock,
		beaconCfg:        beaconConfig,
		syncedData:       syncedData,
		vaidatorStatuses: newValidatorStatuses(),
	}
	go m.runReportAttesterStatus()
	go m.runReportProposerStatus()
	return m
}

func (m *validatorMonitorImpl) ObserveValidator(vid uint64) {
	m.vaidatorStatuses.addValidator(vid)
}

func (m *validatorMonitorImpl) RemoveValidator(vid uint64) {
	m.vaidatorStatuses.removeValidator(vid)
}

func (m *validatorMonitorImpl) OnNewBlock(state *state.CachingBeaconState, block *cltypes.BeaconBlock) error {
	var (
		atts         = block.Body.Attestations
		blockEpoch   = m.ethClock.GetEpochAtSlot(block.Slot)
		currentEpoch = m.ethClock.GetCurrentEpoch()
	)
	if blockEpoch+2 < currentEpoch {
		// skip old blocks
		return nil
	}

	// todo: maybe launch a goroutine to update attester status
	// update attester status
	atts.Range(func(i int, att *solid.Attestation, length int) bool {
		indicies, err := state.GetAttestingIndicies(att, true)
		if err != nil {
			log.Warn("failed to get attesting indicies", "err", err, "slot", block.Slot, "stateRoot", block.StateRoot)
			return false
		}
		slot := att.Data.Slot
		attEpoch := m.ethClock.GetEpochAtSlot(slot)
		for _, vidx := range indicies {
			status := m.vaidatorStatuses.getValidatorStatus(vidx, attEpoch)
			if status == nil {
				continue
			}
			status.updateAttesterStatus(att)
		}
		return true
	})
	// update proposer status
	pIndex := block.ProposerIndex
	if status := m.vaidatorStatuses.getValidatorStatus(pIndex, blockEpoch); status != nil {
		status.proposeSlots.Add(block.Slot)
	}

	return nil
}

func (m *validatorMonitorImpl) runReportAttesterStatus() {
	// every epoch seconds
	epochDuration := time.Duration(m.beaconCfg.SlotsPerEpoch) * time.Duration(m.beaconCfg.SecondsPerSlot) * time.Second
	ticker := time.NewTicker(epochDuration)
	for range ticker.C {
		currentEpoch := m.ethClock.GetCurrentEpoch()
		// report attester status for current_epoch - 2
		epoch := currentEpoch - 2
		hitCount := 0
		missCount := 0
		m.vaidatorStatuses.iterate(func(vindex uint64, epochStatuses map[uint64]*validatorStatus) {
			if status, ok := epochStatuses[epoch]; ok {
				successAtt := status.attestedBlockRoots.Cardinality()
				metricAttestHit.AddInt(successAtt)
				hitCount += successAtt
				delete(epochStatuses, epoch)
				log.Debug("[monitor] report attester status hit", "epoch", epoch, "vindex", vindex, "countAttestedBlock", status.attestedBlockRoots.Cardinality())
			} else {
				metricAttestMiss.AddInt(1)
				missCount++
				log.Debug("[monitor] report attester status miss", "epoch", epoch, "vindex", vindex, "countAttestedBlock", 0)
			}
		})
		log.Info("[monitor] report attester hit/miss", "epoch", epoch, "hitCount", hitCount, "missCount", missCount, "cur_epoch", currentEpoch)
	}

}

func (m *validatorMonitorImpl) runReportProposerStatus() {
	// check proposer in previous slot every slot duration
	ticker := time.NewTicker(time.Duration(m.beaconCfg.SecondsPerSlot) * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		prevSlot := m.ethClock.GetCurrentSlot() - 1

		var proposerIndex uint64
		if err := m.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) (err error) {
			proposerIndex, err = headState.GetBeaconProposerIndexForSlot(prevSlot)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			log.Warn("failed to get proposer index", "err", err, "slot", prevSlot)
			continue
		}
		// check proposer in previous slot

		if status := m.vaidatorStatuses.getValidatorStatus(proposerIndex, prevSlot/m.beaconCfg.SlotsPerEpoch); status != nil {
			if status.proposeSlots.Contains(prevSlot) {
				metricProposerHit.AddInt(1)
				log.Warn("[monitor] proposer hit", "slot", prevSlot, "proposerIndex", proposerIndex)
			} else {
				metricProposerMiss.AddInt(1)
				log.Warn("[monitor] proposer miss", "slot", prevSlot, "proposerIndex", proposerIndex)
			}
		}
	}
}

type validatorStatus struct {
	// attestedBlockRoots is the set of block roots that the validator has successfully attested during one epoch.
	attestedBlockRoots mapset.Set[common.Hash]
	// proposeSlots is the set of slots that the proposer has successfully proposed blocks during one epoch.
	proposeSlots mapset.Set[uint64]
}

func (s *validatorStatus) updateAttesterStatus(att *solid.Attestation) {
	data := att.Data
	s.attestedBlockRoots.Add(data.BeaconBlockRoot)
}

type validatorStatuses struct {
	statuses     map[uint64]map[uint64]*validatorStatus
	vStatusMutex sync.RWMutex
}

func newValidatorStatuses() *validatorStatuses {
	return &validatorStatuses{
		statuses: make(map[uint64]map[uint64]*validatorStatus),
	}
}

// getValidatorStatus returns the validator status for the given validator index and epoch.
// returns nil if validator is not observed.
func (s *validatorStatuses) getValidatorStatus(vid uint64, epoch uint64) *validatorStatus {
	s.vStatusMutex.Lock()
	defer s.vStatusMutex.Unlock()
	statusByEpoch, ok := s.statuses[vid]
	if !ok {
		return nil
	}
	if _, ok := statusByEpoch[epoch]; !ok {
		statusByEpoch[epoch] = &validatorStatus{
			attestedBlockRoots: mapset.NewSet[common.Hash](),
			proposeSlots:       mapset.NewSet[uint64](),
		}
	}

	return statusByEpoch[epoch]
}

func (s *validatorStatuses) addValidator(vid uint64) {
	s.vStatusMutex.Lock()
	defer s.vStatusMutex.Unlock()
	if _, ok := s.statuses[vid]; !ok {
		s.statuses[vid] = make(map[uint64]*validatorStatus)
		log.Trace("[monitor] add validator", "vid", vid)
	}
}

func (s *validatorStatuses) removeValidator(vid uint64) {
	s.vStatusMutex.Lock()
	defer s.vStatusMutex.Unlock()
	if _, ok := s.statuses[vid]; ok {
		delete(s.statuses, vid)
		log.Trace("[monitor] remove validator", "vid", vid)
	}
}

func (s *validatorStatuses) iterate(run func(vid uint64, statuses map[uint64]*validatorStatus)) {
	s.vStatusMutex.Lock()
	defer s.vStatusMutex.Unlock()
	for vid, statuses := range s.statuses {
		run(vid, statuses)
	}
}
