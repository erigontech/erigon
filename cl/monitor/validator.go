package monitor

import (
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

var (
	// metrics
	metricAttestHit    = metrics.GetOrCreateCounter("validator_attestation_hit")
	metricAttestMiss   = metrics.GetOrCreateCounter("validator_attestation_miss")
	metricProposerHit  = metrics.GetOrCreateCounter("validator_propose_hit")
	metricProposerMiss = metrics.GetOrCreateCounter("validator_propose_miss")
)

type ValidatorMonitorImpl struct {
	fc               forkchoice.ForkChoiceStorageReader
	syncedData       *synced_data.SyncedDataManager
	ethClock         eth_clock.EthereumClock
	beaconCfg        *clparams.BeaconChainConfig
	vStatusMutex     sync.RWMutex
	vaidatorStatuses map[uint64]map[uint64]*validatorStatus // map validatorID -> epoch -> validatorStatus
	//proposerHitCache map[uint64]*proposerStatus             // map slot -> proposerStatus
}

func NewValidatorMonitor(
	enableMonitor bool,
	fc forkchoice.ForkChoiceStorageReader,
	ethClock eth_clock.EthereumClock,
	beaconConfig *clparams.BeaconChainConfig,
	syncedData *synced_data.SyncedDataManager,
) ValidatorMonitor {
	if !enableMonitor {
		return &dummyValdatorMonitor{}
	}

	m := &ValidatorMonitorImpl{
		fc:               fc,
		ethClock:         ethClock,
		beaconCfg:        beaconConfig,
		syncedData:       syncedData,
		vaidatorStatuses: make(map[uint64]map[uint64]*validatorStatus),
	}
	go m.runReportAttesterStatus()
	go m.runReportProposerStatus()
	return m
}

func (m *ValidatorMonitorImpl) AddValidator(vid uint64) {
	m.vStatusMutex.Lock()
	defer m.vStatusMutex.Unlock()
	if _, ok := m.vaidatorStatuses[vid]; !ok {
		m.vaidatorStatuses[vid] = make(map[uint64]*validatorStatus)
		log.Info("[monitor] add validator", "vid", vid)
	}
}

func (m *ValidatorMonitorImpl) RemoveValidator(vid uint64) {
	m.vStatusMutex.Lock()
	defer m.vStatusMutex.Unlock()
	delete(m.vaidatorStatuses, vid)
}

func (m *ValidatorMonitorImpl) OnNewBlock(block *cltypes.BeaconBlock) error {
	var (
		atts         = block.Body.Attestations
		blockEpoch   = m.ethClock.GetEpochAtSlot(block.Slot)
		currentEpoch = m.ethClock.GetCurrentEpoch()
	)
	if blockEpoch+2 < currentEpoch {
		// skip old blocks
		return nil
	}

	blockRoot, err := block.HashSSZ()
	if err != nil {
		log.Warn("failed to hash block", "err", err, "slot", block.Slot)
		return err
	}

	state, err := m.fc.GetStateAtBlockRoot(blockRoot, false)
	if err != nil {
		log.Warn("failed to get state at block root", "err", err, "slot", block.Slot, "blockRoot", blockRoot)
		return err
	} else if state == nil {
		log.Info("state is nil. syncing", "slot", block.Slot, "blockRoot", blockRoot)
		return nil
	}

	// todo: maybe launch a goroutine to update attester status
	m.vStatusMutex.Lock()
	defer m.vStatusMutex.Unlock()
	// update attester status
	atts.Range(func(i int, att *solid.Attestation, length int) bool {
		indicies, err := state.GetAttestingIndicies(att.AttestantionData(), att.AggregationBits(), true)
		if err != nil {
			log.Warn("failed to get attesting indicies", "err", err, "slot", block.Slot, "stateRoot", block.StateRoot)
			return false
		}
		slot := att.AttestantionData().Slot()
		attEpoch := m.ethClock.GetEpochAtSlot(slot)
		for _, vidx := range indicies {
			status := m.getValidatorStatus(vidx, attEpoch)
			if status == nil {
				continue
			}
			status.updateAttesterStatus(att)
		}
		return true
	})
	// update proposer status
	pIndex := block.ProposerIndex
	if _, ok := m.vaidatorStatuses[pIndex]; ok {
		status := m.getValidatorStatus(pIndex, blockEpoch)
		if status == nil {
			return nil
		}
		status.proposerHitSlot.Add(block.Slot)
	}

	return nil
}

func (m *ValidatorMonitorImpl) getValidatorStatus(vid uint64, epoch uint64) *validatorStatus {
	statusByEpoch, ok := m.vaidatorStatuses[vid]
	if !ok {
		return nil
	}
	if _, ok := statusByEpoch[epoch]; !ok {
		statusByEpoch[epoch] = &validatorStatus{
			epoch:              epoch,
			vindex:             vid,
			attestedBlockRoots: mapset.NewSet[common.Hash](),
			proposerHitSlot:    mapset.NewSet[uint64](),
		}
	}

	return statusByEpoch[epoch]
}

func (m *ValidatorMonitorImpl) runReportAttesterStatus() {
	// every epoch seconds
	epochDuration := time.Duration(m.beaconCfg.SlotsPerEpoch) * time.Duration(m.beaconCfg.SecondsPerSlot) * time.Second
	ticker := time.NewTicker(epochDuration)
	for range ticker.C {
		m.vStatusMutex.Lock()
		currentEpoch := m.ethClock.GetCurrentEpoch()
		// report attester status for current_epoch - 2
		epoch := currentEpoch - 2
		hitCount := 0
		missCount := 0
		for vindex, statuses := range m.vaidatorStatuses {
			if status, ok := statuses[epoch]; ok {
				successAtt := status.attestedBlockRoots.Cardinality()
				metricAttestHit.AddInt(successAtt)
				hitCount += successAtt
				delete(statuses, epoch)
				log.Debug("[monitor] report attester status hit", "epoch", epoch, "vindex", vindex, "countAttestedBlock", status.attestedBlockRoots.Cardinality())
			} else {
				metricAttestMiss.AddInt(1)
				missCount++
				log.Debug("[monitor] report attester status miss", "epoch", epoch, "vindex", vindex, "countAttestedBlock", 0)
			}
		}
		m.vStatusMutex.Unlock()
		log.Info("[monitor] report attester hit/miss", "epoch", epoch, "hitCount", hitCount, "missCount", missCount)
	}

}

type validatorStatus struct {
	epoch              uint64
	vindex             uint64
	attestedBlockRoots mapset.Set[common.Hash]
	proposerHitSlot    mapset.Set[uint64]
}

func (s *validatorStatus) updateAttesterStatus(att *solid.Attestation) {
	data := att.AttestantionData()
	s.attestedBlockRoots.Add(data.BeaconBlockRoot())
}

func (m *ValidatorMonitorImpl) runReportProposerStatus() {
	// check proposer in previous slot every slot duration
	ticker := time.NewTicker(time.Duration(m.beaconCfg.SecondsPerSlot) * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		m.vStatusMutex.Lock()
		headState := m.syncedData.HeadStateReader()
		if headState == nil {
			m.vStatusMutex.Unlock()
			continue
		}
		// check proposer in previous slot
		prevSlot := m.ethClock.GetCurrentSlot() - 1
		proposerIndex, err := headState.GetBeaconProposerIndexForSlot(prevSlot)
		if err != nil {
			log.Warn("failed to get proposer index", "slot", prevSlot, "err", err)
			continue
		}
		if status := m.getValidatorStatus(proposerIndex, prevSlot/m.beaconCfg.SlotsPerEpoch); status != nil {
			if status.proposerHitSlot.Contains(prevSlot) {
				metricProposerHit.AddInt(1)
				log.Info("[monitor] proposer hit", "slot", prevSlot, "proposerIndex", proposerIndex)
			} else {
				metricProposerMiss.AddInt(1)
				log.Info("[monitor] proposer miss", "slot", prevSlot, "proposerIndex", proposerIndex)
			}
		}
		m.vStatusMutex.Unlock()
	}
}
