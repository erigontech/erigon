package monitor

import (
	"fmt"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

var (
	// metrics
	metricAttestHit = "validator_attestation_hit{vid=\"%d\"}"
)

type ValidatorMonitorImpl struct {
	fc               forkchoice.ForkChoiceStorageReader
	ethClock         eth_clock.EthereumClock
	beaconCfg        *clparams.BeaconChainConfig
	vStatusMutex     sync.RWMutex
	vaidatorStatuses map[uint64]map[uint64]*validatorStatus // map validatorID -> epoch -> validatorStatus
}

func NewValidatorMonitor(fc forkchoice.ForkChoiceStorageReader, ethClock eth_clock.EthereumClock, beaconConfig *clparams.BeaconChainConfig) ValidatorMonitor {
	m := &ValidatorMonitorImpl{
		fc:               fc,
		ethClock:         ethClock,
		beaconCfg:        beaconConfig,
		vaidatorStatuses: make(map[uint64]map[uint64]*validatorStatus),
	}
	go m.runReportAttesterStatus()
	return m
}

func (m *ValidatorMonitorImpl) AddValidator(vid uint64) {
	m.vStatusMutex.Lock()
	defer m.vStatusMutex.Unlock()
	if _, ok := m.vaidatorStatuses[vid]; !ok {
		m.vaidatorStatuses[vid] = make(map[uint64]*validatorStatus)
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
	atts.Range(func(i int, att *solid.Attestation, length int) bool {
		indicies, err := state.GetAttestingIndicies(att.AttestantionData(), att.AggregationBits(), true)
		if err != nil {
			log.Warn("failed to get attesting indicies", "err", err, "slot", block.Slot, "stateRoot", block.StateRoot)
			return false
		}
		slot := att.AttestantionData().Slot()
		attEpoch := m.ethClock.GetEpochAtSlot(slot)
		for _, vidx := range indicies {
			if _, ok := m.vaidatorStatuses[vidx]; !ok {
				// skip unknown validators
				continue
			}
			status, ok := m.vaidatorStatuses[vidx][attEpoch]
			if !ok {
				status = &validatorStatus{
					epoch:              attEpoch,
					attestedBlockRoots: mapset.NewSet[common.Hash](),
					attesterMetric:     metrics.GetOrCreateCounter(fmt.Sprintf(metricAttestHit, vidx)),
				}
				m.vaidatorStatuses[vidx][attEpoch] = status
			}
			status.updateAttesterStatus(att)
		}
		return true
	})

	return nil
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
		for _, statuses := range m.vaidatorStatuses {
			if status, ok := statuses[epoch]; ok {
				status.reportAttester()
				delete(statuses, epoch)
			}
		}
		m.vStatusMutex.Unlock()
	}

}

type validatorStatus struct {
	epoch              uint64
	attestedBlockRoots mapset.Set[common.Hash]
	attesterMetric     metrics.Counter
}

func (s *validatorStatus) updateAttesterStatus(att *solid.Attestation) {
	data := att.AttestantionData()
	s.attestedBlockRoots.Add(data.BeaconBlockRoot())
}

func (s *validatorStatus) reportAttester() {
	countAttestedBlock := s.attestedBlockRoots.Cardinality()
	s.attesterMetric.AddInt(countAttestedBlock)
}
