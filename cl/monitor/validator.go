package monitor

import (
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
)

var (
	// metrics
	metricValidatorAttHit  = metrics.GetOrCreateCounter("validator_attestation_hit")
	metricValidatorAttMiss = metrics.GetOrCreateCounter("validator_attestation_miss")
)

type ValidatorMonitorImpl struct {
	fc          forkchoice.ForkChoiceStorageReader
	vaidatorIdx mapset.Set[uint64] // validator index set
}

func NewValidatorMonitor(fc forkchoice.ForkChoiceStorageReader) ValidatorMonitor {
	return &ValidatorMonitorImpl{
		fc:          fc,
		vaidatorIdx: mapset.NewSet[uint64](),
	}
}

func (m *ValidatorMonitorImpl) AddValidator(vid uint64) {
	m.vaidatorIdx.Add(vid)
}

func (m *ValidatorMonitorImpl) RemoveValidator(vid uint64) {
	m.vaidatorIdx.Remove(vid)
}

func (m *ValidatorMonitorImpl) OnNewBlock(block *cltypes.BeaconBlock) error {
	var (
		atts    = block.Body.Attestations
		hitSet  = mapset.NewSet[uint64]()
		missSet mapset.Set[uint64]
	)

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

	atts.Range(func(i int, att *solid.Attestation, length int) bool {
		indicies, err := state.GetAttestingIndicies(att.AttestantionData(), att.AggregationBits(), true)
		if err != nil {
			log.Warn("failed to get attesting indicies", "err", err, "slot", block.Slot, "stateRoot", block.StateRoot)
			return false
		}
		for _, idx := range indicies {
			if m.vaidatorIdx.Contains(idx) {
				hitSet.Add(idx)
			}
		}
		return true
	})
	missSet = m.vaidatorIdx.Difference(hitSet)

	// bump metrics
	metricValidatorAttHit.AddInt(hitSet.Cardinality())
	metricValidatorAttMiss.AddInt(missSet.Cardinality())
	return nil
}
