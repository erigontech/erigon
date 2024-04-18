package attestation_producer

import (
	"errors"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	"github.com/ledgerwatch/erigon/cl/transition"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

var (
	ErrHeadStateNotAvailable = errors.New("head state not available")
)

const attestationsCacheSize = 21

type attestationProducer struct {
	beaconCfg *clparams.BeaconChainConfig

	attestationsCache *lru.Cache[uint64, solid.AttestationData] // Epoch => Base AttestationData
}

func New(beaconCfg *clparams.BeaconChainConfig) AttestationDataProducer {
	attestationsCache, err := lru.New[uint64, solid.AttestationData]("attestations", attestationsCacheSize)
	if err != nil {
		panic(err)
	}

	return &attestationProducer{
		beaconCfg:         beaconCfg,
		attestationsCache: attestationsCache,
	}
}

func (ap *attestationProducer) ProduceAndCacheAttestationData(baseState *state.CachingBeaconState, slot uint64, committeeIndex uint64) (solid.AttestationData, error) {
	epoch := slot / ap.beaconCfg.SlotsPerEpoch
	baseStateBlockRoot, err := baseState.BlockRoot()
	if err != nil {
		return solid.AttestationData{}, err
	}
	if baseAttestationData, ok := ap.attestationsCache.Get(epoch); ok {
		beaconBlockRoot := baseStateBlockRoot
		if baseState.Slot() > slot {
			beaconBlockRoot, err = baseState.GetBlockRootAtSlot(slot)
			if err != nil {
				return solid.AttestationData{}, err
			}
		}
		return solid.NewAttestionDataFromParameters(
			slot,
			committeeIndex,
			beaconBlockRoot,
			baseAttestationData.Source(),
			baseAttestationData.Target(),
		), nil
	}
	if baseState.Slot() > slot {
		return solid.AttestationData{}, errors.New("head state slot is bigger than requested slot, the attestation should have been cached, try again later.")
	}
	stateEpoch := state.Epoch(baseState)

	if stateEpoch < epoch {
		baseState, err = baseState.Copy()
		if err != nil {
			return solid.AttestationData{}, err
		}
		if err := transition.DefaultMachine.ProcessSlots(baseState, slot); err != nil {
			return solid.AttestationData{}, err
		}
	}

	targetEpoch := state.Epoch(baseState)
	epochStartTargetSlot := (targetEpoch * ap.beaconCfg.SlotsPerEpoch) - (ap.beaconCfg.SlotsPerEpoch - 1)
	var targetRoot libcommon.Hash
	if epochStartTargetSlot == baseState.Slot() {
		targetRoot = baseStateBlockRoot
	} else {
		targetRoot, err = baseState.GetBlockRootAtSlot(epochStartTargetSlot)
		if err != nil {
			return solid.AttestationData{}, err
		}
		if targetRoot == (libcommon.Hash{}) {
			targetRoot = baseStateBlockRoot
		}
	}

	baseAttestationData := solid.NewAttestionDataFromParameters(
		0,
		0,
		libcommon.Hash{},
		baseState.CurrentJustifiedCheckpoint(),
		solid.NewCheckpointFromParameters(
			targetRoot,
			targetEpoch,
		),
	)
	ap.attestationsCache.Add(epoch, baseAttestationData)
	return solid.NewAttestionDataFromParameters(
		slot,
		committeeIndex,
		baseStateBlockRoot,
		baseAttestationData.Source(),
		baseAttestationData.Target(),
	), nil
}
