package aggregation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/utils"
)

var (
	blsAggregate = bls.AggregateSignatures
)

type aggregationPoolImpl struct {
	// config
	genesisConfig  *clparams.GenesisConfig
	beaconConfig   *clparams.BeaconChainConfig
	netConfig      *clparams.NetworkConfig
	aggregatesLock sync.RWMutex
	aggregates     map[common.Hash]*solid.Attestation
}

func NewAggregationPool(
	ctx context.Context,
	genesisConfig *clparams.GenesisConfig,
	beaconConfig *clparams.BeaconChainConfig,
	netConfig *clparams.NetworkConfig,
) AggregationPool {
	p := &aggregationPoolImpl{
		genesisConfig:  genesisConfig,
		beaconConfig:   beaconConfig,
		netConfig:      netConfig,
		aggregatesLock: sync.RWMutex{},
		aggregates:     make(map[common.Hash]*solid.Attestation),
	}
	go p.sweepStaleAtt(ctx)
	return p
}

func (p *aggregationPoolImpl) AddAttestation(inAtt *solid.Attestation) error {
	// use hash of attestation data as key
	hashRoot, err := inAtt.AttestantionData().HashSSZ()
	if err != nil {
		return err
	}

	p.aggregatesLock.Lock()
	defer p.aggregatesLock.Unlock()
	att, ok := p.aggregates[hashRoot]
	if !ok {
		p.aggregates[hashRoot] = inAtt.Clone().(*solid.Attestation)
		return nil
	}

	if utils.IsSupersetBitlist(att.AggregationBits(), inAtt.AggregationBits()) {
		// no need to merge existing signatures
		return nil
	}

	// merge signature
	baseSig := att.Signature()
	inSig := inAtt.Signature()
	merged, err := blsAggregate([][]byte{baseSig[:], inSig[:]})
	if err != nil {
		return err
	}
	if len(merged) > 96 {
		return fmt.Errorf("merged signature is too long")
	}
	var mergedSig [96]byte
	copy(mergedSig[:], merged)

	// merge aggregation bits
	mergedBits := make([]byte, len(att.AggregationBits()))
	for i := range att.AggregationBits() {
		mergedBits[i] = att.AggregationBits()[i] | inAtt.AggregationBits()[i]
	}

	// update attestation
	p.aggregates[hashRoot] = solid.NewAttestionFromParameters(
		mergedBits,
		inAtt.AttestantionData(),
		mergedSig,
	)
	return nil
}

func (p *aggregationPoolImpl) GetAggregatationByRoot(root common.Hash) *solid.Attestation {
	p.aggregatesLock.RLock()
	defer p.aggregatesLock.RUnlock()
	att := p.aggregates[root]
	if att == nil {
		return nil
	}
	return att.Clone().(*solid.Attestation)
}

func (p *aggregationPoolImpl) sweepStaleAtt(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.aggregatesLock.Lock()
			toRemoves := make([][32]byte, 0)
			for hashRoot := range p.aggregates {
				att := p.aggregates[hashRoot]
				if p.slotIsStale(att.AttestantionData().Slot()) {
					toRemoves = append(toRemoves, hashRoot)
				}
			}
			// remove stale attestation
			for _, hashRoot := range toRemoves {
				delete(p.aggregates, hashRoot)
			}
			p.aggregatesLock.Unlock()
		}
	}
}

func (p *aggregationPoolImpl) slotIsStale(targetSlot uint64) bool {
	curSlot := utils.GetCurrentSlot(p.genesisConfig.GenesisTime, p.beaconConfig.SecondsPerSlot)
	return curSlot-targetSlot > p.netConfig.AttestationPropagationSlotRange
}
