package aggregation

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
)

var ErrIsSuperset = fmt.Errorf("attestation is superset of existing attestation")

var (
	blsAggregate = bls.AggregateSignatures
)

type aggregationPoolImpl struct {
	// config
	beaconConfig   *clparams.BeaconChainConfig
	netConfig      *clparams.NetworkConfig
	ethClock       eth_clock.EthereumClock
	aggregatesLock sync.RWMutex
	aggregates     map[common.Hash]*aggregate
}

type aggregate struct {
	attCandidates   []*solid.Attestation
	bestAggregation *solid.Attestation
}

func NewAggregationPool(
	ctx context.Context,
	beaconConfig *clparams.BeaconChainConfig,
	netConfig *clparams.NetworkConfig,
	ethClock eth_clock.EthereumClock,
) AggregationPool {
	p := &aggregationPoolImpl{
		ethClock:       ethClock,
		beaconConfig:   beaconConfig,
		netConfig:      netConfig,
		aggregatesLock: sync.RWMutex{},
		aggregates:     make(map[common.Hash]*aggregate),
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
	inAttCopy := inAtt.Copy()

	p.aggregatesLock.Lock()
	defer p.aggregatesLock.Unlock()
	aggr, ok := p.aggregates[hashRoot]
	if !ok {
		p.aggregates[hashRoot] = &aggregate{
			attCandidates:   []*solid.Attestation{inAttCopy},
			bestAggregation: inAttCopy,
		}
		return nil
	}
	// check if attestation with same bits already exists
	for _, att := range aggr.attCandidates {
		if bytes.Equal(att.AggregationBits(), inAtt.AggregationBits()) {
			return nil
		}
	}
	p.aggregates[hashRoot].attCandidates = append(p.aggregates[hashRoot].attCandidates, inAttCopy)
	p.aggregates[hashRoot].bestAggregation, err = findBestAggregation(p.aggregates[hashRoot].attCandidates)
	if err != nil {
		return err
	}
	return nil
}

func findBestAggregation(attestations []*solid.Attestation) (*solid.Attestation, error) {
	// find the largest set of attestations that are disjoint. It's essentially a max disjoint set coverage problem,
	// which is NP-hard. We use a greedy algorithm to approximate the solution.
	// The algorithm is as follows:
	// 1. Sort the attestations by the number of bits set in the aggregation bits.
	// 2. Iterate through the attestations in order, adding each attestation to the set if it is disjoint from the
	//    attestations already in the set.
	// 3. The set of attestations at the end of the iteration is the best aggregation.

	if len(attestations) == 0 {
		return nil, fmt.Errorf("no attestations to aggregate")
	}

	// sort attestations by number of bits set in aggregation bits (descending)
	sort.Slice(attestations, func(i, j int) bool {
		return utils.BitsOnCount(attestations[i].AggregationBits()) >
			utils.BitsOnCount(attestations[j].AggregationBits())
	})

	// naive greedy algorithm to find the best aggregation
	//bestBits := make([]byte, len(attestations[0].AggregationBits()))
	//bestSig := attestations[0].Signature()
	bestAggregation := attestations[0].Copy()
	for _, att := range attestations {
		if utils.IsDisjoint(att.AggregationBits(), bestAggregation.AggregationBits()) {
			// merge bits
			bits := make([]byte, len(bestAggregation.AggregationBits()))
			copy(bits, bestAggregation.AggregationBits())
			utils.MergeBitlists(bits, att.AggregationBits())

			// merge signatures
			sig := att.Signature()
			bestSig := bestAggregation.Signature()
			mergedSig, err := blsAggregate([][]byte{bestSig[:], sig[:]})
			if err != nil {
				return nil, err
			}
			if len(mergedSig) != 96 {
				return nil, fmt.Errorf("merged signature is too long")
			}
			copy(bestSig[:], mergedSig)
			bestAggregation.SetAggregationBits(bits)
			bestAggregation.SetSignature(bestSig)
		}
	}
	return bestAggregation, nil
}

func (p *aggregationPoolImpl) GetAggregatationByRoot(root common.Hash) *solid.Attestation {
	p.aggregatesLock.RLock()
	defer p.aggregatesLock.RUnlock()
	agg, ok := p.aggregates[root]
	if !ok {
		return nil
	}
	return agg.bestAggregation
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
				if p.slotIsStale(att.bestAggregation.AttestantionData().Slot()) {
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
	curSlot := p.ethClock.GetCurrentSlot()
	return curSlot-targetSlot > p.netConfig.AttestationPropagationSlotRange
}
