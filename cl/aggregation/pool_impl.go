package aggregation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Giulio2002/bls"
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
	aggregates     map[[32]byte][]*attestation
}

type attestation struct {
	bitCount int
	att      *solid.Attestation
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
		aggregates:     make(map[[32]byte][]*attestation),
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
	if _, ok := p.aggregates[hashRoot]; !ok {
		p.aggregates[hashRoot] = []*attestation{
			{
				bitCount: countBit(inAtt),
				att:      inAtt,
			},
		}
		return nil
	}

	// NOTE: naive merge attestation for each existing attestation in the pool,
	// but it's not optimal. it's kind of a maximum coverage problem.
	mergeCount := 0
	alreadyContain := false
	after := []*attestation{}
	for _, curAtt := range p.aggregates[hashRoot] {
		if isSubset(curAtt.att.AggregationBits(), inAtt.AggregationBits()) {
			// in this case, the new attestation is already contained in the existing attestation, so do not need
			// to add it again no matter it's merged or not.
			alreadyContain = true
			after = append(after, curAtt)
			continue
		}

		if overlap, err := checkOverlap(curAtt.att.AggregationBits(), inAtt.AggregationBits()); err != nil {
			return err
		} else if overlap {
			// do nothing but just append the original attestation
			after = append(after, curAtt)
		} else {
			// merge attestation
			mergedAtt, err := mergeAttestationNoOverlap(inAtt, curAtt.att)
			if err != nil {
				return err
			}
			after = append(after, &attestation{
				bitCount: countBit(mergedAtt),
				att:      mergedAtt,
			})
			mergeCount++
		}
	}
	if mergeCount == 0 && !alreadyContain {
		// no merge and no contain, add new attestation
		after = append(after, &attestation{
			bitCount: countBit(inAtt),
			att:      inAtt,
		})
	}
	p.aggregates[hashRoot] = after
	return nil
}

func mergeAttestationNoOverlap(a, b *solid.Attestation) (*solid.Attestation, error) {
	// merge bit
	newBits := make([]byte, len(a.AggregationBits()))
	for i := range a.AggregationBits() {
		newBits[i] = a.AggregationBits()[i] | b.AggregationBits()[i]
	}
	// merge sig
	aSig := a.Signature()
	bSig := b.Signature()
	sig1 := make([]byte, len(aSig))
	sig2 := make([]byte, len(bSig))
	copy(sig1, aSig[:])
	copy(sig2, bSig[:])
	mergedSig, err := blsAggregate([][]byte{sig1, sig2})
	if err != nil {
		return nil, err
	}
	if len(mergedSig) > 96 {
		return nil, fmt.Errorf("merged signature is too long")
	}
	var mergedResult [96]byte
	copy(mergedResult[:], mergedSig)
	merge := solid.NewAttestionFromParameters(
		newBits,
		a.AttestantionData(),
		mergedResult,
	)
	return merge, nil
}

func isSubset(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	count := 0
	for i := range a {
		if a[i]&b[i] == b[i] {
			count++
		}
	}
	return count == len(b)
}

func checkOverlap(a, b []byte) (bool, error) {
	if len(a) != len(b) {
		return false, fmt.Errorf("different lengths")
	}
	for i := range a {
		if a[i]&b[i] != 0 {
			return true, nil
		}
	}
	return false, nil
}

func countBit(att *solid.Attestation) int {
	count := 0
	for _, b := range att.AggregationBits() {
		for i := 0; i < 8; i++ {
			count += int(b >> i & 1)
		}
	}
	return count
}

func (p *aggregationPoolImpl) GetAggregatationByRoot(root [32]byte) *solid.Attestation {
	p.aggregatesLock.RLock()
	defer p.aggregatesLock.RUnlock()
	atts, ok := p.aggregates[root]
	if !ok || atts == nil {
		return nil
	}

	// find the attestation with the most bits set
	maxBits := 0
	var maxAtt *solid.Attestation
	for _, att := range atts {
		if att.bitCount > maxBits {
			maxBits = att.bitCount
			maxAtt = att.att
		}
	}
	return maxAtt
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
				if len(p.aggregates[hashRoot]) == 0 {
					toRemoves = append(toRemoves, hashRoot)
					continue
				} else {
					slot := p.aggregates[hashRoot][0].att.AttestantionData().Slot()
					if p.slotIsStale(slot) {
						toRemoves = append(toRemoves, hashRoot)
					}
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
