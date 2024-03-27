package aggregation

import (
	"fmt"
	"sync"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

var (
	blsAggregate = bls.AggregateSignatures
)

type aggregationPoolImpl struct {
	aggregatesLock sync.RWMutex
	aggregates     map[[32]byte][]*attestation
}

func NewAggregationPool() AggregationPool {
	return &aggregationPoolImpl{
		aggregatesLock: sync.RWMutex{},
		aggregates:     make(map[[32]byte][]*attestation),
	}
}

type attestation struct {
	bitCount int
	att      *solid.Attestation
}

func (a *aggregationPoolImpl) AddAttestation(inAtt *solid.Attestation) error {
	key, err := inAtt.AttestantionData().HashSSZ()
	if err != nil {
		return err
	}

	a.aggregatesLock.Lock()
	defer a.aggregatesLock.Unlock()
	if _, ok := a.aggregates[key]; !ok {
		a.aggregates[key] = []*attestation{
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
	after := []*attestation{}
	for _, curAtt := range a.aggregates[key] {
		if overlap, err := checkOverlap(inAtt.AggregationBits(), curAtt.att.AggregationBits()); err != nil {
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
	if mergeCount == 0 {
		// no merge happened, just append the new attestation
		after = append(after, &attestation{
			bitCount: countBit(inAtt),
			att:      inAtt,
		})
	}
	a.aggregates[key] = after
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
	copy(mergedResult[:], mergedSig[:])
	merge := solid.NewAttestionFromParameters(
		newBits,
		a.AttestantionData(),
		mergedResult,
	)
	return merge, nil
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

func (a *aggregationPoolImpl) GetAggregatationByRoot(root [32]byte) *solid.Attestation {
	a.aggregatesLock.RLock()
	defer a.aggregatesLock.RUnlock()
	atts, ok := a.aggregates[root]
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
