// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package aggregation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Giulio2002/bls"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
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
	aggregates     map[common.Hash]*solid.Attestation
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
		p.aggregates[hashRoot] = inAtt.Copy()
		return nil
	}

	if utils.IsNonStrictSupersetBitlist(att.AggregationBits(), inAtt.AggregationBits()) {
		// the on bit is already set, so ignore
		return ErrIsSuperset
	}

	// merge signature
	baseSig := att.Signature()
	inSig := inAtt.Signature()
	merged, err := blsAggregate([][]byte{baseSig[:], inSig[:]})
	if err != nil {
		return err
	}
	if len(merged) != 96 {
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
	return att.Copy()
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
	curSlot := p.ethClock.GetCurrentSlot()
	return curSlot-targetSlot > p.netConfig.AttestationPropagationSlotRange
}
