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
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

var ErrIsSuperset = errors.New("attestation is superset of existing attestation")

// ErrPoolLimit reports an attestation dropped because the pool holds as many aggregates as it may.
var ErrPoolLimit = errors.New("aggregation pool limit reached")

// maxDataPerCommittee bounds the distinct AttestationData kept per slot and committee. Honest
// committees vote for one or two (during a fork); more come from attesters alternating votes.
const maxDataPerCommittee = 4

var blsAggregate = bls.AggregateSignatures

type signatureAggregator interface {
	AddValidated(sig []byte) error
	Compressed() [96]byte
}

var newSignatureAggregator = func() signatureAggregator { return &bls.SignatureAggregator{} }

// committeeAggregate keeps the running signature uncompressed, so adding a verified
// attestation does not decompress and group-check the stored aggregate again.
type committeeAggregate struct {
	attestation *solid.Attestation
	signature   signatureAggregator
}

type aggregationPoolImpl struct {
	// config
	beaconConfig   *clparams.BeaconChainConfig
	netConfig      *clparams.NetworkConfig
	ethClock       eth_clock.EthereumClock
	aggregatesLock sync.RWMutex
	aggregates     map[common.Hash]*solid.Attestation // don't need this anymore after electra upgrade
	// aggregatesInCommittee holds the per-committee aggregates used after the electra upgrade
	aggregatesInCommittee map[keyAggrInCommittee]*committeeAggregate
	dataPerCommittee      map[slotCommittee][]keyAggrInCommittee
	// refusedVotes counts the votes a full committee refused for each data root.
	refusedVotes map[slotCommittee]map[common.Hash]int
}

type slotCommittee struct {
	slot      uint64
	committee uint64
}

type keyAggrInCommittee struct {
	dataRoot       common.Hash
	committeeIndex uint64
}

func NewAggregationPool(
	ctx context.Context,
	beaconConfig *clparams.BeaconChainConfig,
	netConfig *clparams.NetworkConfig,
	ethClock eth_clock.EthereumClock,
) AggregationPool {
	p := &aggregationPoolImpl{
		ethClock:              ethClock,
		beaconConfig:          beaconConfig,
		netConfig:             netConfig,
		aggregatesLock:        sync.RWMutex{},
		aggregates:            make(map[common.Hash]*solid.Attestation),
		aggregatesInCommittee: make(map[keyAggrInCommittee]*committeeAggregate),
		dataPerCommittee:      make(map[slotCommittee][]keyAggrInCommittee),
		refusedVotes:          make(map[slotCommittee]map[common.Hash]int),
	}
	go p.sweepStaleAtt(ctx)
	return p
}

func (p *aggregationPoolImpl) AddAttestation(inAtt *solid.Attestation) error {
	// use hash of attestation data as key
	hashRoot, err := inAtt.Data.HashSSZ()
	if err != nil {
		return err
	}

	epoch := p.ethClock.GetEpochAtSlot(inAtt.Data.Slot)
	clversion := p.ethClock.StateVersionByEpoch(epoch)
	if clversion.BeforeOrEqual(clparams.DenebVersion) {
		p.aggregatesLock.Lock()
		defer p.aggregatesLock.Unlock()
		att, ok := p.aggregates[hashRoot]
		if !ok {
			storedAttestation := inAtt.Copy()
			storedAttestation.SetVersion(clversion)
			p.aggregates[hashRoot] = storedAttestation
			return nil
		}

		if utils.IsOverlappingSSZBitlist(att.AggregationBits.Bytes(), inAtt.AggregationBits.Bytes()) {
			// the on bit is already set, so ignore
			return ErrIsSuperset
		}
		// merge signature
		baseSig := att.Signature
		inSig := inAtt.Signature
		merged, err := blsAggregate([][]byte{baseSig[:], inSig[:]})
		if err != nil {
			return err
		}
		if len(merged) != 96 {
			return errors.New("merged signature is too long")
		}
		var mergedSig [96]byte
		copy(mergedSig[:], merged)

		// merge aggregation bits
		mergedBits, err := att.AggregationBits.Merge(inAtt.AggregationBits)
		if err != nil {
			return err
		}
		// update attestation
		mergedAttestation := &solid.Attestation{
			AggregationBits: mergedBits,
			Data:            att.Data,
			Signature:       mergedSig,
		}
		mergedAttestation.SetVersion(clversion)
		p.aggregates[hashRoot] = mergedAttestation
	} else {
		// Electra and after case, aggregate by committee
		return p.aggregateByCommittee(inAtt, clversion)
	}
	return nil
}

func (p *aggregationPoolImpl) aggregateByCommittee(inAtt *solid.Attestation, version clparams.StateVersion) error {
	indices := inAtt.CommitteeBits.GetOnIndices()
	if len(indices) != 1 {
		// it's composed of multiple committees, so ignore
		return nil
	}
	committeeIndex := uint64(indices[0])
	hashRoot, err := inAtt.Data.HashSSZ()
	if err != nil {
		return err
	}
	key := keyAggrInCommittee{
		dataRoot:       hashRoot,
		committeeIndex: committeeIndex,
	}
	p.aggregatesLock.Lock()
	defer p.aggregatesLock.Unlock()
	entry, exist := p.aggregatesInCommittee[key]
	if !exist {
		// Decode the signature before making room, so a rejected attestation evicts nothing.
		signature := newSignatureAggregator()
		if err := signature.AddValidated(inAtt.Signature[:]); err != nil {
			return err
		}
		perCommittee := slotCommittee{slot: inAtt.Data.Slot, committee: committeeIndex}
		if len(p.dataPerCommittee[perCommittee]) >= maxDataPerCommittee {
			votes := p.refusedVotes[perCommittee][hashRoot] + voteCount(inAtt)
			if !p.evictWeakerData(perCommittee, votes) {
				if p.refusedVotes[perCommittee] == nil {
					p.refusedVotes[perCommittee] = make(map[common.Hash]int)
				}
				p.refusedVotes[perCommittee][hashRoot] = votes
				return ErrPoolLimit
			}
		} else if len(p.aggregatesInCommittee) >= p.maxCommitteeAggregates() {
			p.pruneStaleLocked()
			if len(p.aggregatesInCommittee) >= p.maxCommitteeAggregates() {
				return ErrPoolLimit
			}
		}
		storedAttestation := inAtt.Copy()
		storedAttestation.SetVersion(version)
		p.aggregatesInCommittee[key] = &committeeAggregate{attestation: storedAttestation, signature: signature}
		p.dataPerCommittee[perCommittee] = append(p.dataPerCommittee[perCommittee], key)
		delete(p.refusedVotes[perCommittee], hashRoot)
		return nil
	}
	att := entry.attestation
	if utils.IsOverlappingSSZBitlist(att.AggregationBits.Bytes(), inAtt.AggregationBits.Bytes()) {
		// the on bit is already set, so ignore
		return ErrIsSuperset
	}

	// It's fine to directly merge aggregation bits here, because the attestation is from the same committee
	mergedAggrBits, err := att.AggregationBits.Merge(inAtt.AggregationBits)
	if err != nil {
		log.Debug("failed to merge aggregation bits", "err", err)
		return err
	}
	if err := entry.signature.AddValidated(inAtt.Signature[:]); err != nil {
		return err
	}
	mergedAttestation := &solid.Attestation{
		AggregationBits: mergedAggrBits,
		CommitteeBits:   att.CommitteeBits,
		Data:            att.Data,
		Signature:       entry.signature.Compressed(),
	}
	mergedAttestation.SetVersion(version)
	entry.attestation = mergedAttestation
	return nil
}

func (p *aggregationPoolImpl) GetAggregatationByRoot(root common.Hash) *solid.Attestation {
	p.aggregatesLock.RLock()
	defer p.aggregatesLock.RUnlock()
	return p.aggregates[root]
}

func (p *aggregationPoolImpl) GetAggregatationByRootAndCommittee(root common.Hash, committeeIndex uint64) *solid.Attestation {
	p.aggregatesLock.RLock()
	defer p.aggregatesLock.RUnlock()
	key := keyAggrInCommittee{
		dataRoot:       root,
		committeeIndex: committeeIndex,
	}
	entry, exist := p.aggregatesInCommittee[key]
	if !exist {
		return nil
	}
	return entry.attestation
}

func (p *aggregationPoolImpl) Aggregates() []*solid.Attestation {
	p.aggregatesLock.RLock()
	defer p.aggregatesLock.RUnlock()
	aggregates := make([]*solid.Attestation, 0, len(p.aggregatesInCommittee))
	for _, entry := range p.aggregatesInCommittee {
		aggregates = append(aggregates, entry.attestation)
	}
	return aggregates
}

func (p *aggregationPoolImpl) sweepStaleAtt(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.pruneStale()
		}
	}
}

func (p *aggregationPoolImpl) pruneStale() {
	p.aggregatesLock.Lock()
	defer p.aggregatesLock.Unlock()
	p.pruneStaleLocked()
}

func (p *aggregationPoolImpl) pruneStaleLocked() {
	for hashRoot, att := range p.aggregates {
		if p.slotIsStale(att.Data.Slot) {
			delete(p.aggregates, hashRoot)
		}
	}
	// Since Deneb an attestation stays includable until the end of the epoch after its own.
	currentEpoch := p.ethClock.GetEpochAtSlot(p.ethClock.GetCurrentSlot())
	for key, entry := range p.aggregatesInCommittee {
		if p.ethClock.GetEpochAtSlot(entry.attestation.Data.Slot)+1 < currentEpoch {
			p.removeCommitteeAggregate(key, entry)
		}
	}
	for perCommittee := range p.refusedVotes {
		if p.ethClock.GetEpochAtSlot(perCommittee.slot)+1 < currentEpoch {
			delete(p.refusedVotes, perCommittee)
		}
	}
}

// evictWeakerData removes the committee's data with the fewest votes if it has fewer than
// incomingVotes, and reports whether it made room.
func (p *aggregationPoolImpl) evictWeakerData(perCommittee slotCommittee, incomingVotes int) bool {
	var weakest keyAggrInCommittee
	weakestVotes := -1
	for _, key := range p.dataPerCommittee[perCommittee] {
		if votes := voteCount(p.aggregatesInCommittee[key].attestation); weakestVotes < 0 || votes < weakestVotes {
			weakest, weakestVotes = key, votes
		}
	}
	if weakestVotes < 0 || weakestVotes >= incomingVotes {
		return false
	}
	p.removeCommitteeAggregate(weakest, p.aggregatesInCommittee[weakest])
	return true
}

func (p *aggregationPoolImpl) removeCommitteeAggregate(key keyAggrInCommittee, entry *committeeAggregate) {
	delete(p.aggregatesInCommittee, key)
	perCommittee := slotCommittee{slot: entry.attestation.Data.Slot, committee: key.committeeIndex}
	keys := slices.DeleteFunc(p.dataPerCommittee[perCommittee], func(k keyAggrInCommittee) bool { return k == key })
	if len(keys) == 0 {
		delete(p.dataPerCommittee, perCommittee)
	} else {
		p.dataPerCommittee[perCommittee] = keys
	}
}

func voteCount(att *solid.Attestation) int {
	return utils.BitsOnCount(att.AggregationBits.Bytes()) - 1 // without the length bit
}

// maxCommitteeAggregates bounds the per-committee aggregates: the two epochs that stay includable,
// each committee with its allowance of distinct AttestationData.
func (p *aggregationPoolImpl) maxCommitteeAggregates() int {
	return 2 * int(p.beaconConfig.SlotsPerEpoch*p.beaconConfig.MaxCommitteesPerSlot) * maxDataPerCommittee
}

func (p *aggregationPoolImpl) slotIsStale(targetSlot uint64) bool {
	curSlot := p.ethClock.GetCurrentSlot()
	if curSlot <= targetSlot {
		// Avoid subtracting unsigned integers
		return false
	}
	return curSlot-targetSlot > p.netConfig.AttestationPropagationSlotRange
}
