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
	"bytes"
	"context"
	"errors"
	"math"
	"math/rand/v2"
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

// ErrVoterAlreadyCounted reports a vote of a committee member the pool already counts for other data.
var ErrVoterAlreadyCounted = errors.New("committee member already voted for other attestation data")

// maxDataPerCommittee bounds the distinct AttestationData kept per slot and committee, and also the
// refused data tracked for it. Honest votes split by head block and, from Gloas, by payload status
// (data.index), so a late block already gives up to three honest data; the fourth is a spare for a fork.
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
	// refused aggregates the votes a full committee refused, per data root, until that data has more
	// voters than the committee's weakest data. Only full committees have entries, so the pool
	// bound also bounds these, together with refusedLimit.
	refused map[slotCommittee]map[common.Hash]*committeeAggregate
	// aggregatorDuties holds the committees with a local aggregator. An aggregator may get its data
	// from another beacon node, so these committees keep the refused data of every voter.
	aggregatorDuties map[slotCommittee]struct{}
	// voters holds the members whose vote is counted, per slot and committee. A member votes once per
	// epoch, so one already counted for other data equivocates, and every data needs its own voter.
	voters map[slotCommittee]*solid.BitList
	// pinned holds the data roots the node served its validators, with their slot for pruning. Gossip
	// cannot displace them.
	pinned map[common.Hash]uint64
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
		refused:               make(map[slotCommittee]map[common.Hash]*committeeAggregate),
		aggregatorDuties:      make(map[slotCommittee]struct{}),
		voters:                make(map[slotCommittee]*solid.BitList),
		pinned:                make(map[common.Hash]uint64),
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
	perCommittee := slotCommittee{slot: inAtt.Data.Slot, committee: committeeIndex}
	p.aggregatesLock.Lock()
	defer p.aggregatesLock.Unlock()
	if p.votedForOtherData(perCommittee, key, inAtt) {
		return ErrVoterAlreadyCounted
	}
	if err := p.addCommitteeVote(perCommittee, key, inAtt, version); err != nil {
		return err
	}
	return p.countVoters(perCommittee, inAtt)
}

func (p *aggregationPoolImpl) addCommitteeVote(perCommittee slotCommittee, key keyAggrInCommittee, inAtt *solid.Attestation, version clparams.StateVersion) error {
	entry, exist := p.aggregatesInCommittee[key]
	if exist {
		return entry.add(inAtt, version)
	}
	_, pinned := p.pinned[key.dataRoot]
	full := len(p.dataPerCommittee[perCommittee]) >= maxDataPerCommittee
	if full && !(pinned && p.hasUnpinnedData(perCommittee)) {
		return p.addRefusedVote(perCommittee, key, inAtt, version)
	}
	// Decode the signature before making room, so a rejected attestation evicts nothing.
	entry, err := newCommitteeAggregate(inAtt, version)
	if err != nil {
		return err
	}
	if full {
		p.storePinnedAggregate(perCommittee, key, entry)
		return nil
	}
	if len(p.aggregatesInCommittee) >= p.maxCommitteeAggregates() {
		p.pruneStaleLocked()
		if len(p.aggregatesInCommittee) >= p.maxCommitteeAggregates() {
			return ErrPoolLimit
		}
	}
	p.storeCommitteeAggregate(perCommittee, key, entry)
	return nil
}

// votedForOtherData reports a vote whose members are already counted for other data of the committee.
// A member counted for this data is left to the aggregate, which refuses the overlap.
func (p *aggregationPoolImpl) votedForOtherData(perCommittee slotCommittee, key keyAggrInCommittee, inAtt *solid.Attestation) bool {
	counted, ok := p.voters[perCommittee]
	if !ok || !utils.IsOverlappingSSZBitlist(counted.Bytes(), inAtt.AggregationBits.Bytes()) {
		return false
	}
	own, ok := p.aggregatesInCommittee[key]
	if !ok {
		own, ok = p.refused[perCommittee][key.dataRoot]
	}
	return !ok || !utils.IsOverlappingSSZBitlist(own.attestation.AggregationBits.Bytes(), inAtt.AggregationBits.Bytes())
}

func (p *aggregationPoolImpl) countVoters(perCommittee slotCommittee, inAtt *solid.Attestation) error {
	counted, ok := p.voters[perCommittee]
	if !ok {
		p.voters[perCommittee] = inAtt.AggregationBits.Copy()
		return nil
	}
	merged, err := counted.Merge(inAtt.AggregationBits)
	if err != nil {
		return err
	}
	p.voters[perCommittee] = merged
	return nil
}

func newCommitteeAggregate(inAtt *solid.Attestation, version clparams.StateVersion) (*committeeAggregate, error) {
	signature := newSignatureAggregator()
	if err := signature.AddValidated(inAtt.Signature[:]); err != nil {
		return nil, err
	}
	attestation := inAtt.Copy()
	attestation.SetVersion(version)
	return &committeeAggregate{attestation: attestation, signature: signature}, nil
}

// add merges a vote of the same data and committee into the aggregate.
func (entry *committeeAggregate) add(inAtt *solid.Attestation, version clparams.StateVersion) error {
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

func (p *aggregationPoolImpl) storeCommitteeAggregate(perCommittee slotCommittee, key keyAggrInCommittee, entry *committeeAggregate) {
	p.aggregatesInCommittee[key] = entry
	p.dataPerCommittee[perCommittee] = append(p.dataPerCommittee[perCommittee], key)
}

// PinAttestationData marks data the node serves its validators, which our aggregators ask for. Once
// it is stored, gossip votes for other data cannot displace it. Votes refused before the pin move
// into the store if the committee stores unpinned data it can take the place of.
func (p *aggregationPoolImpl) PinAttestationData(data *solid.AttestationData) error {
	root, err := data.HashSSZ()
	if err != nil {
		return err
	}
	p.aggregatesLock.Lock()
	defer p.aggregatesLock.Unlock()
	if _, ok := p.pinned[root]; ok {
		return nil
	}
	p.pinned[root] = data.Slot
	for perCommittee, refused := range p.refused {
		entry, ok := refused[root]
		if !ok || perCommittee.slot != data.Slot || !p.hasUnpinnedData(perCommittee) {
			continue
		}
		delete(refused, root)
		p.storePinnedAggregate(perCommittee, keyAggrInCommittee{dataRoot: root, committeeIndex: perCommittee.committee}, entry)
	}
	return nil
}

// hasUnpinnedData reports whether the committee stores data that pinned data may take the place of.
// When it stores only pinned data, more pinned data waits with the refused data, so the committee
// bound holds.
func (p *aggregationPoolImpl) hasUnpinnedData(perCommittee slotCommittee) bool {
	return slices.ContainsFunc(p.dataPerCommittee[perCommittee], func(key keyAggrInCommittee) bool {
		_, pinned := p.pinned[key.dataRoot]
		return !pinned
	})
}

// storePinnedAggregate stores pinned data in a full committee in place of its weakest unpinned data,
// which is kept aside with its votes.
func (p *aggregationPoolImpl) storePinnedAggregate(perCommittee slotCommittee, key keyAggrInCommittee, entry *committeeAggregate) {
	if displacedKey, displaced := p.evictWeakerData(perCommittee, math.MaxInt); displaced != nil {
		refused := p.refused[perCommittee]
		if refused == nil {
			refused = make(map[common.Hash]*committeeAggregate)
			p.refused[perCommittee] = refused
		}
		p.evictWeakestRefused(refused, p.refusedLimit(perCommittee))
		refused[displacedKey.dataRoot] = displaced
	}
	p.storeCommitteeAggregate(perCommittee, key, entry)
}

// addRefusedVote adds a vote for data that a full committee does not store. The data replaces the
// committee's weakest data once it has more voters; until then its votes are kept aside.
func (p *aggregationPoolImpl) addRefusedVote(perCommittee slotCommittee, key keyAggrInCommittee, inAtt *solid.Attestation, version clparams.StateVersion) error {
	refused := p.refused[perCommittee]
	entry, exist := refused[key.dataRoot]
	if exist {
		if err := entry.add(inAtt, version); err != nil {
			return err
		}
	} else {
		var err error
		if entry, err = newCommitteeAggregate(inAtt, version); err != nil {
			return err
		}
		if !p.evictWeakestRefused(refused, p.refusedLimit(perCommittee)) {
			return ErrPoolLimit
		}
		if refused == nil {
			refused = make(map[common.Hash]*committeeAggregate)
			p.refused[perCommittee] = refused
		}
		refused[key.dataRoot] = entry
	}
	if displacedKey, displaced := p.evictWeakerData(perCommittee, voteCount(entry.attestation)); displaced != nil {
		delete(refused, key.dataRoot)
		refused[displacedKey.dataRoot] = displaced
		p.storeCommitteeAggregate(perCommittee, key, entry)
	}
	return nil
}

// refusedTieBreak picks which of n equally weak refused data gives way.
var refusedTieBreak = rand.IntN

// AddAggregatorDuty marks a committee with a local aggregator, from the previous epoch to the next.
func (p *aggregationPoolImpl) AddAggregatorDuty(slot, committeeIndex uint64) {
	currentEpoch := p.ethClock.GetEpochAtSlot(p.ethClock.GetCurrentSlot())
	epoch := p.ethClock.GetEpochAtSlot(slot)
	if committeeIndex >= p.beaconConfig.MaxCommitteesPerSlot || epoch+1 < currentEpoch || epoch > currentEpoch+1 {
		return
	}
	p.aggregatesLock.Lock()
	defer p.aggregatesLock.Unlock()
	p.aggregatorDuties[slotCommittee{slot: slot, committee: committeeIndex}] = struct{}{}
}

// refusedLimit bounds the refused data of a committee. With a local aggregator it is
// MAX_VALIDATORS_PER_COMMITTEE: every data needs a member not counted for other data, so the data of
// a committee never outnumber its members, and every data that got a vote keeps it.
func (p *aggregationPoolImpl) refusedLimit(perCommittee slotCommittee) int {
	if _, ok := p.aggregatorDuties[perCommittee]; ok {
		return int(p.beaconConfig.MaxValidatorsPerCommittee)
	}
	return maxDataPerCommittee
}

// evictWeakestRefused makes room for new refused data once the set holds limit data. The new data
// enters unless every refused data is pinned: every new data starts with one voter, so refusing it
// would lock any new data out of a full set. One of the unpinned refused data with the fewest voters
// gives way, picked at random: a predictable pick would let a stream of fresh one-vote data flush
// data that is gathering voters.
func (p *aggregationPoolImpl) evictWeakestRefused(refused map[common.Hash]*committeeAggregate, limit int) bool {
	if len(refused) < limit {
		return true
	}
	var weakest []common.Hash
	weakestVotes := -1
	for root, entry := range refused {
		if _, pinned := p.pinned[root]; pinned {
			continue
		}
		votes := voteCount(entry.attestation)
		if weakestVotes < 0 || votes < weakestVotes {
			weakest, weakestVotes = weakest[:0], votes
		}
		if votes == weakestVotes {
			weakest = append(weakest, root)
		}
	}
	if len(weakest) == 0 {
		return false
	}
	// Sorted, so the pick depends on the tie-break only, not on the map order.
	slices.SortFunc(weakest, func(a, b common.Hash) int { return bytes.Compare(a[:], b[:]) })
	delete(refused, weakest[refusedTieBreak(len(weakest))])
	return true
}

func (p *aggregationPoolImpl) GetAggregatationByRoot(root common.Hash) *solid.Attestation {
	p.aggregatesLock.RLock()
	defer p.aggregatesLock.RUnlock()
	return p.aggregates[root]
}

func (p *aggregationPoolImpl) GetAggregatationByRootAndCommittee(root common.Hash, slot, committeeIndex uint64) *solid.Attestation {
	p.aggregatesLock.RLock()
	defer p.aggregatesLock.RUnlock()
	key := keyAggrInCommittee{
		dataRoot:       root,
		committeeIndex: committeeIndex,
	}
	if entry, exist := p.aggregatesInCommittee[key]; exist {
		return entry.attestation
	}
	if entry, exist := p.refused[slotCommittee{slot: slot, committee: committeeIndex}][root]; exist {
		return entry.attestation
	}
	return nil
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
	for perCommittee := range p.refused {
		if p.ethClock.GetEpochAtSlot(perCommittee.slot)+1 < currentEpoch {
			delete(p.refused, perCommittee)
		}
	}
	for root, slot := range p.pinned {
		if p.ethClock.GetEpochAtSlot(slot)+1 < currentEpoch {
			delete(p.pinned, root)
		}
	}
	for perCommittee := range p.aggregatorDuties {
		if p.ethClock.GetEpochAtSlot(perCommittee.slot)+1 < currentEpoch {
			delete(p.aggregatorDuties, perCommittee)
		}
	}
	for perCommittee := range p.voters {
		if p.ethClock.GetEpochAtSlot(perCommittee.slot)+1 < currentEpoch {
			delete(p.voters, perCommittee)
		}
	}
}

// evictWeakerData removes and returns the committee's data with the fewest votes if it has fewer
// than incomingVotes.
func (p *aggregationPoolImpl) evictWeakerData(perCommittee slotCommittee, incomingVotes int) (keyAggrInCommittee, *committeeAggregate) {
	var weakest keyAggrInCommittee
	weakestVotes := -1
	for _, key := range p.dataPerCommittee[perCommittee] {
		if _, pinned := p.pinned[key.dataRoot]; pinned {
			continue
		}
		if votes := voteCount(p.aggregatesInCommittee[key].attestation); weakestVotes < 0 || votes < weakestVotes {
			weakest, weakestVotes = key, votes
		}
	}
	if weakestVotes < 0 || weakestVotes >= incomingVotes {
		return keyAggrInCommittee{}, nil
	}
	entry := p.aggregatesInCommittee[weakest]
	p.removeCommitteeAggregate(weakest, entry)
	return weakest, entry
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
