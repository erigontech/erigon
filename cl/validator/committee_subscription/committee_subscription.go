package committee_subscription

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/gossip"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
)

type CommitteeSubscribeMgmt struct {
	indiciesDB    kv.RoDB
	genesisConfig *clparams.GenesisConfig
	beaconConfig  *clparams.BeaconChainConfig
	netConfig     *clparams.NetworkConfig
	sentinel      sentinel.SentinelClient
	state         *state.CachingBeaconState
	// subscriptions
	aggregationMutex   sync.RWMutex
	aggregations       map[string]*aggregationData // map from slot:committeeIndex to aggregate data
	validatorSubsMutex sync.RWMutex
	validatorSubs      map[uint64][]*validatorSub // map from validator index to subscription details
}

func NewCommitteeSubscribeManagement(
	ctx context.Context,
	indiciesDB kv.RoDB,
	beaconConfig *clparams.BeaconChainConfig,
	netConfig *clparams.NetworkConfig,
	genesisConfig *clparams.GenesisConfig,
	sentinel sentinel.SentinelClient,
	state *state.CachingBeaconState,
) *CommitteeSubscribeMgmt {
	c := &CommitteeSubscribeMgmt{
		indiciesDB:    indiciesDB,
		beaconConfig:  beaconConfig,
		netConfig:     netConfig,
		genesisConfig: genesisConfig,
		sentinel:      sentinel,
		state:         state,
		//subnets:      make(map[uint64]*subnetSubscription),
		aggregations:  make(map[string]*aggregationData),
		validatorSubs: make(map[uint64][]*validatorSub),
	}
	c.sweepByStaleSlots(ctx)
	return c
}

type aggregationData struct {
	subnetId       uint64
	slot           uint64
	committeeIndex uint64
	signature      []byte
	bits           []byte
}

type validatorSub struct {
	subnetId       uint64
	slot           uint64
	committeeIndex uint64
}

func toAggregationId(slot, committeeIndex uint64) string {
	return fmt.Sprintf("%d:%d", slot, committeeIndex)
}

func (c *CommitteeSubscribeMgmt) AddAttestationSubscription(ctx context.Context, p *cltypes.BeaconCommitteeSubscription) error {
	subnetId, err := c.computeSubnetId(p.Slot, p.CommitteeIndex)
	if err != nil {
		return err
	}
	// 1. add validator to subscription
	c.validatorSubsMutex.Lock()
	if _, exist := c.validatorSubs[p.ValidatorIndex]; !exist {
		c.validatorSubs[p.ValidatorIndex] = make([]*validatorSub, 0)
	}
	c.validatorSubs[p.ValidatorIndex] = append(c.validatorSubs[p.ValidatorIndex],
		&validatorSub{
			subnetId:       subnetId,
			slot:           p.Slot,
			committeeIndex: p.CommitteeIndex,
		})
	c.validatorSubsMutex.Unlock()

	// 2. if aggregator, add to aggregation collection
	if p.IsAggregator {
		c.aggregationMutex.Lock()
		aggrId := toAggregationId(p.Slot, p.CommitteeIndex)
		if _, exist := c.aggregations[aggrId]; !exist {
			c.aggregations[aggrId] = &aggregationData{
				subnetId:       subnetId,
				slot:           p.Slot,
				committeeIndex: p.CommitteeIndex,
				signature:      nil,
				bits:           make([]byte, c.beaconConfig.MaxValidatorsPerCommittee/8),
			}
		}
		c.aggregationMutex.Unlock()
	}

	// 3. set sentinel gossip expiration by subnet id
	request := sentinel.RequestSubscribeExpiry{
		Topic:          gossip.TopicNameBeaconAttestation(subnetId),
		ExpiryUnixSecs: uint64(time.Now().Add(24 * time.Hour).Unix()), // temporarily set to 24 hours
	}
	if _, err := c.sentinel.SetSubscribeExpiry(ctx, &request); err != nil {
		return err
	}
	return nil
}

var (
	ErrIgnore                   = fmt.Errorf("ignore")
	ErrCommitteeIndexOutOfRange = fmt.Errorf("committee index out of range")
	ErrWrongSubnet              = fmt.Errorf("attestation is for the wrong subnet")
	ErrNotInPropagationRange    = fmt.Errorf("attestation is not in propagation range. %w", ErrIgnore)
	ErrEpochMismatch            = fmt.Errorf("epoch mismatch")
	ErrExactlyOneBitSet         = fmt.Errorf("exactly one aggregation bit should be set")
	ErrAggregationBitsMismatch  = fmt.Errorf("aggregation bits mismatch committee size")
)

func (c *CommitteeSubscribeMgmt) checkAttestationData(topic string, att *solid.Attestation) error {
	var (
		slot           = att.AttestantionData().Slot()
		committeeIndex = att.AttestantionData().CommitteeIndex()
		epoch          = att.AttestantionData().Target().Epoch()
		bits           = att.AggregationBits()
	)
	// [REJECT] The committee index is within the expected range
	committeeCount, err := c.computeCommitteePerSlot(slot)
	if err != nil {
		return err
	}
	if committeeIndex >= committeeCount {
		return ErrCommitteeIndexOutOfRange
	}
	// [REJECT] The attestation is for the correct subnet -- i.e. compute_subnet_for_attestation(committees_per_slot, attestation.data.slot, index) == subnet_id
	subnetId, err := c.computeSubnetId(slot, committeeIndex)
	if err != nil {
		return err
	}
	topicSubnetId, err := gossip.SubnetIdFromTopicBeaconAttestation(topic)
	if err != nil {
		return err
	}
	if subnetId != topicSubnetId {
		return ErrWrongSubnet
	}
	// [IGNORE] attestation.data.slot is within the last ATTESTATION_PROPAGATION_SLOT_RANGE slots (within a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) --
	// i.e. attestation.data.slot + ATTESTATION_PROPAGATION_SLOT_RANGE >= current_slot >= attestation.data.slot (a client MAY queue future attestations for processing at the appropriate slot).
	currentSlot := utils.GetCurrentSlot(c.genesisConfig.GenesisTime, c.beaconConfig.SecondsPerSlot)
	if currentSlot < slot || currentSlot > slot+c.netConfig.AttestationPropagationSlotRange {
		return ErrNotInPropagationRange
	}
	// [REJECT] The attestation's epoch matches its target -- i.e. attestation.data.target.epoch == compute_epoch_at_slot(attestation.data.slot)
	if epoch != slot/c.beaconConfig.SlotsPerEpoch {
		return ErrEpochMismatch
	}
	//[REJECT] The attestation is unaggregated -- that is, it has exactly one participating validator (len([bit for bit in aggregation_bits if bit]) == 1, i.e. exactly 1 bit is set).
	emptyCount := 0
	for i := 0; i < len(bits); i++ {
		if bits[i] == 0 {
			emptyCount++
		} else if bits[i]&(bits[i]-1) != 0 {
			return ErrExactlyOneBitSet
		}
	}
	if emptyCount != len(bits)-1 {
		return ErrExactlyOneBitSet
	}
	// [REJECT] The number of aggregation bits matches the committee size -- i.e. len(aggregation_bits) == len(get_beacon_committee(state, attestation.data.slot, index)).
	if len(bits)*8 != int(committeeCount) {
		return ErrAggregationBitsMismatch
	}
	// todo ...
	// [IGNORE] There has been no other valid attestation seen on an attestation subnet that has an identical attestation.data.target.epoch and participating validator index.
	// [REJECT] The signature of attestation is valid.
	// [IGNORE] The block being voted for (attestation.data.beacon_block_root) has been seen (via both gossip and non-gossip sources)
	// (a client MAY queue attestations for processing once block is retrieved).
	// [REJECT] The block being voted for (attestation.data.beacon_block_root) passes validation.
	// [REJECT] The attestation's target block is an ancestor of the block named in the LMD vote -- i.e.
	// get_checkpoint_block(store, attestation.data.beacon_block_root, attestation.data.target.epoch) == attestation.data.target.root
	// [IGNORE] The current finalized_checkpoint is an ancestor of the block defined by attestation.data.beacon_block_root --
	// i.e. get_checkpoint_block(store, attestation.data.beacon_block_root, store.finalized_checkpoint.epoch) == store.finalized_checkpoint.root
	return nil
}

func (c *CommitteeSubscribeMgmt) OnReceiveAttestation(topic string, att *solid.Attestation) error {
	if err := c.checkAttestationData(topic, att); err != nil {
		return err
	}

	var (
		slot           = att.AttestantionData().Slot()
		committeeIndex = att.AttestantionData().CommitteeIndex()
		sig            = att.Signature()
		bits           = att.AggregationBits()
		aggrId         = toAggregationId(slot, committeeIndex)
	)
	c.aggregationMutex.Lock()
	defer c.aggregationMutex.Unlock()
	aggrData, exist := c.aggregations[aggrId]
	if !exist {
		// no one is interested in this aggregation
		return nil
	}
	bitGroupIdx := -1
	// check if already have aggregation signature associated with the bit. if not, add it
	for i := 0; i < len(bits); i++ {
		if bits[i] == 0 {
			continue
		} else if bits[i]|aggrData.bits[i] == aggrData.bits[i] {
			// already have this bit, skip current attestation
			return nil
		} else {
			// get a new bit
			bitGroupIdx = i
			break
		}
	}
	if bitGroupIdx == -1 {
		// weird case. all bits are 0
		log.Warn("all bits are 0")
		return nil
	}
	// aggregate
	sigBytes := make([]byte, 96)
	copy(sigBytes, sig[:])
	if aggrData != nil {
		aggrSig, err := bls.AggregateSignatures([][]byte{
			aggrData.signature,
			sigBytes,
		})
		if err != nil {
			log.Error("aggregate signature failed", "err", err)
			return err
		}
		aggrData.signature = aggrSig
	} else {
		aggrData.signature = sigBytes
	}
	// update aggregation bits
	aggrData.bits[bitGroupIdx] |= bits[bitGroupIdx]
	return nil
}

func (c *CommitteeSubscribeMgmt) sweepByStaleSlots(ctx context.Context) {
	// sweep subscriptions if slot is older than current slot
	sweepValidatorSubscriptions := func(curSlot uint64) {
		c.validatorSubsMutex.Lock()
		defer c.validatorSubsMutex.Unlock()
		for idx, subs := range c.validatorSubs {
			liveSubs := make([]*validatorSub, 0)
			for i := 0; i < len(subs); i++ {
				if curSlot <= subs[i].slot {
					// keep this subscription
					liveSubs = append(liveSubs, subs[i])
				}
			}
			if len(liveSubs) == 0 {
				delete(c.validatorSubs, idx)
			} else {
				c.validatorSubs[idx] = liveSubs
			}
		}
	}
	// sweep aggregations if slot is older than current slot
	sweepAggregations := func(curSlot uint64) {
		c.aggregationMutex.Lock()
		defer c.aggregationMutex.Unlock()
		for id, aggrData := range c.aggregations {
			if curSlot > aggrData.slot {
				delete(c.aggregations, id)
			}
		}
	}
	// sweep every minute
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			curSlot := utils.GetCurrentSlot(c.genesisConfig.GenesisTime, c.beaconConfig.SecondsPerSlot)
			sweepValidatorSubscriptions(curSlot)
			sweepAggregations(curSlot)
		}
	}
}

func (c *CommitteeSubscribeMgmt) computeSubnetId(slot uint64, committeeIndex uint64) (uint64, error) {
	committeePerSlot, err := c.computeCommitteePerSlot(slot)
	if err != nil {
		return 0, err
	}
	// slots_since_epoch_start = uint64(slot % SLOTS_PER_EPOCH)
	// committees_since_epoch_start = committees_per_slot * slots_since_epoch_start
	// return SubnetID((committees_since_epoch_start + committee_index) % ATTESTATION_SUBNET_COUNT)
	slotsSinceEpochStart := slot % c.beaconConfig.SlotsPerEpoch
	committeesSinceEpochStart := committeePerSlot * slotsSinceEpochStart
	return (committeesSinceEpochStart + committeeIndex) % c.netConfig.AttestationSubnetCount, nil
}

func (c *CommitteeSubscribeMgmt) computeCommitteePerSlot(slot uint64) (uint64, error) {
	tx, err := c.indiciesDB.BeginRo(context.Background())
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	activeIndicies, err := state_accessors.ReadActiveIndicies(tx, slot)
	if err != nil {
		return 0, err
	}
	cfg := c.beaconConfig
	committeePerSlot := uint64(len(activeIndicies)) / cfg.SlotsPerEpoch / cfg.TargetCommitteeSize
	if cfg.MaxCommitteesPerSlot < committeePerSlot {
		committeePerSlot = cfg.MaxCommitteesPerSlot
	}
	if committeePerSlot < 1 {
		committeePerSlot = 1
	}
	return committeePerSlot, nil
}
