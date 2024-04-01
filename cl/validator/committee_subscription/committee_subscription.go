package committee_subscription

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/aggregation"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
)

var (
	ErrIgnore                   = fmt.Errorf("ignore")
	ErrCommitteeIndexOutOfRange = fmt.Errorf("committee index out of range")
	ErrWrongSubnet              = fmt.Errorf("attestation is for the wrong subnet")
	ErrNotInPropagationRange    = fmt.Errorf("attestation is not in propagation range. %w", ErrIgnore)
	ErrEpochMismatch            = fmt.Errorf("epoch mismatch")
	ErrExactlyOneBitSet         = fmt.Errorf("exactly one aggregation bit should be set")
	ErrAggregationBitsMismatch  = fmt.Errorf("aggregation bits mismatch committee size")
)

type CommitteeSubscribeMgmt struct {
	indiciesDB    kv.RoDB
	genesisConfig *clparams.GenesisConfig
	beaconConfig  *clparams.BeaconChainConfig
	netConfig     *clparams.NetworkConfig
	sentinel      sentinel.SentinelClient
	state         *state.CachingBeaconState
	syncedData    *synced_data.SyncedDataManager
	// subscriptions
	aggregationPool    aggregation.AggregationPool
	validatorSubsMutex sync.RWMutex
	validatorSubs      map[uint64]map[uint64]*validatorSub // slot -> committeeIndex -> validatorSub
}

func NewCommitteeSubscribeManagement(
	ctx context.Context,
	indiciesDB kv.RoDB,
	beaconConfig *clparams.BeaconChainConfig,
	netConfig *clparams.NetworkConfig,
	genesisConfig *clparams.GenesisConfig,
	sentinel sentinel.SentinelClient,
	state *state.CachingBeaconState,
	aggregationPool aggregation.AggregationPool,
	syncedData *synced_data.SyncedDataManager,
) *CommitteeSubscribeMgmt {
	c := &CommitteeSubscribeMgmt{
		indiciesDB:      indiciesDB,
		beaconConfig:    beaconConfig,
		netConfig:       netConfig,
		genesisConfig:   genesisConfig,
		sentinel:        sentinel,
		state:           state,
		aggregationPool: aggregationPool,
		syncedData:      syncedData,
		validatorSubs:   make(map[uint64]map[uint64]*validatorSub),
	}
	c.sweepByStaleSlots(ctx)
	return c
}

type validatorSub struct {
	subnetId      uint64
	aggregate     bool
	validatorIdxs map[uint64]struct{}
}

func (c *CommitteeSubscribeMgmt) AddAttestationSubscription(ctx context.Context, p *cltypes.BeaconCommitteeSubscription) error {
	var (
		slot   = p.Slot
		cIndex = p.CommitteeIndex
		vIndex = p.ValidatorIndex
	)

	subnetId := c.computeSubnetId(slot, cIndex)
	// add validator to subscription
	c.validatorSubsMutex.Lock()
	if _, ok := c.validatorSubs[slot]; !ok {
		c.validatorSubs[slot] = make(map[uint64]*validatorSub)
	}
	if _, ok := c.validatorSubs[slot][cIndex]; !ok {
		c.validatorSubs[slot][cIndex] = &validatorSub{
			subnetId:  subnetId,
			aggregate: p.IsAggregator,
			validatorIdxs: map[uint64]struct{}{
				vIndex: {},
			},
		}
	} else {
		if p.IsAggregator {
			c.validatorSubs[slot][cIndex].aggregate = true
		}
		c.validatorSubs[slot][cIndex].validatorIdxs[vIndex] = struct{}{}
	}
	c.validatorSubsMutex.Unlock()

	// set sentinel gossip expiration by subnet id
	request := sentinel.RequestSubscribeExpiry{
		Topic:          gossip.TopicNameBeaconAttestation(subnetId),
		ExpiryUnixSecs: uint64(time.Now().Add(24 * time.Hour).Unix()), // temporarily set to 24 hours
	}
	if _, err := c.sentinel.SetSubscribeExpiry(ctx, &request); err != nil {
		return err
	}
	return nil
}

func (c *CommitteeSubscribeMgmt) checkAttestation(topic string, att *solid.Attestation) error {
	var (
		slot           = att.AttestantionData().Slot()
		committeeIndex = att.AttestantionData().CommitteeIndex()
		epoch          = att.AttestantionData().Target().Epoch()
		bits           = att.AggregationBits()
	)
	// [REJECT] The committee index is within the expected range
	committeeCount := c.computeCommitteePerSlot(slot)
	if committeeIndex >= committeeCount {
		return ErrCommitteeIndexOutOfRange
	}
	// [REJECT] The attestation is for the correct subnet -- i.e. compute_subnet_for_attestation(committees_per_slot, attestation.data.slot, index) == subnet_id
	subnetId := c.computeSubnetId(slot, committeeIndex)
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
	if len(bits)*8 < int(committeeCount) {
		return ErrAggregationBitsMismatch
	}
	// todo ...
	// [IGNORE] There has been no other valid attestation seen on an attestation subnet that has an identical attestation.data.target.epoch and participating validator index.
	// [REJECT] The signature of attestation is valid.
	// [IGNORE] The block being voted for (attestation.data.beacon_block_root) has been seen (via both gossip and non-gossip sources)
	// (a client MAY queue attestations for processing once block is retrieved).  // this case will be handled by the aggregation pool
	// [REJECT] The block being voted for (attestation.data.beacon_block_root) passes validation.
	// [REJECT] The attestation's target block is an ancestor of the block named in the LMD vote -- i.e.
	// get_checkpoint_block(store, attestation.data.beacon_block_root, attestation.data.target.epoch) == attestation.data.target.root
	// [IGNORE] The current finalized_checkpoint is an ancestor of the block defined by attestation.data.beacon_block_root --
	// i.e. get_checkpoint_block(store, attestation.data.beacon_block_root, store.finalized_checkpoint.epoch) == store.finalized_checkpoint.root
	return nil
}

func (c *CommitteeSubscribeMgmt) OnReceiveAttestation(topic string, att *solid.Attestation) error {
	if err := c.checkAttestation(topic, att); err != nil {
		return err
	}

	var (
		slot           = att.AttestantionData().Slot()
		committeeIndex = att.AttestantionData().CommitteeIndex()
	)
	c.validatorSubsMutex.RLock()
	defer c.validatorSubsMutex.RUnlock()
	if subs, ok := c.validatorSubs[slot]; ok {
		if sub, ok := subs[committeeIndex]; ok && sub.aggregate {
			// aggregate attestation
			if err := c.aggregationPool.AddAttestation(att); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CommitteeSubscribeMgmt) sweepByStaleSlots(ctx context.Context) {
	slotIsStale := func(curSlot, targetSlot uint64) bool {
		return curSlot-targetSlot > c.netConfig.AttestationPropagationSlotRange
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
			c.validatorSubsMutex.Lock()
			for slot := range c.validatorSubs {
				if slotIsStale(curSlot, slot) {
					delete(c.validatorSubs, slot)
				}
			}
			c.validatorSubsMutex.Unlock()
		}
	}
}

func (c *CommitteeSubscribeMgmt) computeSubnetId(slot uint64, committeeIndex uint64) uint64 {
	committeePerSlot := c.computeCommitteePerSlot(slot)
	// slots_since_epoch_start = uint64(slot % SLOTS_PER_EPOCH)
	// committees_since_epoch_start = committees_per_slot * slots_since_epoch_start
	// return SubnetID((committees_since_epoch_start + committee_index) % ATTESTATION_SUBNET_COUNT)
	slotsSinceEpochStart := slot % c.beaconConfig.SlotsPerEpoch
	committeesSinceEpochStart := committeePerSlot * slotsSinceEpochStart
	return (committeesSinceEpochStart + committeeIndex) % c.netConfig.AttestationSubnetCount
}

func (c *CommitteeSubscribeMgmt) computeCommitteePerSlot(slot uint64) uint64 {
	epoch := slot / c.beaconConfig.SlotsPerEpoch
	return c.syncedData.HeadState().CommitteeCount(epoch)
}
