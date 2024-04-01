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

func (c *CommitteeSubscribeMgmt) CheckAggregateAttestation(att *solid.Attestation) error {
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
