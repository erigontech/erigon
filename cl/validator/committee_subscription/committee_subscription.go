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
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
)

type CommitteeSubscribeMgmt struct {
	indiciesDB    kv.RoDB
	genesisConfig *clparams.GenesisConfig
	beaconConfig  *clparams.BeaconChainConfig
	netConfig     *clparams.NetworkConfig
	sentinel      sentinel.SentinelClient
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
) *CommitteeSubscribeMgmt {
	a := &CommitteeSubscribeMgmt{
		indiciesDB:    indiciesDB,
		beaconConfig:  beaconConfig,
		netConfig:     netConfig,
		genesisConfig: genesisConfig,
		sentinel:      sentinel,
		//subnets:      make(map[uint64]*subnetSubscription),
		aggregations:  make(map[string]*aggregationData),
		validatorSubs: make(map[uint64][]*validatorSub),
	}
	a.sweepByStaleSlots(ctx)
	return a
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

func (a *CommitteeSubscribeMgmt) AddAttestationSubscription(ctx context.Context, p *cltypes.BeaconCommitteeSubscription) error {
	subnetId, err := a.computeSubnetId(p.Slot, p.CommitteeIndex)
	if err != nil {
		return err
	}
	// 1. add validator to subscription
	a.validatorSubsMutex.Lock()
	if _, exist := a.validatorSubs[p.ValidatorIndex]; !exist {
		a.validatorSubs[p.ValidatorIndex] = make([]*validatorSub, 0)
	}
	a.validatorSubs[p.ValidatorIndex] = append(a.validatorSubs[p.ValidatorIndex],
		&validatorSub{
			subnetId:       subnetId,
			slot:           p.Slot,
			committeeIndex: p.CommitteeIndex,
		})
	a.validatorSubsMutex.Unlock()

	// 2. if aggregator, add to aggregation collection
	if p.IsAggregator {
		a.aggregationMutex.Lock()
		aggrId := toAggregationId(p.Slot, p.CommitteeIndex)
		if _, exist := a.aggregations[aggrId]; !exist {
			a.aggregations[aggrId] = &aggregationData{
				subnetId:       subnetId,
				slot:           p.Slot,
				committeeIndex: p.CommitteeIndex,
				signature:      nil,
				bits:           make([]byte, a.beaconConfig.MaxValidatorsPerCommittee/8),
			}
		}
		a.aggregationMutex.Unlock()
	}

	// 3. set sentinel gossip expiration by subnet id
	request := sentinel.RequestSubscribeExpiry{
		Topic:          gossip.TopicNameBeaconAttestation(subnetId),
		ExpiryUnixSecs: uint64(time.Now().Add(24 * time.Hour).Unix()), // temporarily set to 24 hours
	}
	if _, err := a.sentinel.SetSubscribeExpiry(ctx, &request); err != nil {
		return err
	}
	return nil
}

func (a *CommitteeSubscribeMgmt) OnReceiveAttestation(att *solid.Attestation) error {
	var (
		slot           = att.AttestantionData().Slot()
		committeeIndex = att.AttestantionData().CommitteeIndex()
		sig            = att.Signature()
		bits           = att.AggregationBits()
	)
	aggrId := toAggregationId(slot, committeeIndex)
	a.aggregationMutex.Lock()
	defer a.aggregationMutex.Unlock()
	aggrData, exist := a.aggregations[aggrId]
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

func (a *CommitteeSubscribeMgmt) sweepByStaleSlots(ctx context.Context) {
	// sweep subscriptions if slot is older than current slot
	sweepValidatorSubscriptions := func(curSlot uint64) {
		a.validatorSubsMutex.Lock()
		defer a.validatorSubsMutex.Unlock()
		for idx, subs := range a.validatorSubs {
			liveSubs := make([]*validatorSub, 0)
			for i := 0; i < len(subs); i++ {
				if curSlot <= subs[i].slot {
					// keep this subscription
					liveSubs = append(liveSubs, subs[i])
				}
			}
			if len(liveSubs) == 0 {
				delete(a.validatorSubs, idx)
			} else {
				a.validatorSubs[idx] = liveSubs
			}
		}
	}
	// sweep aggregations if slot is older than current slot
	sweepAggregations := func(curSlot uint64) {
		a.aggregationMutex.Lock()
		defer a.aggregationMutex.Unlock()
		for id, aggrData := range a.aggregations {
			if curSlot > aggrData.slot {
				delete(a.aggregations, id)
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
			curSlot := utils.GetCurrentSlot(a.genesisConfig.GenesisTime, a.beaconConfig.SecondsPerSlot)
			sweepValidatorSubscriptions(curSlot)
			sweepAggregations(curSlot)
		}
	}
}

func (a *CommitteeSubscribeMgmt) computeSubnetId(slot uint64, committeeIndex uint64) (uint64, error) {
	tx, err := a.indiciesDB.BeginRo(context.Background())
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	activeIndicies, err := state_accessors.ReadActiveIndicies(tx, slot)
	if err != nil {
		return 0, err
	}
	committeePerSlot := a.computeCommitteePerSlot(uint64(len(activeIndicies)))

	// slots_since_epoch_start = uint64(slot % SLOTS_PER_EPOCH)
	// committees_since_epoch_start = committees_per_slot * slots_since_epoch_start
	// return SubnetID((committees_since_epoch_start + committee_index) % ATTESTATION_SUBNET_COUNT)
	slotsSinceEpochStart := slot % a.beaconConfig.SlotsPerEpoch
	committeesSinceEpochStart := committeePerSlot * slotsSinceEpochStart
	return (committeesSinceEpochStart + committeeIndex) % a.netConfig.AttestationSubnetCount, nil
}

func (a *CommitteeSubscribeMgmt) computeCommitteePerSlot(activeIndiciesLength uint64) uint64 {
	cfg := a.beaconConfig
	committeePerSlot := activeIndiciesLength / cfg.SlotsPerEpoch / cfg.TargetCommitteeSize
	if cfg.MaxCommitteesPerSlot < committeePerSlot {
		committeePerSlot = cfg.MaxCommitteesPerSlot
	}
	if committeePerSlot < 1 {
		committeePerSlot = 1
	}
	return committeePerSlot
}
