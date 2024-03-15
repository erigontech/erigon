package attestation

import (
	"context"
	"sync"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/sentinel"
)

type Attestation struct {
	indiciesDB   kv.RoDB
	beaconConfig *clparams.BeaconChainConfig
	netConfig    *clparams.NetworkConfig
	sentinel     *sentinel.Sentinel
	// subscriptions
	subnetAttMutex sync.Mutex
	subnets        map[uint64]*subnetSubscription // map from subnet id to subscription list
}

func NewAttestation(
	ctx context.Context,
	indiciesDB kv.RoDB,
	beaconConfig *clparams.BeaconChainConfig,
	netConfig *clparams.NetworkConfig,
	sentinel *sentinel.Sentinel,
) *Attestation {
	return &Attestation{
		indiciesDB:   indiciesDB,
		beaconConfig: beaconConfig,
		subnets:      make(map[uint64]*subnetSubscription),
		netConfig:    netConfig,
	}
}

type subnetSubscription struct {
	subnetId      uint64
	gossipSub     *sentinel.GossipSubscription
	subscriptions []subscription
	attestations  []*solid.Attestation
	needAggregate bool
}

type subscription struct {
	validatorIndex uint64
	// subscription data structure?
}

func (a *Attestation) AddAttestationSubscription(p *cltypes.BeaconCommitteeSubscription) error {
	a.subnetAttMutex.Lock()
	defer a.subnetAttMutex.Unlock()
	// add subscription to attestationSubscriptions
	subnetId, err := a.computeSubnetId(p.Slot, p.CommitteeIndex)
	if err != nil {
		return err
	}
	if _, exist := a.subnets[subnetId]; !exist {
		a.subnets[subnetId] = &subnetSubscription{
			subnetId:      subnetId,
			subscriptions: make([]subscription, 0),
			attestations:  make([]*solid.Attestation, 0),
			needAggregate: false,
		}
		// todo: subscribe to attestation subnet topic
		topic := sentinel.GossipBeaconAttestationTopic(subnetId)
		gossipSub, err := a.sentinel.SubscribeGossip(topic)
		if err != nil {
			return err
		}
		if err := gossipSub.Listen(); err != nil {
			gossipSub.Close()
			return err
		}
		a.subnets[subnetId].gossipSub = gossipSub
	}
	theSubnet := a.subnets[subnetId]
	theSubnet.subscriptions = append(theSubnet.subscriptions, subscription{
		validatorIndex: p.ValidatorIndex,
	})

	if p.IsAggregator {
		theSubnet.needAggregate = true
	}
	return nil
}

func (a *Attestation) OnReceiveAttestation(att *solid.Attestation) {
	// todo:
	// add attestation to attestationSubscriptions
	// try aggregate?
}

func (a *Attestation) computeSubnetId(slot uint64, committeeIndex uint64) (uint64, error) {
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

func (a *Attestation) computeCommitteePerSlot(activeIndiciesLength uint64) uint64 {
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
