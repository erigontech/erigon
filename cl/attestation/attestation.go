package attestation

import (
	"context"
	"fmt"
	"sync"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/log/v3"
)

type Attestation struct {
	indiciesDB   kv.RoDB
	beaconConfig *clparams.BeaconChainConfig
	netConfig    *clparams.NetworkConfig
	sentinel     sentinel.SentinelClient
	// subscriptions
	//subnetAttMutex sync.Mutex
	//subnets        map[uint64]*subnetSubscription // map from subnet id to subscription list
	aggregationMutex   sync.RWMutex
	aggregations       map[string]*aggregateData // map from slot:committeeIndex to aggregate data
	validatorSubsMutex sync.RWMutex
	validatorSubs      map[uint64][]*validatorSub // map from validator index to subscription details
}

func NewAttestation(
	ctx context.Context,
	indiciesDB kv.RoDB,
	beaconConfig *clparams.BeaconChainConfig,
	netConfig *clparams.NetworkConfig,
	sentinel sentinel.SentinelClient,
) *Attestation {
	return &Attestation{
		indiciesDB:   indiciesDB,
		beaconConfig: beaconConfig,
		netConfig:    netConfig,
		sentinel:     sentinel,
		//subnets:      make(map[uint64]*subnetSubscription),
		aggregations:  make(map[string]*aggregateData),
		validatorSubs: make(map[uint64][]*validatorSub),
	}
}

type aggregateData struct {
	subnetId       uint64
	slot           uint64
	committeeIndex uint64
	internalLock   sync.RWMutex
	signature      []byte
	bits           []byte
}

/*
	type subnetSubscription struct {
		subnetId             uint64
		slot                 uint64
		subscribers          map[uint64]validator // map from validator index to subscription details
		needAggregate        bool
		aggregationSignature []byte
		aggregationBits      []byte
	}
*/
type validatorSub struct {
	subnetId       uint64
	slot           uint64
	committeeIndex uint64
}

func toAggregationId(slot, committeeIndex uint64) string {
	return fmt.Sprintf("%d:%d", slot, committeeIndex)
}

func (a *Attestation) AddAttestationSubscription(p *cltypes.BeaconCommitteeSubscription) error {
	subnetId, err := a.computeSubnetId(p.Slot, p.CommitteeIndex)
	if err != nil {
		return err
	}
	// 1. add validator to subscription
	a.validatorSubsMutex.Lock()
	if _, exist := a.validatorSubs[p.ValidatorIndex]; !exist {
		a.validatorSubs[p.ValidatorIndex] = make([]*validatorSub, 0)
	}
	a.validatorSubs[p.ValidatorIndex] = append(a.validatorSubs[p.ValidatorIndex], &validatorSub{
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
			a.aggregations[aggrId] = &aggregateData{
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

	/*
		a.subnetAttMutex.Lock()
		defer a.subnetAttMutex.Unlock()
		// add subscription to attestationSubscriptions
		curSubnet, exist := a.subnets[subnetId]
		if !exist {
			// a new subnet
			a.subnets[subnetId] = &subnetSubscription{
				subnetId:             subnetId,
				subscribers:          make(map[uint64]validator),
				needAggregate:        false,
				aggregationSignature: nil,
				aggregationBits:      make([]byte, a.beaconConfig.MaxValidatorsPerCommittee/8),
			}
		}
		curSubnet.subscribers[p.ValidatorIndex] = validator{
			// todo: might need to consider expiration
			expiry: time.Now().Add(7 * 24 * time.Hour),
		}

		// todo: a.sentinel.SetGossipExpiration()
		if p.IsAggregator {
			curSubnet.needAggregate = true
		}
		return nil
	*/
}

func (a *Attestation) OnReceiveAttestation(att *solid.Attestation) error {
	// compute subnet id
	slot := att.AttestantionData().Slot()
	committeeIndex := att.AttestantionData().CommitteeIndex()
	subnetId, err := a.computeSubnetId(slot, committeeIndex)
	if err != nil {
		log.Error("computeSubnetId failed", "err", err)
		return err
	}

	a.subnetAttMutex.Lock()
	defer a.subnetAttMutex.Unlock()
	curSubnet, exist := a.subnets[subnetId]
	if !exist {
		// no one is interested in this subnet
		return nil
	}

	if curSubnet.needAggregate {
		sig := att.Signature()
		bits := att.AggregationBits()
		bitGroupIdx := -1
		// check if already have aggregation signature associated with the bit. if not, add it
		for i := 0; i < len(bits); i++ {
			if bits[i] == 0 {
				continue
			} else if bits[i]|curSubnet.aggregationBits[i] == curSubnet.aggregationBits[i] {
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
		signatures := [][]byte{sigBytes}
		if curSubnet.aggregationSignature != nil {
			signatures = append(signatures, curSubnet.aggregationSignature)
			aggrSig, err := bls.AggregateSignatures(signatures)
			if err != nil {
				log.Error("aggregate signature failed", "err", err)
				return err
			}
			curSubnet.aggregationSignature = aggrSig
		} else {
			curSubnet.aggregationSignature = sigBytes
		}
		// update aggregation bits
		curSubnet.aggregationBits[bitGroupIdx] |= bits[bitGroupIdx]
	}
	return nil
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
