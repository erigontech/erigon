package attestation

import (
	"context"
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
	subnetAttMutex sync.Mutex
	subnets        map[uint64]*subnetSubscription // map from subnet id to subscription list
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
		subnets:      make(map[uint64]*subnetSubscription),
		netConfig:    netConfig,
	}
}

type subnetSubscription struct {
	subnetId             uint64
	subscribers          []validator
	needAggregate        bool
	aggregationSignature []byte
	aggregationBits      []byte
}

type validator struct {
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
	curSubnet, exist := a.subnets[subnetId]
	if !exist {
		a.subnets[subnetId] = &subnetSubscription{
			subnetId:             subnetId,
			subscribers:          make([]validator, 0),
			needAggregate:        false,
			aggregationSignature: nil,
			aggregationBits:      make([]byte, a.beaconConfig.MaxValidatorsPerCommittee/8),
		}
	}
	curSubnet.subscribers = append(curSubnet.subscribers, validator{
		validatorIndex: p.ValidatorIndex,
	})
	// todo: a.sentinel.SetGossipExpiration()
	if p.IsAggregator {
		curSubnet.needAggregate = true
	}
	return nil
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
	// acquire lock
	a.subnetAttMutex.Lock()
	defer a.subnetAttMutex.Unlock()
	curSubnet, exist := a.subnets[subnetId]
	if !exist {
		// no one is interested in this subnet
		return nil
	}

	// add attestation to the list
	if curSubnet.needAggregate {
		// aggregate
		sig := att.Signature()
		bits := att.AggregationBits()
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

		// collect aggregation bits
		for i := 0; i < len(curSubnet.aggregationBits); i++ {
			curSubnet.aggregationBits[i] |= bits[i]
		}
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
