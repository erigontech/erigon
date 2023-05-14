package forkchoice

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"

	"github.com/Giulio2002/bls"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
)

const randaoMixesLength = 65536

// Active returns if validator is active for given epoch
func (cv *checkpointValidator) active(epoch uint64) bool {
	return cv.activationEpoch <= epoch && epoch < cv.exitEpoch
}

type checkpointValidator struct {
	publicKey       [48]byte
	activationEpoch uint64
	exitEpoch       uint64
	balance         uint64
	slashed         bool
}

type shuffledSet struct {
	set       []uint64
	lenActive uint64
}

// We only keep in memory a fraction of the beacon state
type checkpointState struct {
	beaconConfig      *clparams.BeaconChainConfig
	randaoMixes       []libcommon.Hash
	shuffledSetsCache map[uint64]*shuffledSet // Map each epoch to its shuffled index
	// public keys list
	validators []*checkpointValidator
	// fork data
	genesisValidatorsRoot libcommon.Hash
	fork                  *cltypes.Fork
	activeBalance, epoch  uint64 // current active balance and epoch
}

func copyMixes(mixes []libcommon.Hash) (ret []libcommon.Hash) {
	ret = make([]libcommon.Hash, len(mixes))
	copy(ret, mixes)
	return
}

func newCheckpointState(beaconConfig *clparams.BeaconChainConfig, validatorSet []*cltypes.Validator, randaoMixes []libcommon.Hash,
	genesisValidatorsRoot libcommon.Hash, fork *cltypes.Fork, activeBalance, epoch uint64) *checkpointState {
	validators := make([]*checkpointValidator, len(validatorSet))
	for i := range validatorSet {
		validators[i] = &checkpointValidator{
			publicKey:       validatorSet[i].PublicKey(),
			activationEpoch: validatorSet[i].ActivationEpoch(),
			exitEpoch:       validatorSet[i].ExitEpoch(),
			balance:         validatorSet[i].EffectiveBalance(),
			slashed:         validatorSet[i].Slashed(),
		}
	}
	return &checkpointState{
		beaconConfig:          beaconConfig,
		randaoMixes:           copyMixes(randaoMixes),
		validators:            validators,
		genesisValidatorsRoot: genesisValidatorsRoot,
		fork:                  fork,
		shuffledSetsCache:     map[uint64]*shuffledSet{},
		activeBalance:         activeBalance,
		epoch:                 epoch,
	}
}

// getAttestingIndicies retrieves the beacon committee.
func (c *checkpointState) getAttestingIndicies(attestation *solid.AttestationData, aggregationBits []byte) ([]uint64, error) {
	// First get beacon committee
	slot := attestation.Slot()
	epoch := c.epochAtSlot(slot)
	// Compute shuffled indicies
	var shuffledIndicies []uint64
	var lenIndicies uint64
	if shuffledIndicesCached, ok := c.shuffledSetsCache[epoch]; ok {
		shuffledIndicies = shuffledIndicesCached.set
		lenIndicies = shuffledIndicesCached.lenActive
	} else {
		activeIndicies := c.getActiveIndicies(epoch)
		lenIndicies = uint64(len(activeIndicies))
		shuffledIndicies = shuffling.ComputeShuffledIndicies(c.beaconConfig, c.randaoMixes, activeIndicies, slot)
		c.shuffledSetsCache[epoch] = &shuffledSet{set: shuffledIndicies, lenActive: uint64(len(activeIndicies))}
	}
	committeesPerSlot := c.committeeCount(epoch, lenIndicies)
	count := committeesPerSlot * c.beaconConfig.SlotsPerEpoch
	index := (slot%c.beaconConfig.SlotsPerEpoch)*committeesPerSlot + attestation.ValidatorIndex()
	start := (lenIndicies * index) / count
	end := (lenIndicies * (index + 1)) / count
	committee := shuffledIndicies[start:end]

	attestingIndices := []uint64{}
	for i, member := range committee {
		bitIndex := i % 8
		sliceIndex := i / 8
		if sliceIndex >= len(aggregationBits) {
			return nil, fmt.Errorf("GetAttestingIndicies: committee is too big")
		}
		if (aggregationBits[sliceIndex] & (1 << bitIndex)) > 0 {
			attestingIndices = append(attestingIndices, member)
		}
	}
	return attestingIndices, nil
}

func (c *checkpointState) getActiveIndicies(epoch uint64) (activeIndicies []uint64) {
	for i, validator := range c.validators {
		if !validator.active(epoch) {
			continue
		}
		activeIndicies = append(activeIndicies, uint64(i))
	}
	return activeIndicies
}

// committeeCount retrieves size of sync committee
func (c *checkpointState) committeeCount(epoch, lenIndicies uint64) uint64 {
	committeCount := lenIndicies / c.beaconConfig.SlotsPerEpoch / c.beaconConfig.TargetCommitteeSize
	if c.beaconConfig.MaxCommitteesPerSlot < committeCount {
		committeCount = c.beaconConfig.MaxCommitteesPerSlot
	}
	if committeCount < 1 {
		committeCount = 1
	}
	return committeCount
}

func (c *checkpointState) getDomain(domainType [4]byte, epoch uint64) ([]byte, error) {
	if epoch < c.fork.Epoch {
		return fork.ComputeDomain(domainType[:], c.fork.PreviousVersion, c.genesisValidatorsRoot)
	}
	return fork.ComputeDomain(domainType[:], c.fork.CurrentVersion, c.genesisValidatorsRoot)
}

// isValidIndexedAttestation verifies indexed attestation
func (c *checkpointState) isValidIndexedAttestation(att *cltypes.IndexedAttestation) (bool, error) {
	inds := att.AttestingIndices
	if len(inds) == 0 || !utils.IsSliceSortedSet(inds) {
		return false, fmt.Errorf("isValidIndexedAttestation: attesting indices are not sorted or are null")
	}

	pks := [][]byte{}
	for _, v := range inds {
		publicKey := c.validators[v].publicKey
		pks = append(pks, publicKey[:])
	}

	domain, err := c.getDomain(c.beaconConfig.DomainBeaconAttester, att.Data.Target().Epoch())
	if err != nil {
		return false, fmt.Errorf("unable to get the domain: %v", err)
	}

	signingRoot, err := fork.ComputeSigningRoot(att.Data, domain)
	if err != nil {
		return false, fmt.Errorf("unable to get signing root: %v", err)
	}

	valid, err := bls.VerifyAggregate(att.Signature[:], signingRoot[:], pks)
	if err != nil {
		return false, fmt.Errorf("error while validating signature: %v", err)
	}
	if !valid {
		return false, fmt.Errorf("invalid aggregate signature")
	}
	return true, nil
}
func (c *checkpointState) epochAtSlot(slot uint64) uint64 {
	return slot / c.beaconConfig.SlotsPerEpoch
}
