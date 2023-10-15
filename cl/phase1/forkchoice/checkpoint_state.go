package forkchoice

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"

	"github.com/Giulio2002/bls"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
)

const randaoMixesLength = 65536

// We only keep in memory a fraction of the beacon state when it comes to checkpoint.
type checkpointState struct {
	beaconConfig *clparams.BeaconChainConfig
	randaoMixes  solid.HashVectorSSZ
	shuffledSet  []uint64 // shuffled set of active validators
	// validator data
	balances []uint64
	// These are flattened to save memory and anchor public keys are static and shared.
	anchorPublicKeys []byte // flattened base public keys
	publicKeys       []byte // flattened public keys
	actives          []byte
	slasheds         []byte

	validatorSetSize int
	// fork data
	genesisValidatorsRoot libcommon.Hash
	fork                  *cltypes.Fork
	activeBalance, epoch  uint64 // current active balance and epoch
}

func writeToBitset(bitset []byte, i int, value bool) {
	bitIndex := i % 8
	sliceIndex := i / 8
	if value {
		bitset[sliceIndex] = ((1 << bitIndex) | bitset[sliceIndex])
	} else {
		bitset[sliceIndex] &= ^(1 << uint(bitIndex))
	}
}

func readFromBitset(bitset []byte, i int) bool {
	bitIndex := i % 8
	sliceIndex := i / 8
	return (bitset[sliceIndex] & (1 << uint(bitIndex))) > 0
}

func newCheckpointState(beaconConfig *clparams.BeaconChainConfig, anchorPublicKeys []byte, validatorSet []solid.Validator, randaoMixes solid.HashVectorSSZ,
	genesisValidatorsRoot libcommon.Hash, fork *cltypes.Fork, activeBalance, epoch uint64) *checkpointState {
	publicKeys := make([]byte, (len(validatorSet)-(len(anchorPublicKeys)/length.Bytes48))*length.Bytes48)
	balances := make([]uint64, len(validatorSet))

	bitsetSize := (len(validatorSet) + 7) / 8
	actives := make([]byte, bitsetSize)
	slasheds := make([]byte, bitsetSize)
	for i := range validatorSet {
		balances[i] = validatorSet[i].EffectiveBalance()
		writeToBitset(actives, i, validatorSet[i].Active(epoch))
		writeToBitset(slasheds, i, validatorSet[i].Slashed())
	}
	// Add the post-anchor public keys as surplus
	for i := len(anchorPublicKeys) / length.Bytes48; i < len(validatorSet); i++ {
		pos := i - len(anchorPublicKeys)/length.Bytes48
		copy(publicKeys[pos*length.Bytes48:], validatorSet[i].PublicKeyBytes())
	}

	mixes := solid.NewHashVector(randaoMixesLength)
	randaoMixes.CopyTo(mixes)

	// bitsets size
	c := &checkpointState{
		beaconConfig:          beaconConfig,
		randaoMixes:           mixes,
		balances:              balances,
		anchorPublicKeys:      anchorPublicKeys,
		publicKeys:            publicKeys,
		genesisValidatorsRoot: genesisValidatorsRoot,
		fork:                  fork,
		activeBalance:         activeBalance,
		slasheds:              slasheds,
		actives:               actives,
		validatorSetSize:      len(validatorSet),

		epoch: epoch,
	}
	mixPosition := (epoch + beaconConfig.EpochsPerHistoricalVector - beaconConfig.MinSeedLookahead - 1) %
		beaconConfig.EpochsPerHistoricalVector
	activeIndicies := c.getActiveIndicies(epoch)
	c.shuffledSet = shuffling.ComputeShuffledIndicies(c.beaconConfig, c.randaoMixes.Get(int(mixPosition)), activeIndicies, epoch*beaconConfig.SlotsPerEpoch)
	return c
}

// getAttestingIndicies retrieves the beacon committee.
func (c *checkpointState) getAttestingIndicies(attestation *solid.AttestationData, aggregationBits []byte) ([]uint64, error) {
	// First get beacon committee
	slot := attestation.Slot()
	epoch := c.epochAtSlot(slot)
	// Compute shuffled indicies

	lenIndicies := uint64(len(c.shuffledSet))
	committeesPerSlot := c.committeeCount(epoch, lenIndicies)
	count := committeesPerSlot * c.beaconConfig.SlotsPerEpoch
	index := (slot%c.beaconConfig.SlotsPerEpoch)*committeesPerSlot + attestation.ValidatorIndex()
	start := (lenIndicies * index) / count
	end := (lenIndicies * (index + 1)) / count
	committee := c.shuffledSet[start:end]

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
	for i := 0; i < c.validatorSetSize; i++ {
		if !readFromBitset(c.actives, i) {
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
	if inds.Length() == 0 || !solid.IsUint64SortedSet(inds) {
		return false, fmt.Errorf("isValidIndexedAttestation: attesting indices are not sorted or are null")
	}

	pks := [][]byte{}
	inds.Range(func(_ int, v uint64, _ int) bool {
		if v < uint64(len(c.anchorPublicKeys)) {
			pks = append(pks, c.anchorPublicKeys[v*length.Bytes48:(v+1)*length.Bytes48])
		} else {
			offset := uint64(len(c.anchorPublicKeys) / length.Bytes48)
			pks = append(pks, c.publicKeys[(v-offset)*length.Bytes48:])
		}
		return true
	})

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
