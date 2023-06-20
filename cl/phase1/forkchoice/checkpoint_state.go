package forkchoice

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/shuffling"

	"github.com/Giulio2002/bls"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
)

const randaoMixesLength = 65536

// Active returns if validator is active for given epoch
func (cv *checkpointValidator) active(epoch uint64) bool {
	return cv.ActivationEpoch <= epoch && epoch < cv.ExitEpoch
}

type checkpointValidator struct {
	PublicKey       [48]byte
	ActivationEpoch uint64
	ExitEpoch       uint64
	Balance         uint64
	Slashed         bool
}

type shuffledSet struct {
	Set       []uint64
	LenActive uint64
}

// We only keep in memory a fraction of the beacon state
type checkpointState struct {
	beaconConfig      *clparams.BeaconChainConfig
	randaoMixes       solid.HashVectorSSZ
	shuffledSetsCache map[uint64]*shuffledSet // Map each epoch to its shuffled index
	// public keys list
	validators []*checkpointValidator
	// fork data
	genesisValidatorsRoot libcommon.Hash
	fork                  *cltypes.Fork
	activeBalance, epoch  uint64 // current active balance and epoch
}

// encoding format:
// [
// 1 bytesize:[
//  2 [genesisValidatorsRoot][activeBalance][epoch][fork]
//  3 [bytesize:[randao ssz]]
//  4 [bytesize:[validator gob]]
//  5 [bytesize:[beaconConfig gob]]
//  6 [bytesize:[shuffledsets gob]]
// ]
// ]

func (c *checkpointState) UnmarshalBinary(data []byte) (err error) {
	rd := bytes.NewBuffer(data)
	// 1. length prefix,
	var leng uint32
	if err := binary.Read(rd, binary.LittleEndian, &leng); err != nil {
		return err
	}
	rd = bytes.NewBuffer(rd.Next(int(leng)))
	// 2. read the header
	if err = binary.Read(rd, binary.LittleEndian, &c.genesisValidatorsRoot); err != nil {
		return err
	}
	if err = binary.Read(rd, binary.LittleEndian, &c.activeBalance); err != nil {
		return err
	}
	if err = binary.Read(rd, binary.LittleEndian, &c.epoch); err != nil {
		return err
	}
	if err = binary.Read(rd, binary.LittleEndian, &c.fork); err != nil {
		return err
	}
	// 3. randaoMixes
	if err = binary.Read(rd, binary.LittleEndian, &leng); err != nil {
		return err
	}
	// note that version is ignored
	if err := c.randaoMixes.DecodeSSZ(rd.Next(int(leng)), 0); err != nil {
		return err
	}
	// 4. validator
	if err = binary.Read(rd, binary.LittleEndian, &leng); err != nil {
		return err
	}
	if err := gob.NewDecoder(bytes.NewBuffer(rd.Next(int(leng)))).
		Decode(&c.validators); err != nil {
		return err
	}
	// 5. beaconConfig
	if err = binary.Read(rd, binary.LittleEndian, &leng); err != nil {
		return err
	}
	if err := gob.NewDecoder(bytes.NewBuffer(rd.Next(int(leng)))).
		Decode(&c.beaconConfig); err != nil {
		return err
	}
	// 6. shuffledSetsCache
	if err = binary.Read(rd, binary.LittleEndian, &leng); err != nil {
		return err
	}
	if err := gob.NewDecoder(bytes.NewBuffer(rd.Next(int(leng)))).
		Decode(&c.shuffledSetsCache); err != nil {
		return err
	}

	return nil
}

func (c *checkpointState) MarshalBinary() (data []byte, err error) {
	o := new(bytes.Buffer)
	tmp := new(bytes.Buffer)

	// 1 leave four bytes for eventual length offset of entire packet
	o.Write(make([]byte, 4))

	// 2 encode the fixed fields first, as binary
	binary.Write(o, binary.LittleEndian, c.genesisValidatorsRoot)
	binary.Write(o, binary.LittleEndian, c.activeBalance)
	binary.Write(o, binary.LittleEndian, c.epoch)
	binary.Write(o, binary.LittleEndian, c.fork)

	// 3 ssz encode the randao
	randao, err := c.randaoMixes.EncodeSSZ(tmp.Bytes())
	if err != nil {
		return nil, err
	}
	// byte size prefix
	binary.Write(o, binary.LittleEndian, uint32(len(randao)))
	o.Write(randao)
	tmp.Reset()
	// fin

	//4  gob encode the validators
	tmp.Reset()
	err = gob.NewEncoder(tmp).Encode(c.validators)
	if err != nil {
		return
	}
	binary.Write(o, binary.LittleEndian, uint32(tmp.Len()))
	tmp.WriteTo(o)
	tmp.Reset()
	// fin

	//5 gob encode the beacon chain config
	tmp.Reset()
	err = gob.NewEncoder(tmp).Encode(c.beaconConfig)
	if err != nil {
		return
	}
	binary.Write(o, binary.LittleEndian, uint32(tmp.Len()))
	tmp.WriteTo(o)
	tmp.Reset()
	// fin

	//6 gob encode the shuffledSetsCache
	tmp.Reset()
	err = gob.NewEncoder(tmp).Encode(c.shuffledSetsCache)
	if err != nil {
		return
	}
	binary.Write(o, binary.LittleEndian, uint32(tmp.Len()))
	tmp.WriteTo(o)
	tmp.Reset()
	// fin

	bts := o.Bytes()

	// put the length prefix
	binary.LittleEndian.PutUint32(bts[:4], uint32(len(bts))-4)
	return bts, nil
}

func newCheckpointState(beaconConfig *clparams.BeaconChainConfig, validatorSet []solid.Validator, randaoMixes solid.HashVectorSSZ,
	genesisValidatorsRoot libcommon.Hash, fork *cltypes.Fork, activeBalance, epoch uint64) *checkpointState {
	validators := make([]*checkpointValidator, len(validatorSet))
	for i := range validatorSet {
		validators[i] = &checkpointValidator{
			PublicKey:       validatorSet[i].PublicKey(),
			ActivationEpoch: validatorSet[i].ActivationEpoch(),
			ExitEpoch:       validatorSet[i].ExitEpoch(),
			Balance:         validatorSet[i].EffectiveBalance(),
			Slashed:         validatorSet[i].Slashed(),
		}
	}
	mixes := solid.NewHashVector(randaoMixesLength)
	randaoMixes.CopyTo(mixes)
	return &checkpointState{
		beaconConfig:          beaconConfig,
		randaoMixes:           mixes,
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

	beaconConfig := c.beaconConfig

	mixPosition := (epoch + beaconConfig.EpochsPerHistoricalVector - beaconConfig.MinSeedLookahead - 1) %
		beaconConfig.EpochsPerHistoricalVector
	// Input for the seed hash.

	if shuffledIndicesCached, ok := c.shuffledSetsCache[epoch]; ok {
		shuffledIndicies = shuffledIndicesCached.Set
		lenIndicies = shuffledIndicesCached.LenActive
	} else {
		activeIndicies := c.getActiveIndicies(epoch)
		lenIndicies = uint64(len(activeIndicies))
		shuffledIndicies = shuffling.ComputeShuffledIndicies(c.beaconConfig, c.randaoMixes.Get(int(mixPosition)), activeIndicies, slot)
		c.shuffledSetsCache[epoch] = &shuffledSet{Set: shuffledIndicies, LenActive: uint64(len(activeIndicies))}
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
	if inds.Length() == 0 || !solid.IsUint64SortedSet(inds) {
		return false, fmt.Errorf("isValidIndexedAttestation: attesting indices are not sorted or are null")
	}

	pks := [][]byte{}
	inds.Range(func(_ int, v uint64, _ int) bool {
		publicKey := c.validators[v].PublicKey
		pks = append(pks, publicKey[:])
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
