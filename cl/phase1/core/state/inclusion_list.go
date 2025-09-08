package state

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
)

func IsValidInclusionListSignature(b *CachingBeaconState, signed solid.SignedInclusionList) bool {
	msg := signed.Message

	validatorIndexInt, err := strconv.Atoi(msg.ValidatorIndex)
	if err != nil {
		return false
	}

	val, err := b.ValidatorForValidatorIndex(validatorIndexInt)
	if err != nil {
		return false
	}
	publicKey, err := bls.NewPublicKeyFromBytes(val.PublicKeyBytes())
	if err != nil {
		return false
	}

	var signatureBytes []byte
	if strings.HasPrefix(signed.Signature, "0x") {
		signatureBytes, err = hex.DecodeString(signed.Signature[2:])
	} else {
		signatureBytes, err = hex.DecodeString(signed.Signature)
	}
	if err != nil {
		return false
	}

	signature, err := bls.NewSignatureFromBytes(signatureBytes)
	if err != nil {
		return false
	}

	slot, err := strconv.Atoi(msg.Slot)
	if err != nil {
		return false
	}
	epoch := GetEpochAtSlot(b.BeaconConfig(), uint64(slot))

	if b.Version() < clparams.FuluVersion {
		return false
	}

	domain, err := b.GetDomain(b.BeaconConfig().DomainInclusionListCommittee, epoch)
	if err != nil {
		return false
	}
	signingRoot, err := fork.ComputeSigningRoot(msg, domain)
	if err != nil {
		return false
	}

	if !signature.Verify(signingRoot[:], publicKey) {
		return false
	}

	// if all checks passes then IL is valid
	return true
}

func GetInclusionListCommittee(b *CachingBeaconState, signed solid.SignedInclusionList) ([]uint64, error) {
	msg := signed.Message

	slot, err := strconv.Atoi(msg.Slot)
	if err != nil {
		return nil, err
	}
	epoch := GetEpochAtSlot(b.BeaconConfig(), uint64(slot))
	beaconConfig := b.BeaconConfig()
	mixPosition := (epoch + beaconConfig.EpochsPerHistoricalVector - beaconConfig.MinSeedLookahead - 1) %
		beaconConfig.EpochsPerHistoricalVector

	mix := b.GetRandaoMix(int(mixPosition))
	seed := shuffling.GetSeed(beaconConfig, mix, epoch, beaconConfig.DomainInclusionListCommittee)

	indices := b.GetActiveValidatorsIndices(epoch)
	n := uint64(len(indices))

	if n == 0 {
		return nil, fmt.Errorf("no active validators found in the cache")
	}

	start := uint64(slot%int(beaconConfig.SlotsPerEpoch)) * beaconConfig.InclusionListCommitteeSize
	end := start + beaconConfig.InclusionListCommitteeSize

	preInputs := shuffling.ComputeShuffledIndexPreInputs(beaconConfig, seed)
	committee := make([]uint64, len(indices))

	for i := start; i < end; i++ {
		pos := i % n
		shuffled, err := shuffling.ComputeShuffledIndex(beaconConfig, pos, n, seed, preInputs, utils.Sha256)
		if err != nil {
			return nil, err
		}
		committee[i-start] = indices[shuffled]
	}
	return committee[start:end], nil
}
