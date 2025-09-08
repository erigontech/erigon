package state

import (
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils/bls"
)

func IsValidInclusionListSignature(b abstract.BeaconStateBasic, signed solid.SignedInclusionList) bool {
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

	slot, err := strconv.Atoi(msg.ValidatorIndex)
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
