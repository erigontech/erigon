package transition

import (
	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

func VerifyBlobSideCarSignature(state *state.BeaconState, signedBlobSideCar *cltypes.SignedBlobSideCar) (bool, error) {
	proposer := state.Validators()[signedBlobSideCar.Message.ProposerIndex]
	domain, err := state.GetDomain(state.BeaconConfig().DomainBlobSideCar, state.Epoch())
	if err != nil {
		return false, err
	}
	signingRoot, err := fork.ComputeSigningRoot(signedBlobSideCar.Message, domain)
	if err != nil {
		return false, err
	}

	return bls.Verify(proposer.PublicKey[:], signingRoot[:], signedBlobSideCar.Signature[:])
}
