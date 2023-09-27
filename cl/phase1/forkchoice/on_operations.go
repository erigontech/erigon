package forkchoice

import (
	"errors"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
)

// NOTE: This file implements non-official handlers for other types of iterations. what it does is,using the forkchoices
// and verify external operations and eventually push them in the operations pool.

// OnVoluntaryExit is a non-official handler for voluntary exit operations. it pushes the voluntary exit in the pool.
func (f *ForkChoiceStore) OnVoluntaryExit(signedVoluntaryExit *cltypes.SignedVoluntaryExit, test bool) (invalid bool, err error) {
	voluntaryExit := signedVoluntaryExit.VoluntaryExit
	if f.operationsPool.VoluntaryExistsPool.Has(voluntaryExit.ValidatorIndex) {
		return false, err
	}
	f.mu.Lock()

	headHash, _, err := f.getHead()
	if err != nil {
		f.mu.Unlock()
		return false, err
	}
	s, _, err := f.forkGraph.GetState(headHash, false)
	if err != nil {
		f.mu.Unlock()
		return false, err
	}

	val, err := s.ValidatorForValidatorIndex(int(voluntaryExit.ValidatorIndex))
	if err != nil {
		f.mu.Unlock()
		return true, err
	}

	if val.ExitEpoch() != f.forkGraph.Config().FarFutureEpoch {
		f.mu.Unlock()
		return false, nil
	}

	pk := val.PublicKey()
	f.mu.Unlock()

	domain, err := s.GetDomain(s.BeaconConfig().DomainVoluntaryExit, voluntaryExit.Epoch)
	if err != nil {
		return false, err
	}
	signingRoot, err := fork.ComputeSigningRoot(voluntaryExit, domain)
	if err != nil {
		return true, err
	}
	if !test {
		valid, err := bls.Verify(signedVoluntaryExit.Signature[:], signingRoot[:], pk[:])
		if err != nil {
			return true, err
		}
		if !valid {
			return true, errors.New("ProcessVoluntaryExit: BLS verification failed")
		}
	}
	f.operationsPool.VoluntaryExistsPool.Insert(voluntaryExit.ValidatorIndex, signedVoluntaryExit)
	return false, nil
}
