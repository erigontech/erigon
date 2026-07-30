// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package forkchoice

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/bls"

	"github.com/erigontech/erigon/cl/cltypes"
)

func (f *ForkChoiceStore) OnAttesterSlashing(attesterSlashing *cltypes.AttesterSlashing, test bool) error {
	if attesterSlashing == nil ||
		attesterSlashing.Attestation_1 == nil ||
		attesterSlashing.Attestation_2 == nil ||
		attesterSlashing.Attestation_1.AttestingIndices == nil ||
		attesterSlashing.Attestation_2.AttestingIndices == nil {
		return errors.New("invalid attester slashing")
	}
	if f.operationsPool.AttesterSlashingsPool.Has(pool.ComputeKeyForAttesterSlashing(attesterSlashing)) {
		return ErrIgnore
	}
	f.mu.Lock()
	defer f.drainQueuedWork()
	defer f.mu.Unlock()

	if f.syncedDataManager.Syncing() {
		s, err := f.forkGraph.GetState(f.justifiedCheckpoint.Load().(solid.Checkpoint).Root, false)
		if err != nil {
			return err
		}
		return f.onProcessAttesterSlashing(attesterSlashing, s, test)
	}

	return f.syncedDataManager.ViewHeadState(func(s *state.CachingBeaconState) error {
		return f.onProcessAttesterSlashing(attesterSlashing, s, test)
	})
}

func (f *ForkChoiceStore) onProcessAttesterSlashing(attesterSlashing *cltypes.AttesterSlashing, s *state.CachingBeaconState, test bool) error {
	if s == nil {
		return errors.New("no state accessible")
	}

	// Check if this attestation is even slashable.
	attestation1 := attesterSlashing.Attestation_1
	attestation2 := attesterSlashing.Attestation_2
	if attestation1.AttestingIndices.Length() > attestation1.AttestingIndices.Cap() ||
		attestation2.AttestingIndices.Length() > attestation2.AttestingIndices.Cap() {
		return errors.New("attesting indices exceed limit")
	}
	intersection := intersectAttestingIndices(attestation1.AttestingIndices, attestation2.AttestingIndices)
	if f.allAttesterSlashingIndicesSeen(intersection) {
		return ErrIgnore
	}
	if attestation1.Data == nil || attestation2.Data == nil {
		return errors.New("invalid attester slashing data")
	}
	if !cltypes.IsSlashableAttestationData(attestation1.Data, attestation2.Data) {
		return errors.New("attestation data is not slashable")
	}
	attestation1PublicKeys, err := getIndexedAttestationPublicKeys(s, attestation1)
	if err != nil {
		return err
	}
	attestation2PublicKeys, err := getIndexedAttestationPublicKeys(s, attestation2)
	if err != nil {
		return err
	}
	intersection = solid.IntersectionOfSortedSets(attestation1.AttestingIndices, attestation2.AttestingIndices)
	domain1, err := s.GetDomain(s.BeaconConfig().DomainBeaconAttester, attestation1.Data.Target.Epoch)
	if err != nil {
		return fmt.Errorf("unable to get the domain: %v", err)
	}
	domain2, err := s.GetDomain(s.BeaconConfig().DomainBeaconAttester, attestation2.Data.Target.Epoch)
	if err != nil {
		return fmt.Errorf("unable to get the domain: %v", err)
	}

	if !test {
		// Verify validity of slashings (1)
		signingRoot, err := fork.ComputeSigningRoot(attestation1.Data, domain1)
		if err != nil {
			return fmt.Errorf("unable to get signing root: %v", err)
		}

		valid, err := bls.VerifyAggregate(attestation1.Signature[:], signingRoot[:], attestation1PublicKeys)
		if err != nil {
			return fmt.Errorf("error while validating signature: %v", err)
		}
		if !valid {
			return errors.New("invalid aggregate signature")
		}
		// Verify validity of slashings (2)
		signingRoot, err = fork.ComputeSigningRoot(attestation2.Data, domain2)
		if err != nil {
			return fmt.Errorf("unable to get signing root: %v", err)
		}

		valid, err = bls.VerifyAggregate(attestation2.Signature[:], signingRoot[:], attestation2PublicKeys)
		if err != nil {
			return fmt.Errorf("error while validating signature: %v", err)
		}
		if !valid {
			return errors.New("invalid aggregate signature")
		}
	}

	var anySlashed bool
	for _, index := range intersection {
		f.setUnequivocating(index)
		if !anySlashed {
			v, err := s.ValidatorForValidatorIndex(int(index))
			if err != nil {
				return fmt.Errorf("unable to retrieve state: %v", err)
			}
			if v.IsSlashable(state.Epoch(s)) {
				anySlashed = true
			}
		}
	}
	if anySlashed {
		f.operationsPool.AttesterSlashingsPool.Insert(pool.ComputeKeyForAttesterSlashing(attesterSlashing), attesterSlashing)
		f.queueEmit(func() { f.emitters.Operation().SendAttesterSlashing(attesterSlashing) })
	}
	return nil
}

func intersectAttestingIndices(left, right solid.IterableSSZ[uint64]) []uint64 {
	if left.Length() > right.Length() {
		left, right = right, left
	}
	leftIndices := make(map[uint64]struct{}, left.Length())
	for i := 0; i < left.Length(); i++ {
		leftIndices[left.Get(i)] = struct{}{}
	}
	intersection := make([]uint64, 0, min(left.Length(), right.Length()))
	for i := 0; i < right.Length(); i++ {
		index := right.Get(i)
		if _, ok := leftIndices[index]; ok {
			intersection = append(intersection, index)
			delete(leftIndices, index)
		}
	}
	return intersection
}

func (f *ForkChoiceStore) allAttesterSlashingIndicesSeen(indices []uint64) bool {
	if len(indices) == 0 {
		return false
	}
	for _, index := range indices {
		if !f.isUnequivocating(index) {
			return false
		}
	}
	return true
}

func getIndexedAttestationPublicKeys(b *state.CachingBeaconState, att *cltypes.IndexedAttestation) ([][]byte, error) {
	inds := att.AttestingIndices
	if inds.Length() == 0 || !solid.IsUint64SortedSet(inds) {
		return nil, errors.New("isValidIndexedAttestation: attesting indices are not sorted or are null")
	}
	pks := make([][]byte, 0, inds.Length())
	if err := solid.RangeErr[uint64](inds, func(_ int, v uint64, _ int) error {
		val, err := b.ValidatorForValidatorIndex(int(v))
		if err != nil {
			return err
		}
		pk := val.PublicKey()
		pks = append(pks, pk[:])
		return nil
	}); err != nil {
		return nil, err
	}
	return pks, nil
}
