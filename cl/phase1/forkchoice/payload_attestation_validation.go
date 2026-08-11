// Copyright 2026 The Erigon Authors
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
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
)

const (
	payloadAttestationValidationContextCacheSize = 128
	maxConcurrentValidationContextBuilds         = 1
)

type payloadAttestationValidationContext struct {
	slot       uint64
	domain     common.Hash
	positions  map[uint64][]int
	publicKeys map[uint64]common.Bytes48
}

type payloadAttestationValidationContexts struct {
	cache      *lru.Cache[common.Hash, *payloadAttestationValidationContext]
	buildSlots chan struct{}
}

func newPayloadAttestationValidationContexts() (*payloadAttestationValidationContexts, error) {
	cache, err := lru.New[common.Hash, *payloadAttestationValidationContext](
		"payload_attestation_validation_contexts",
		payloadAttestationValidationContextCacheSize,
	)
	if err != nil {
		return nil, err
	}
	return &payloadAttestationValidationContexts{
		cache:      cache,
		buildSlots: make(chan struct{}, maxConcurrentValidationContextBuilds),
	}, nil
}

func (c *payloadAttestationValidationContexts) get(
	ctx context.Context,
	blockRoot common.Hash,
	build func() (*payloadAttestationValidationContext, error),
) (*payloadAttestationValidationContext, error) {
	if validationContext, ok := c.cache.Get(blockRoot); ok {
		return validationContext, nil
	}
	select {
	case c.buildSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-c.buildSlots }()
	if validationContext, ok := c.cache.Get(blockRoot); ok {
		return validationContext, nil
	}
	validationContext, err := build()
	if err != nil {
		return nil, err
	}
	c.cache.Add(blockRoot, validationContext)
	return validationContext, nil
}

func (f *ForkChoiceStore) payloadAttestationValidationContext(
	ctx context.Context,
	blockRoot common.Hash,
	slot uint64,
) (*payloadAttestationValidationContext, error) {
	return f.payloadAttestationContexts.get(ctx, blockRoot, func() (*payloadAttestationValidationContext, error) {
		blockState, err := f.GetStateAtBlockRoot(blockRoot, true)
		if err != nil {
			return nil, err
		}
		if blockState == nil {
			return nil, fmt.Errorf("%w: block state not found for root %v", ErrIgnore, blockRoot)
		}
		if slot != blockState.Slot() {
			return nil, fmt.Errorf("%w: attestation slot %d does not match block slot %d", ErrIgnore, slot, blockState.Slot())
		}

		ptc, err := blockState.GetPTC(slot)
		if err != nil {
			return nil, err
		}
		if len(ptc) != int(blockState.BeaconConfig().PtcSize) {
			return nil, fmt.Errorf("invalid PTC length %d, expected %d", len(ptc), blockState.BeaconConfig().PtcSize)
		}
		domain, err := blockState.GetDomain(blockState.BeaconConfig().DomainPtcAttester, state.GetEpochAtSlot(blockState.BeaconConfig(), slot))
		if err != nil {
			return nil, fmt.Errorf("unable to get the domain: %w", err)
		}
		if len(domain) != len(common.Hash{}) {
			return nil, fmt.Errorf("invalid PTC attester domain length %d", len(domain))
		}

		validationContext := &payloadAttestationValidationContext{
			slot:       slot,
			positions:  make(map[uint64][]int, len(ptc)),
			publicKeys: make(map[uint64]common.Bytes48, len(ptc)),
		}
		copy(validationContext.domain[:], domain)
		for position, validatorIndex := range ptc {
			if validatorIndex >= uint64(blockState.ValidatorLength()) {
				return nil, fmt.Errorf("PTC validator %d is out of range", validatorIndex)
			}
			validationContext.positions[validatorIndex] = append(validationContext.positions[validatorIndex], position)
			if _, ok := validationContext.publicKeys[validatorIndex]; ok {
				continue
			}
			validator, err := blockState.ValidatorForValidatorIndex(int(validatorIndex))
			if err != nil {
				return nil, fmt.Errorf("failed to get PTC validator %d: %w", validatorIndex, err)
			}
			if len(validator.PublicKeyBytes()) != len(common.Bytes48{}) {
				return nil, fmt.Errorf("invalid public key length for PTC validator %d", validatorIndex)
			}
			var publicKey common.Bytes48
			copy(publicKey[:], validator.PublicKeyBytes())
			validationContext.publicKeys[validatorIndex] = publicKey
		}
		return validationContext, nil
	})
}

func (c *payloadAttestationValidationContext) ptcPositions(msg *cltypes.PayloadAttestationMessage) ([]int, error) {
	if msg.Data.Slot != c.slot {
		return nil, fmt.Errorf("%w: attestation slot %d does not match block slot %d", ErrIgnore, msg.Data.Slot, c.slot)
	}
	positions, ok := c.positions[msg.ValidatorIndex]
	if !ok {
		return nil, fmt.Errorf("validator %d is not in PTC for slot %d", msg.ValidatorIndex, msg.Data.Slot)
	}
	return positions, nil
}

func (c *payloadAttestationValidationContext) validateSignature(msg *cltypes.PayloadAttestationMessage) error {
	signingRoot, err := fork.ComputeSigningRoot(msg.Data, c.domain[:])
	if err != nil {
		return fmt.Errorf("unable to get signing root: %w", err)
	}
	publicKey := c.publicKeys[msg.ValidatorIndex]
	valid, err := bls.VerifyAggregate(msg.Signature[:], signingRoot[:], [][]byte{publicKey[:]})
	if err != nil {
		return fmt.Errorf("error while validating signature: %w", err)
	}
	if !valid {
		return errors.New("invalid payload attestation signature")
	}
	return nil
}
