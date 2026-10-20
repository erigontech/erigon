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

package parlia

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

const (
	extraVanity          = 32
	extraSeal            = 65
	validatorNumberSize  = 1
	turnLengthSize       = 1
	blsPublicKeyLength   = 48
	blsSignatureLength   = 96
	validatorBytesLength = length.Addr + blsPublicKeyLength
)

var (
	ErrNotEpochHeader     = errors.New("parlia: not an epoch header")
	ErrInvalidValidators  = errors.New("parlia: invalid validator list in epoch header")
	ErrInvalidTurnLength  = errors.New("parlia: invalid turn length in epoch header")
	ErrInvalidAttestation = errors.New("parlia: invalid vote attestation in header extra")
)

type BLSPublicKey [blsPublicKeyLength]byte

type BLSSignature [blsSignatureLength]byte

type VoteData struct {
	SourceNumber uint64
	SourceHash   common.Hash
	TargetNumber uint64
	TargetHash   common.Hash
}

func (d *VoteData) Hash() common.Hash {
	enc, err := rlp.EncodeToBytes(d)
	if err != nil {
		panic(err)
	}
	return crypto.Keccak256Hash(enc)
}

type VoteAttestation struct {
	VoteAddressSet uint64
	AggSignature   BLSSignature
	Data           *VoteData
	Extra          []byte
}

// The extra layout from Bohr on is
// vanity | [epoch: count, count × (address, BLS key), turn length] | [RLP vote attestation] | seal.
func epochValidatorsEnd(h *types.Header, epochLength uint64) (int, error) {
	if h.Number.Uint64()%epochLength != 0 {
		return 0, ErrNotEpochHeader
	}
	if len(h.Extra) <= extraVanity+extraSeal {
		return 0, ErrInvalidValidators
	}
	num := int(h.Extra[extraVanity])
	end := extraVanity + validatorNumberSize + num*validatorBytesLength
	if num == 0 || len(h.Extra) < end+turnLengthSize+extraSeal {
		return 0, ErrInvalidValidators
	}
	return end, nil
}

func ParseValidators(h *types.Header, epochLength uint64) ([]common.Address, []BLSPublicKey, error) {
	end, err := epochValidatorsEnd(h, epochLength)
	if err != nil {
		return nil, nil, err
	}
	raw := h.Extra[extraVanity+validatorNumberSize : end]
	n := len(raw) / validatorBytesLength
	addrs := make([]common.Address, n)
	voteAddrs := make([]BLSPublicKey, n)
	for i := range n {
		v := raw[i*validatorBytesLength : (i+1)*validatorBytesLength]
		addrs[i] = common.BytesToAddress(v[:length.Addr])
		copy(voteAddrs[i][:], v[length.Addr:])
	}
	return addrs, voteAddrs, nil
}

func ParseTurnLength(h *types.Header, epochLength uint64) (uint8, error) {
	end, err := epochValidatorsEnd(h, epochLength)
	if err != nil {
		return 0, err
	}
	turnLength := h.Extra[end]
	if turnLength == 0 {
		return 0, ErrInvalidTurnLength
	}
	return turnLength, nil
}

// ParseVoteAttestation returns nil when the header carries no attestation.
func ParseVoteAttestation(h *types.Header, epochLength uint64) (*VoteAttestation, error) {
	if len(h.Extra) <= extraVanity+extraSeal {
		return nil, nil
	}
	start := extraVanity
	if h.Number.Uint64()%epochLength == 0 {
		end, err := epochValidatorsEnd(h, epochLength)
		if err != nil {
			return nil, err
		}
		start = end + turnLengthSize
	}
	end := len(h.Extra) - extraSeal
	if end <= start {
		return nil, nil
	}
	var att VoteAttestation
	if err := rlp.DecodeBytes(h.Extra[start:end], &att); err != nil {
		return nil, fmt.Errorf("%w: block %d: %w", ErrInvalidAttestation, h.Number.Uint64(), err)
	}
	if att.Data == nil {
		return nil, fmt.Errorf("%w: block %d: no vote data", ErrInvalidAttestation, h.Number.Uint64())
	}
	return &att, nil
}
