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
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/erigontech/erigon/bsc/parlia/seal"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

const (
	epochLength      = 1000
	checkpointOffset = 200
)

var (
	ErrNotFermi              = errors.New("parlia: header verification supports Fermi and later only")
	ErrNotCheckpoint         = errors.New("parlia: not a checkpoint header")
	ErrOutOfRangeChain       = errors.New("parlia: out of range or non-contiguous chain")
	ErrUnknownAncestor       = errors.New("parlia: unknown ancestor")
	ErrUnauthorizedValidator = errors.New("parlia: unauthorized validator")
	ErrRecentlySigned        = errors.New("parlia: recently signed")
)

type HeaderReader interface {
	GetHeader(hash common.Hash, number uint64) *types.Header
}

type ValidatorInfo struct {
	Index       int
	VoteAddress BLSPublicKey
}

type Snapshot struct {
	Number      uint64
	Hash        common.Hash
	TurnLength  uint8
	Validators  map[common.Address]*ValidatorInfo
	Recents     map[uint64]common.Address
	Attestation *VoteData
}

// NewCheckpointSnapshot trusts the checkpoint header and takes the validator set from the
// epoch header checkpointOffset blocks before it. Recents start empty, which only makes the
// recently-signed check more lenient until a full history has been applied.
func NewCheckpointSnapshot(config *chain.Config, checkpoint, epochHeader *types.Header) (*Snapshot, error) {
	number := checkpoint.Number.Uint64()
	if !config.IsFermi(number, checkpoint.Time) {
		return nil, ErrNotFermi
	}
	if number%epochLength != checkpointOffset || epochHeader.Number.Uint64()+checkpointOffset != number {
		return nil, fmt.Errorf("%w: %d with epoch header %d", ErrNotCheckpoint, number, epochHeader.Number.Uint64())
	}
	validators, voteAddrs, err := ParseValidators(epochHeader, epochLength)
	if err != nil {
		return nil, err
	}
	turnLength, err := ParseTurnLength(epochHeader, epochLength)
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		Number:     number,
		Hash:       checkpoint.Hash(),
		TurnLength: turnLength,
		Validators: newValidatorSet(validators, voteAddrs),
		Recents:    make(map[uint64]common.Address),
	}, nil
}

func newValidatorSet(validators []common.Address, voteAddrs []BLSPublicKey) map[common.Address]*ValidatorInfo {
	set := make(map[common.Address]*ValidatorInfo, len(validators))
	for i, v := range validators {
		set[v] = &ValidatorInfo{VoteAddress: voteAddrs[i]}
	}
	for i, v := range sortedValidators(set) {
		set[v].Index = i + 1
	}
	return set
}

func sortedValidators(set map[common.Address]*ValidatorInfo) []common.Address {
	return slices.SortedFunc(maps.Keys(set), func(a, b common.Address) int { return bytes.Compare(a[:], b[:]) })
}

func (s *Snapshot) copy() *Snapshot {
	cpy := &Snapshot{
		Number:     s.Number,
		Hash:       s.Hash,
		TurnLength: s.TurnLength,
		Validators: make(map[common.Address]*ValidatorInfo, len(s.Validators)),
		Recents:    maps.Clone(s.Recents),
	}
	for v, info := range s.Validators {
		infoCopy := *info
		cpy.Validators[v] = &infoCopy
	}
	if s.Attestation != nil {
		att := *s.Attestation
		cpy.Attestation = &att
	}
	return cpy
}

func (s *Snapshot) Justified() (uint64, common.Hash) {
	if s.Attestation == nil {
		return 0, common.Hash{}
	}
	return s.Attestation.TargetNumber, s.Attestation.TargetHash
}

func (s *Snapshot) Finalized() (uint64, common.Hash) {
	if s.Attestation == nil {
		return 0, common.Hash{}
	}
	return s.Attestation.SourceNumber, s.Attestation.SourceHash
}

func (s *Snapshot) minerHistoryCheckLen() uint64 {
	return (uint64(len(s.Validators))/2+1)*uint64(s.TurnLength) - 1
}

func (s *Snapshot) countRecents() map[common.Address]uint8 {
	var leftHistoryBound uint64
	if checkLen := s.minerHistoryCheckLen(); s.Number > checkLen {
		leftHistoryBound = s.Number - checkLen
	}
	counts := make(map[common.Address]uint8, len(s.Validators))
	for seen, recent := range s.Recents {
		if seen <= leftHistoryBound || recent == (common.Address{}) {
			continue
		}
		counts[recent]++
	}
	return counts
}

func (s *Snapshot) SignRecently(validator common.Address) bool {
	return s.countRecents()[validator] >= s.TurnLength
}

func (s *Snapshot) updateAttestation(h *types.Header) {
	att, _ := ParseVoteAttestation(h, epochLength)
	if att == nil {
		return
	}
	if s.Attestation != nil && att.Data.SourceNumber+1 != att.Data.TargetNumber {
		s.Attestation.TargetNumber = att.Data.TargetNumber
		s.Attestation.TargetHash = att.Data.TargetHash
	} else {
		s.Attestation = att.Data
	}
}

// Apply returns a new snapshot with the headers applied; the receiver is not modified.
func (s *Snapshot) Apply(config *chain.Config, headers []*types.Header, reader HeaderReader) (*Snapshot, error) {
	if len(headers) == 0 {
		return s, nil
	}
	if headers[0].Number.Uint64() != s.Number+1 || headers[0].ParentHash != s.Hash {
		return nil, ErrOutOfRangeChain
	}
	for i := 1; i < len(headers); i++ {
		if headers[i].Number.Uint64() != headers[i-1].Number.Uint64()+1 || headers[i].ParentHash != headers[i-1].Hash() {
			return nil, ErrOutOfRangeChain
		}
	}
	batch := make(map[common.Hash]*types.Header, len(headers))
	for _, h := range headers {
		batch[h.Hash()] = h
	}

	snap := s.copy()
	for _, h := range headers {
		number := h.Number.Uint64()
		if !config.IsFermi(number, h.Time) {
			return nil, ErrNotFermi
		}
		if limit := snap.minerHistoryCheckLen() + 1; number >= limit {
			delete(snap.Recents, number-limit)
		}
		validator, err := RecoverSigner(config, h)
		if err != nil {
			return nil, err
		}
		if _, ok := snap.Validators[validator]; !ok {
			return nil, fmt.Errorf("%w: %x at %d", ErrUnauthorizedValidator, validator, number)
		}
		if snap.SignRecently(validator) {
			return nil, fmt.Errorf("%w: %x at %d", ErrRecentlySigned, validator, number)
		}

		snap.updateAttestation(h)

		snap.Recents[number] = validator
		finalized, _ := snap.Finalized()
		for seen := range snap.Recents {
			if seen <= finalized {
				delete(snap.Recents, seen)
			}
		}

		if number%epochLength == snap.minerHistoryCheckLen() {
			if err := snap.switchValidatorSet(h, batch, reader); err != nil {
				return nil, err
			}
		}
		snap.Number, snap.Hash = number, h.Hash()
	}
	return snap, nil
}

func (s *Snapshot) switchValidatorSet(h *types.Header, batch map[common.Hash]*types.Header, reader HeaderReader) error {
	epochKey := math.MaxUint64 - h.Number.Uint64()/epochLength
	if _, ok := s.Recents[epochKey]; ok {
		return nil
	}
	epochHeader := h
	for range s.minerHistoryCheckLen() {
		parent, ok := batch[epochHeader.ParentHash]
		if !ok {
			parent = reader.GetHeader(epochHeader.ParentHash, epochHeader.Number.Uint64()-1)
		}
		if parent == nil {
			return fmt.Errorf("%w: %d", ErrUnknownAncestor, epochHeader.Number.Uint64()-1)
		}
		epochHeader = parent
	}
	turnLength, err := ParseTurnLength(epochHeader, epochLength)
	if err != nil {
		return err
	}
	validators, voteAddrs, err := ParseValidators(epochHeader, epochLength)
	if err != nil {
		return err
	}
	s.TurnLength = turnLength
	s.Validators = newValidatorSet(validators, voteAddrs)
	s.Recents = map[uint64]common.Address{epochKey: {}}
	return nil
}

func RecoverSigner(config *chain.Config, h *types.Header) (common.Address, error) {
	if len(h.Extra) < extraSeal {
		return common.Address{}, seal.ErrMissingSignature
	}
	sealHash, err := seal.Hash(h, config.ChainID)
	if err != nil {
		return common.Address{}, err
	}
	pubkey, err := crypto.Ecrecover(sealHash[:], h.Extra[len(h.Extra)-extraSeal:])
	if err != nil {
		return common.Address{}, err
	}
	var signer common.Address
	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])
	return signer, nil
}
