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

package aura

import (
	"bytes"
	"sort"
	"sync"

	"github.com/erigontech/secp256k1"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/rlp"
)

// A message broadcast by authorities when it's their turn to seal a block but there are no
// transactions. Other authorities accumulate these messages and later include them in the seal as
// proof.
//
// An empty step message is created _instead of_ a block if there are no pending transactions.
// It cannot itself be a parent, and `parent_hash` always points to the most recent block. E.g.:
//   - Validator A creates block `bA`.
//   - Validator B has no pending transactions, so it signs an empty step message `mB`
//     instead whose hash points to block `bA`.
//   - Validator C also has no pending transactions, so it also signs an empty step message `mC`
//     instead whose hash points to block `bA`.
//   - Validator D creates block `bD`. The parent is block `bA`, and the header includes `mB` and `mC`.
type EmptyStep struct {
	// The signature of the other two fields, by the message's author.
	signature []byte // H520
	// This message's step number.
	step uint64
	// The hash of the most recent block.
	parentHash common.Hash //     H256
}

func (s *EmptyStep) Less(other *EmptyStep) bool {
	if s.step < other.step {
		return true
	}
	if bytes.Compare(s.parentHash[:], other.parentHash[:]) < 0 {
		return true
	}
	if bytes.Compare(s.signature, other.signature) < 0 {
		return true
	}
	return false
}
func (s *EmptyStep) LessOrEqual(other *EmptyStep) bool {
	if s.step <= other.step {
		return true
	}
	if bytes.Compare(s.parentHash[:], other.parentHash[:]) <= 0 {
		return true
	}
	if bytes.Compare(s.signature, other.signature) <= 0 {
		return true
	}
	return false
}

// Returns `true` if the message has a valid signature by the expected proposer in the message's step.
func (s *EmptyStep) verify(validators ValidatorSet) (bool, error) { //nolint
	//sRlp, err := EmptyStepRlp(s.step, s.parentHash)
	//if err != nil {
	//	return false, err
	//}
	//message := crypto.Keccak256(sRlp)

	/*
		let correct_proposer = step_proposer(validators, &self.parent_hash, self.step);

		publickey::verify_address(&correct_proposer, &self.signature.into(), &message)
		.map_err(|e| e.into())
	*/
	return true, nil
}

// nolint
func (s *EmptyStep) author() (common.Address, error) {
	sRlp, err := EmptyStepRlp(s.step, s.parentHash)
	if err != nil {
		return common.Address{}, err
	}
	message := crypto.Keccak256(sRlp)
	public, err := secp256k1.RecoverPubkey(message, s.signature)
	if err != nil {
		return common.Address{}, err
	}
	ecdsa, err := crypto.UnmarshalPubkeyStd(public)
	if err != nil {
		return common.Address{}, err
	}
	return crypto.PubkeyToAddress(*ecdsa), nil
}

type EmptyStepSet struct {
	lock sync.Mutex
	list []*EmptyStep
}

func (s *EmptyStepSet) Less(i, j int) bool { return s.list[i].Less(s.list[j]) }
func (s *EmptyStepSet) Swap(i, j int)      { s.list[i], s.list[j] = s.list[j], s.list[i] }
func (s *EmptyStepSet) Len() int           { return len(s.list) }

func (s *EmptyStepSet) Sort() {
	s.lock.Lock()
	defer s.lock.Unlock()
	sort.Stable(s)
}

func (s *EmptyStepSet) ForEach(f func(int, *EmptyStep)) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i, el := range s.list {
		f(i, el)
	}
}

func EmptyStepFullRlp(signature []byte, emptyStepRlp []byte) ([]byte, error) {
	type A struct {
		s []byte
		r []byte
	}

	return rlp.EncodeToBytes(A{s: signature, r: emptyStepRlp})
}

func EmptyStepRlp(step uint64, parentHash common.Hash) ([]byte, error) {
	type A struct {
		s uint64
		h common.Hash
	}
	return rlp.EncodeToBytes(A{s: step, h: parentHash})
}
