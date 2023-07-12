// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package aura

import (
	"container/list"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

// RollingFinality checker for authority round consensus.
// Stores a chain of unfinalized hashes that can be pushed onto.
// nolint
type RollingFinality struct {
	headers    unAssembledHeaders //nolint
	signers    *SimpleList
	signCount  map[libcommon.Address]uint
	lastPushed *libcommon.Hash // Option<H256>,
}

// NewRollingFinality creates a blank finality checker under the given validator set.
func NewRollingFinality(signers []libcommon.Address) *RollingFinality {
	return &RollingFinality{
		signers:   NewSimpleList(signers),
		headers:   unAssembledHeaders{l: list.New()},
		signCount: map[libcommon.Address]uint{},
	}
}

// Clears the finality status, but keeps the validator set.
func (f *RollingFinality) print(num uint64) {
	if num > DEBUG_LOG_FROM {
		h := f.headers
		fmt.Printf("finality_heads: %d\n", num)
		i := 0
		for e := h.l.Front(); e != nil; e = e.Next() {
			i++
			a := e.Value.(*unAssembledHeader)
			fmt.Printf("\t%d,%x\n", a.number, a.signers[0])
		}
		if i == 0 {
			fmt.Printf("\tempty\n")
		}
	}
}

func (f *RollingFinality) clear() {
	f.headers = unAssembledHeaders{l: list.New()}
	f.signCount = map[libcommon.Address]uint{}
	f.lastPushed = nil
}

// Push a hash onto the rolling finality checker (implying `subchain_head` == head.parent)
//
// Fails if `signer` isn't a member of the active validator set.
// Returns a list of all newly finalized headers.
func (f *RollingFinality) push(head libcommon.Hash, num uint64, signers []libcommon.Address) (newlyFinalized []unAssembledHeader, err error) {
	for i := range signers {
		if !f.hasSigner(signers[i]) {
			return nil, fmt.Errorf("unknown validator")
		}
	}

	f.addSigners(signers)
	f.headers.PushBack(&unAssembledHeader{hash: head, number: num, signers: signers})

	for f.isFinalized() {
		e := f.headers.Pop()
		if e == nil {
			panic("headers length always greater than sign count length")
		}
		f.removeSigners(e.signers)
		newlyFinalized = append(newlyFinalized, *e)
	}
	f.lastPushed = &head
	return newlyFinalized, nil
}

// isFinalized returns whether the first entry in `self.headers` is finalized.
func (f *RollingFinality) isFinalized() bool {
	e := f.headers.Front()
	if e == nil {
		return false
	}
	return len(f.signCount)*2 > len(f.signers.validators)
}
func (f *RollingFinality) hasSigner(signer libcommon.Address) bool {
	for j := range f.signers.validators {
		if f.signers.validators[j] == signer {
			return true

		}
	}
	return false
}
func (f *RollingFinality) addSigners(signers []libcommon.Address) bool {
	for i := range signers {
		count, ok := f.signCount[signers[i]]
		if ok {
			f.signCount[signers[i]] = count + 1
		} else {
			f.signCount[signers[i]] = 1
		}
	}
	return false
}
func (f *RollingFinality) removeSigners(signers []libcommon.Address) {
	for i := range signers {
		count, ok := f.signCount[signers[i]]
		if !ok {
			panic("all hashes in `header` should have entries in `sign_count` for their signers")
			//continue
		}
		if count <= 1 {
			delete(f.signCount, signers[i])
		} else {
			f.signCount[signers[i]] = count - 1
		}
	}
}
func (f *RollingFinality) buildAncestrySubChain(get func(hash libcommon.Hash) ([]libcommon.Address, libcommon.Hash, libcommon.Hash, uint64, bool), parentHash, epochTransitionHash libcommon.Hash) error { // starts from chainHeadParentHash
	f.clear()

	for {
		signers, blockHash, newParentHash, blockNum, ok := get(parentHash)
		if !ok {
			return nil
		}
		if blockHash == epochTransitionHash {
			return nil
		}
		for i := range signers {
			if !f.hasSigner(signers[i]) {
				return fmt.Errorf("unknown validator: blockNum=%d", blockNum)
			}
		}
		if f.lastPushed == nil {
			copyHash := parentHash
			f.lastPushed = &copyHash
		}
		f.addSigners(signers)
		f.headers.PushFront(&unAssembledHeader{hash: blockHash, number: blockNum, signers: signers})
		// break when we've got our first finalized block.
		if f.isFinalized() {
			e := f.headers.Pop()
			if e == nil {
				panic("we just pushed a block")
			}
			f.removeSigners(e.signers)
			//log.Info("[aura] finality encountered already finalized block", "hash", e.hash.String(), "number", e.number)
			break
		}

		parentHash = newParentHash
	}
	return nil
}
