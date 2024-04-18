// Copyright 2021 The go-ethereum Authors
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

package state

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/math"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
)

// mode specifies how a tree location has been accessed
// for the byte value:
// * the first bit is set if the branch has been edited
// * the second bit is set if the branch has been read
type mode byte

const (
	AccessWitnessReadFlag  = mode(1)
	AccessWitnessWriteFlag = mode(2)
)

var zeroTreeIndex uint256.Int

// AccessWitness lists the locations of the state that are being accessed
// during the production of a block.
type AccessWitness struct {
	branches map[branchAccessKey]mode
	chunks   map[chunkAccessKey]mode

	pointCache *vtree.PointCache
}

func NewAccessWitness(pointCache *vtree.PointCache) *AccessWitness {
	return &AccessWitness{
		branches:   make(map[branchAccessKey]mode),
		chunks:     make(map[chunkAccessKey]mode),
		pointCache: pointCache,
	}
}

// Merge is used to merge the witness that got generated during the execution
// of a tx, with the accumulation of witnesses that were generated during the
// execution of all the txs preceding this one in a given block.
func (aw *AccessWitness) Merge(other *AccessWitness) {
	for k := range other.branches {
		aw.branches[k] |= other.branches[k]
	}
	for k, chunk := range other.chunks {
		aw.chunks[k] |= chunk
	}
}

// Key returns, predictably, the list of keys that were touched during the
// buildup of the access witness.
func (aw *AccessWitness) Keys() [][]byte {
	// TODO: consider if parallelizing this is worth it, probably depending on len(aw.chunks).
	keys := make([][]byte, 0, len(aw.chunks))
	for chunk := range aw.chunks {
		basePoint := aw.pointCache.Get(chunk.addr[:])
		key := vtree.GetTreeKeyWithEvaluatedAddress(basePoint, &chunk.treeIndex, chunk.leafKey)
		keys = append(keys, key)
	}
	return keys
}

func (aw *AccessWitness) Copy() *AccessWitness {
	naw := &AccessWitness{
		branches:   make(map[branchAccessKey]mode),
		chunks:     make(map[chunkAccessKey]mode),
		pointCache: aw.pointCache,
	}
	naw.Merge(aw)
	return naw
}

// AddAccount returns the gas to be charged for each of the currently cold
// member fields of an account.
func (aw *AccessWitness) AddAccount(addr []byte, isWrite bool) uint64 {
	var gas uint64
	for i := vtree.VersionLeafKey; i <= vtree.CodeSizeLeafKey; i++ {
		gas += aw.touchAddressAndChargeGas(addr, zeroTreeIndex, byte(i), isWrite)
	}
	return gas
}

// MessageCallGas returns the gas to be charged for each of the currently
// cold member fields of an account, that need to be touched when making a message
// call to that account.
func (aw *AccessWitness) MessageCallGas(destination []byte) uint64 {
	var gas uint64
	gas += aw.touchAddressAndChargeGas(destination, zeroTreeIndex, vtree.VersionLeafKey, false)
	gas += aw.touchAddressAndChargeGas(destination, zeroTreeIndex, vtree.CodeSizeLeafKey, false)
	return gas
}

// ValueTransferGas returns the gas to be charged for each of the currently
// cold balance member fields of the caller and the callee accounts.
func (aw *AccessWitness) ValueTransferGas(callerAddr, targetAddr []byte) uint64 {
	var gas uint64
	gas += aw.touchAddressAndChargeGas(callerAddr, zeroTreeIndex, vtree.BalanceLeafKey, true)
	gas += aw.touchAddressAndChargeGas(targetAddr, zeroTreeIndex, vtree.BalanceLeafKey, true)
	return gas
}

// ContractCreateInitGas returns the access gas costs for the initialization of
// a contract creation.
func (aw *AccessWitness) ContractCreateInitGas(addr []byte, createSendsValue bool) uint64 {
	var gas uint64
	gas += aw.touchAddressAndChargeGas(addr, zeroTreeIndex, vtree.VersionLeafKey, true)
	gas += aw.touchAddressAndChargeGas(addr, zeroTreeIndex, vtree.NonceLeafKey, true)
	if createSendsValue {
		gas += aw.touchAddressAndChargeGas(addr, zeroTreeIndex, vtree.BalanceLeafKey, true)
	}
	return gas
}

// AddTxOrigin adds the member fields of the sender account to the witness,
// so that cold accesses are not charged, since they are covered by the 21000 gas.
func (aw *AccessWitness) AddTxOrigin(originAddr []byte) {
	for i := vtree.VersionLeafKey; i <= vtree.CodeSizeLeafKey; i++ {
		aw.touchAddressAndChargeGas(originAddr, zeroTreeIndex, byte(i), i == vtree.BalanceLeafKey || i == vtree.NonceLeafKey)
	}
}

// AddTxDestination adds the member fields of the sender account to the witness,
// so that cold accesses are not charged, since they are covered by the 21000 gas.
func (aw *AccessWitness) AddTxDestination(targetAddr []byte, sendsValue bool) {
	for i := vtree.VersionLeafKey; i <= vtree.CodeSizeLeafKey; i++ {
		aw.touchAddressAndChargeGas(targetAddr, zeroTreeIndex, byte(i), i == vtree.VersionLeafKey && sendsValue)
	}
}

// SlotGas returns the amount of gas to be charged for a cold storage access.
func (aw *AccessWitness) SlotGas(addr []byte, slot common.Hash, isWrite bool) uint64 {
	treeIndex, subIndex := vtree.StorageIndex(slot.Bytes())
	return aw.touchAddressAndChargeGas(addr, *treeIndex, subIndex, isWrite)
}

// touchAddressAndChargeGas adds any missing access event to the witness, and returns the cold
// access cost to be charged, if need be.
func (aw *AccessWitness) touchAddressAndChargeGas(addr []byte, treeIndex uint256.Int, subIndex byte, isWrite bool) uint64 {
	stemRead, selectorRead, stemWrite, selectorWrite, selectorFill := aw.touchAddress(addr, treeIndex, subIndex, isWrite)

	var gas uint64
	if stemRead {
		gas += params.WitnessBranchReadCost
	}
	if selectorRead {
		gas += params.WitnessChunkReadCost
	}
	if stemWrite {
		gas += params.WitnessBranchWriteCost
	}
	if selectorWrite {
		gas += params.WitnessChunkWriteCost
	}
	if selectorFill {
		gas += params.WitnessChunkFillCost
	}

	return gas
}

// touchAddress adds any missing access event to the witness.
func (aw *AccessWitness) touchAddress(addr []byte, treeIndex uint256.Int, subIndex byte, isWrite bool) (bool, bool, bool, bool, bool) {
	branchKey := newBranchAccessKey(addr, treeIndex)
	chunkKey := newChunkAccessKey(branchKey, subIndex)

	// Read access.
	var branchRead, chunkRead bool
	if _, hasStem := aw.branches[branchKey]; !hasStem {
		branchRead = true
		aw.branches[branchKey] = AccessWitnessReadFlag
	}
	if _, hasSelector := aw.chunks[chunkKey]; !hasSelector {
		chunkRead = true
		aw.chunks[chunkKey] = AccessWitnessReadFlag
	}

	// Write access.
	var branchWrite, chunkWrite, chunkFill bool
	if isWrite {
		if (aw.branches[branchKey] & AccessWitnessWriteFlag) == 0 {
			branchWrite = true
			aw.branches[branchKey] |= AccessWitnessWriteFlag
		}

		chunkValue := aw.chunks[chunkKey]
		if (chunkValue & AccessWitnessWriteFlag) == 0 {
			chunkWrite = true
			aw.chunks[chunkKey] |= AccessWitnessWriteFlag
		}

		// TODO: charge chunk filling costs if the leaf was previously empty in the state
	}

	return branchRead, chunkRead, branchWrite, chunkWrite, chunkFill
}

type branchAccessKey struct {
	addr      common.Address
	treeIndex uint256.Int
}

func newBranchAccessKey(addr []byte, treeIndex uint256.Int) branchAccessKey {
	var sk branchAccessKey
	copy(sk.addr[20-len(addr):], addr)
	sk.treeIndex = treeIndex
	return sk
}

type chunkAccessKey struct {
	branchAccessKey
	leafKey byte
}

func newChunkAccessKey(branchKey branchAccessKey, leafKey byte) chunkAccessKey {
	var lk chunkAccessKey
	lk.branchAccessKey = branchKey
	lk.leafKey = leafKey
	return lk
}

// touchCodeChunksRangeOnReadAndChargeGas is a helper function to touch every chunk in a code range and charge witness gas costs
func (aw *AccessWitness) CodeChunksRangeGas(contractAddr []byte, startPC, size uint64, codeLen uint64, isWrite bool) uint64 {
	// note that in the case where the copied code is outside the range of the
	// contract code but touches the last leaf with contract code in it,
	// we don't include the last leaf of code in the AccessWitness.  The
	// reason that we do not need the last leaf is the account's code size
	// is already in the AccessWitness so a stateless verifier can see that
	// the code from the last leaf is not needed.
	if (codeLen == 0 && size == 0) || startPC > codeLen {
		return 0
	}

	endPC := startPC + size
	if endPC > codeLen {
		endPC = codeLen
	}
	if endPC > 0 {
		endPC -= 1 // endPC is the last bytecode that will be touched.
	}

	var statelessGasCharged uint64
	for chunkNumber := startPC / 31; chunkNumber <= endPC/31; chunkNumber++ {
		treeIndex := *uint256.NewInt((chunkNumber + 128) / 256)
		subIndex := byte((chunkNumber + 128) % 256)
		gas := aw.touchAddressAndChargeGas(contractAddr, treeIndex, subIndex, isWrite)
		var overflow bool
		statelessGasCharged, overflow = math.SafeAdd(statelessGasCharged, gas)
		if overflow {
			panic("overflow when adding gas")
		}
	}

	return statelessGasCharged
}

func (aw *AccessWitness) VersionGas(addr []byte, isWrite bool) uint64 {
	return aw.touchAddressAndChargeGas(addr, zeroTreeIndex, vtree.VersionLeafKey, isWrite)
}

func (aw *AccessWitness) BalanceGas(addr []byte, isWrite bool) uint64 {
	return aw.touchAddressAndChargeGas(addr, zeroTreeIndex, vtree.BalanceLeafKey, isWrite)
}

func (aw *AccessWitness) NonceGas(addr []byte, isWrite bool) uint64 {
	return aw.touchAddressAndChargeGas(addr, zeroTreeIndex, vtree.NonceLeafKey, isWrite)
}

func (aw *AccessWitness) CodeSizeGas(addr []byte, isWrite bool) uint64 {
	return aw.touchAddressAndChargeGas(addr, zeroTreeIndex, vtree.CodeSizeLeafKey, isWrite)
}

func (aw *AccessWitness) CodeHashGas(addr []byte, isWrite bool) uint64 {
	return aw.touchAddressAndChargeGas(addr, zeroTreeIndex, vtree.CodeKeccakLeafKey, isWrite)
}
