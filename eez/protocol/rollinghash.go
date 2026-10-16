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
package protocol

import (
	"encoding/binary"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

// Rolling-hash tags of EEZBase.
const (
	tagCallBegin           byte = 1
	tagCallEnd             byte = 2
	tagNestedBegin         byte = 3
	tagNestedEnd           byte = 4
	tagCallNotFound        byte = 5
	tagCallInsufficientGas byte = 6
)

// RollingHash accumulates the execution trace EEZ and EEZL2 fold while an
// entry runs; the result must equal the entry's rollingHash. Start from
// L1EntrySeed or L2EntrySeed. Every fold is keccak256 over abi.encodePacked.
type RollingHash struct {
	sum common.Hash
}

// L1EntrySeed starts an L1 entry (_rollingHashEntryBegin): roots folds
// keccak256(roots ‖ rollupId ‖ currentRoot) over the rollup updates from zero,
// then the seed is keccak256(roots ‖ proxyEntryHash).
func L1EntrySeed(updates []RollupUpdate, proxyEntryHash common.Hash) RollingHash {
	var roots common.Hash
	var rollupID [8]byte
	for i := range updates {
		binary.BigEndian.PutUint64(rollupID[:], updates[i].RollupID)
		roots = keccak(roots[:], rollupID[:], updates[i].CurrentRoot[:])
	}
	return RollingHash{keccak(roots[:], proxyEntryHash[:])}
}

// L2EntrySeed starts an EEZL2 entry (_seedRollingHash):
// keccak256(bytes32(0) ‖ proxyEntryHash).
func L2EntrySeed(proxyEntryHash common.Hash) RollingHash {
	var zero common.Hash
	return RollingHash{keccak(zero[:], proxyEntryHash[:])}
}

// Sum returns the accumulated hash.
func (r *RollingHash) Sum() common.Hash { return r.sum }

// CallBegin folds the start of a call identified by its call hash.
func (r *RollingHash) CallBegin(callHash common.Hash) {
	r.sum = keccak(r.sum[:], []byte{tagCallBegin}, callHash[:])
}

// CallEnd folds a call's outcome and raw return or revert data.
func (r *RollingHash) CallEnd(success bool, returnData []byte) {
	r.sum = keccak(r.sum[:], []byte{tagCallEnd, boolByte(success)}, returnData)
}

// NestedBegin folds the start of a reentrant frame for the given call.
func (r *RollingHash) NestedBegin(callHash common.Hash) {
	r.sum = keccak(r.sum[:], []byte{tagNestedBegin}, callHash[:])
}

// NestedEnd folds the end of a reentrant frame.
func (r *RollingHash) NestedEnd() {
	r.sum = keccak(r.sum[:], []byte{tagNestedEnd})
}

// CallNotFound folds a reentrant call without a matching expected call.
func (r *RollingHash) CallNotFound(callHash common.Hash) {
	r.sum = keccak(r.sum[:], []byte{tagCallNotFound}, callHash[:])
}

// CallInsufficientGas folds a call that stopped its call array for lack of gas.
func (r *RollingHash) CallInsufficientGas() {
	r.sum = keccak(r.sum[:], []byte{tagCallInsufficientGas})
}

// StaticResult folds a static sub-call result into prev without a tag
// (_rollingHashStaticResult): keccak256(prev ‖ success ‖ returnData).
func StaticResult(prev common.Hash, success bool, returnData []byte) common.Hash {
	return keccak(prev[:], []byte{boolByte(success)}, returnData)
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func keccak(parts ...[]byte) common.Hash {
	return common.BytesToHash(crypto.Keccak256(parts...))
}
