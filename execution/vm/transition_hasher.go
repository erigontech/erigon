// Copyright 2025 The Erigon Authors
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

package vm

import (
	"encoding/binary"
	"sync"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TransitionHasher computes a rolling keccak256 hash over the application-level
// operations that surround the EVM during transaction execution. It captures
// the spec-mandated state transition process: gas purchase, nonce increment,
// access list warm-up, value transfers, fee distribution, etc.
//
// The transition hash embeds the execution hash (from ExecHasher) as one of
// its records, creating a composition where the transition hash proves the
// full process and the exec hash proves the EVM interpreter loop.
//
// See docs/TRANSITION_FORMAT.md for the canonical record format specification.
type TransitionHasher struct {
	hasher keccak.KeccakState
	buf    [148]byte // scratch buffer (largest record is TX_CONTEXT at 146 + tag)
}

// Record tag bytes. Each tag identifies a record type and determines the
// exact byte layout that follows.
const (
	tagTxContext       = 0x01
	tagIntrinsicGas    = 0x02
	tagGasPurchase     = 0x03
	tagNonceIncrement  = 0x04
	tagAuthResult      = 0x05
	tagAccessWarm      = 0x06
	tagAccessSlot      = 0x07
	tagTransfer        = 0x10
	tagExecHash        = 0x11
	tagGasAccounting   = 0x20
	tagFeeDistribution = 0x21
)

var transitionHasherPool = sync.Pool{
	New: func() any {
		return &TransitionHasher{}
	},
}

// GetTransitionHasher returns a TransitionHasher from the pool.
func GetTransitionHasher() *TransitionHasher {
	return transitionHasherPool.Get().(*TransitionHasher)
}

// PutTransitionHasher returns a TransitionHasher to the pool.
func PutTransitionHasher(h *TransitionHasher) {
	transitionHasherPool.Put(h)
}

// Reset prepares the hasher for a new transaction.
func (h *TransitionHasher) Reset() {
	if h.hasher == nil {
		h.hasher = crypto.NewKeccakState()
	} else {
		h.hasher.Reset()
	}
}

// HashTxContext records the transaction identity and fee parameters.
// Size: 1 + 32 + 1 + 8 + 8 + 32 + 32 + 32 = 146 bytes.
func (h *TransitionHasher) HashTxContext(txHash common.Hash, txType byte, nonce, gasLimit uint64, feeCap, tipCap, value *uint256.Int) {
	h.buf[0] = tagTxContext
	copy(h.buf[1:33], txHash[:])
	h.buf[33] = txType
	binary.BigEndian.PutUint64(h.buf[34:42], nonce)
	binary.BigEndian.PutUint64(h.buf[42:50], gasLimit)
	feeCap.WriteToSlice(h.buf[50:82])
	tipCap.WriteToSlice(h.buf[82:114])
	value.WriteToSlice(h.buf[114:146])
	h.hasher.Write(h.buf[:146])
}

// HashIntrinsicGas records the intrinsic gas calculation result.
// Size: 1 + 8 + 8 = 17 bytes.
func (h *TransitionHasher) HashIntrinsicGas(regularGas, floorGas uint64) {
	h.buf[0] = tagIntrinsicGas
	binary.BigEndian.PutUint64(h.buf[1:9], regularGas)
	binary.BigEndian.PutUint64(h.buf[9:17], floorGas)
	h.hasher.Write(h.buf[:17])
}

// HashGasPurchase records the gas purchase — balance deducted from sender.
// Size: 1 + 32 + 32 = 65 bytes.
func (h *TransitionHasher) HashGasPurchase(gasValue, blobGasValue *uint256.Int) {
	h.buf[0] = tagGasPurchase
	gasValue.WriteToSlice(h.buf[1:33])
	blobGasValue.WriteToSlice(h.buf[33:65])
	h.hasher.Write(h.buf[:65])
}

// HashNonceIncrement records the sender nonce increment.
// Size: 1 + 8 = 9 bytes.
func (h *TransitionHasher) HashNonceIncrement(newNonce uint64) {
	h.buf[0] = tagNonceIncrement
	binary.BigEndian.PutUint64(h.buf[1:9], newNonce)
	h.hasher.Write(h.buf[:9])
}

// HashAuthResult records the result of processing one EIP-7702 authorization.
// Size: 1 + 20 + 8 + 20 + 1 = 50 bytes.
func (h *TransitionHasher) HashAuthResult(authority accounts.Address, nonce uint64, delegation accounts.Address, accepted bool) {
	h.buf[0] = tagAuthResult
	authVal := authority.Value()
	copy(h.buf[1:21], authVal[:])
	binary.BigEndian.PutUint64(h.buf[21:29], nonce)
	delegVal := delegation.Value()
	copy(h.buf[29:49], delegVal[:])
	if accepted {
		h.buf[49] = 0x01
	} else {
		h.buf[49] = 0x00
	}
	h.hasher.Write(h.buf[:50])
}

// HashAccessWarm records an address added to the access list.
// Size: 1 + 20 = 21 bytes.
// Addresses MUST be emitted in sorted (lexicographic) order.
func (h *TransitionHasher) HashAccessWarm(addr accounts.Address) {
	h.buf[0] = tagAccessWarm
	addrVal := addr.Value()
	copy(h.buf[1:21], addrVal[:])
	h.hasher.Write(h.buf[:21])
}

// HashAccessSlot records a storage slot added to the access list.
// Size: 1 + 20 + 32 = 53 bytes.
// Slots MUST be emitted sorted by (address, slot), both lexicographic.
func (h *TransitionHasher) HashAccessSlot(addr accounts.Address, slot common.Hash) {
	h.buf[0] = tagAccessSlot
	addrVal := addr.Value()
	copy(h.buf[1:21], addrVal[:])
	copy(h.buf[21:53], slot[:])
	h.hasher.Write(h.buf[:53])
}

// HashTransfer records a value transfer (balance movement).
// Size: 1 + 2 + 20 + 20 + 32 = 75 bytes.
// Only emitted when value > 0.
func (h *TransitionHasher) HashTransfer(depth int, from, to accounts.Address, value *uint256.Int) {
	h.buf[0] = tagTransfer
	binary.BigEndian.PutUint16(h.buf[1:3], uint16(depth))
	fromVal := from.Value()
	copy(h.buf[3:23], fromVal[:])
	toVal := to.Value()
	copy(h.buf[23:43], toVal[:])
	value.WriteToSlice(h.buf[43:75])
	h.hasher.Write(h.buf[:75])
}

// HashExecHash embeds the execution hash from ExecHasher.Finalize().
// Size: 1 + 32 + 8 = 41 bytes.
func (h *TransitionHasher) HashExecHash(execHash [32]byte, ops uint64) {
	h.buf[0] = tagExecHash
	copy(h.buf[1:33], execHash[:])
	binary.BigEndian.PutUint64(h.buf[33:41], ops)
	h.hasher.Write(h.buf[:41])
}

// HashGasAccounting records the post-execution gas accounting.
// Size: 1 + 8 + 8 + 8 + 8 + 8 = 41 bytes.
func (h *TransitionHasher) HashGasAccounting(gasUsed, stateRefund, effectiveRefund, remainingGas, blockGasUsed uint64) {
	h.buf[0] = tagGasAccounting
	binary.BigEndian.PutUint64(h.buf[1:9], gasUsed)
	binary.BigEndian.PutUint64(h.buf[9:17], stateRefund)
	binary.BigEndian.PutUint64(h.buf[17:25], effectiveRefund)
	binary.BigEndian.PutUint64(h.buf[25:33], remainingGas)
	binary.BigEndian.PutUint64(h.buf[33:41], blockGasUsed)
	h.hasher.Write(h.buf[:41])
}

// HashFeeDistribution records how fees are distributed after execution.
// Size: 1 + 20 + 32 + 20 + 32 + 32 = 137 bytes.
func (h *TransitionHasher) HashFeeDistribution(coinbase accounts.Address, tipAmount *uint256.Int, burntContract accounts.Address, burnAmount, gasReturnValue *uint256.Int) {
	h.buf[0] = tagFeeDistribution
	coinbaseVal := coinbase.Value()
	copy(h.buf[1:21], coinbaseVal[:])
	tipAmount.WriteToSlice(h.buf[21:53])
	burntVal := burntContract.Value()
	copy(h.buf[53:73], burntVal[:])
	burnAmount.WriteToSlice(h.buf[73:105])
	gasReturnValue.WriteToSlice(h.buf[105:137])
	h.hasher.Write(h.buf[:137])
}

// Finalize returns the transition hash.
func (h *TransitionHasher) Finalize() [32]byte {
	var hash [32]byte
	h.hasher.Read(hash[:])
	return hash
}
