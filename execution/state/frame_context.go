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

package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// FrameContext shares transaction-wide approvals and gas accounting across frames
// and their nested calls. Approvals and state-gas charges follow the state journal:
// call, frame and atomic-batch rollbacks undo them, while execution gas remains charged.
type FrameContext struct {
	Transaction    *types.FrameTransaction
	Receipts       []types.FrameReceipt
	Index          int
	Payer          accounts.Address
	SenderApproved bool
	MaxCost        uint256.Int
	LegacyNonce    uint64 // Sender's account nonce before any frame executes, for TXPARAM.
	GasBailout     bool
	journal        *journal
	undo           journalExtra
	storageOwners  map[frameStorageSlot]uint8 // Charging frame index plus one; zero means no outstanding charge.
	journalStart   int
	logStart       int
	trace          *TransactionTrace

	// Transaction data is fixed during execution, so hash caches survive frame reverts.
	signingHash      common.Hash
	nonceKeysHash    common.Hash
	signingHashSet   bool
	nonceKeysHashSet bool
}

type frameStorageSlot struct {
	address accounts.Address
	key     accounts.StorageKey
}

func (ibs *IntraBlockState) NewFrameContext(tx *types.FrameTransaction, maxCost uint256.Int) *FrameContext {
	ctx := &FrameContext{Transaction: tx, Receipts: make([]types.FrameReceipt, len(tx.Frames)), MaxCost: maxCost, journal: ibs.journal}
	ctx.undo.frame = ctx
	ctx.storageOwners = make(map[frameStorageSlot]uint8)
	ctx.journalStart, ctx.logStart = len(ibs.journal.entries), ibs.LogCount()
	return ctx
}

func (ctx *FrameContext) SigningHash() common.Hash {
	if !ctx.signingHashSet {
		ctx.signingHash = ctx.Transaction.SigningHash(nil)
		ctx.signingHashSet = true
	}
	return ctx.signingHash
}

func (ctx *FrameContext) NonceKeysHash() common.Hash {
	if !ctx.nonceKeysHashSet {
		ctx.nonceKeysHash = ctx.Transaction.NonceKeysHash()
		ctx.nonceKeysHashSet = true
	}
	return ctx.nonceKeysHash
}

// ResetFrameTransientStorage isolates transient storage between transaction frames.
// The reset is journaled so an enclosing batch or transaction rollback can undo it.
func (ibs *IntraBlockState) ResetFrameTransientStorage() {
	if len(ibs.transientStorage) == 0 {
		return
	}
	ibs.journal.entries = append(ibs.journal.entries, journalEntry{kind: kindFrameTransient, extra: &journalExtra{prevTransient: ibs.transientStorage}})
	ibs.transientStorage = newTransientStorage()
}

func (ctx *FrameContext) StateGasLeft() uint64 {
	return ctx.Transaction.Frames[ctx.Index].Limits.State - ctx.Receipts[ctx.Index].GasUsed.State
}

func (ctx *FrameContext) ChargeStateGas(amount uint64) bool {
	if amount > ctx.StateGasLeft() {
		return false
	}
	used := &ctx.Receipts[ctx.Index].GasUsed.State
	ctx.journal.entries = append(ctx.journal.entries, journalEntry{kind: kindFrameStateGas, flags: uint8(ctx.Index), aux: *used, extra: &ctx.undo})
	*used += amount
	return true
}

func (ctx *FrameContext) SetApproval(senderApproved bool, payer accounts.Address) {
	var flags uint8
	if ctx.SenderApproved {
		flags = 1
	}
	ctx.journal.entries = append(ctx.journal.entries, journalEntry{kind: kindFrameApproval, account: ctx.Payer, flags: flags, extra: &ctx.undo})
	ctx.SenderApproved, ctx.Payer = senderApproved, payer
}

func (ctx *FrameContext) RecordStorageCharge(address accounts.Address, key accounts.StorageKey) {
	ctx.setStorageOwner(address, key, uint8(ctx.Index+1))
}

func (ctx *FrameContext) setStorageOwner(address accounts.Address, key accounts.StorageKey, owner uint8) {
	slot := frameStorageSlot{address, key}
	ctx.journal.entries = append(ctx.journal.entries, journalEntry{kind: kindFrameStorageOwner, account: address, key: key, flags: ctx.storageOwners[slot], extra: &ctx.undo})
	if owner == 0 {
		delete(ctx.storageOwners, slot)
	} else {
		ctx.storageOwners[slot] = owner
	}
}

func (ctx *FrameContext) RefillStorageGas(address accounts.Address, key accounts.StorageKey, amount uint64) {
	owner := ctx.storageOwners[frameStorageSlot{address, key}]
	if owner == 0 {
		panic("storage refill without frame owner")
	}
	// Return the charge to the frame that created the slot. A later frame may
	// clear it, but must not gain spendable state gas from an earlier frame.
	ctx.refillStateGas(int(owner)-1, amount)
	ctx.setStorageOwner(address, key, 0)
}

func (ctx *FrameContext) RefillStateGas(amount uint64) { ctx.refillStateGas(ctx.Index, amount) }

func (ctx *FrameContext) refillStateGas(index int, amount uint64) {
	used := &ctx.Receipts[index].GasUsed.State
	if *used < amount {
		panic("frame state gas refill exceeds charge")
	}
	ctx.journal.entries = append(ctx.journal.entries, journalEntry{kind: kindFrameStateGas, flags: uint8(index), aux: *used, extra: &ctx.undo})
	*used -= amount
}
