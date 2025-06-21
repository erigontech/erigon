// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/holiman/uint256"
)

type entryType int

const (
	createObjectEntry entryType = iota
	addLogEntry
	touchEntry
	balanceChangeEntry
	balanceIncreaseEntry
	balanceIncreaseTransferEntry
	nonceChangeEntry
	resetObjectEntry
	selfdestructEntry
	codeChangeEntry
	storageChangeEntry
	fakeStorageChangeEntry
	transientStorageChangeEntry
	refundEntry
	accessListAddAccountEntry
	accessListAddSlotEntry
)

// journal contains the list of state modifications applied since the last state
// commit. These are tracked to be able to be reverted in case of an execution
// exception or revertal request.
type journal struct {
	entries []entry                // Current changes tracked by the journal
	dirties map[common.Address]int // Dirty accounts and the number of changes
}

// newJournal create a new initialized journal.
func newJournal() *journal {
	return &journal{
		entries: make([]entry, 0, 1024),
		dirties: map[common.Address]int{},
	}
}
func (j *journal) Reset() {
	j.entries = j.entries[:0]
	clear(j.dirties)
}

// append inserts a new modification entry to the end of the change journal.
func (j *journal) append(entry entry) {
	j.entries = append(j.entries, entry)
	if entry.ops.dirtied() {
		j.dirties[entry.account]++
	}
}

// revert undoes a batch of journalled modifications along with any reverted
// dirty handling too.
func (j *journal) revert(statedb *IntraBlockState, snapshot int) {
	for i := len(j.entries) - 1; i >= snapshot; i-- {
		entry := j.entries[i]
		// Undo the changes made by the operation
		entry.ops.revert(entry, statedb)

		// Drop any dirty tracking induced by the change
		if entry.ops.dirtied() {
			addr := entry.account
			if j.dirties[addr]--; j.dirties[addr] == 0 {
				delete(j.dirties, addr)
			}
		}
	}
	j.entries = j.entries[:snapshot]
}

// dirty explicitly sets an address to dirty, even if the change entries would
// otherwise suggest it as clean. This method is an ugly hack to handle the RIPEMD
// precompile consensus exception.
func (j *journal) dirty(addr common.Address) {
	j.dirties[addr]++
}

// length returns the current number of entries in the journal.
func (j *journal) length() int {
	return len(j.entries)
}

type (
	ops struct {
		// revert undoes the changes introduced by this journal entry.
		revert func(c entry, s *IntraBlockState) error
		// dirtied returns the Ethereum address modified by this journal entry.
		dirtied   func() bool
		entryType func() entryType
	}

	// journalEntry is a modification entry in the state change journal that can be
	// reverted on demand.
	entry struct {
		account common.Address
		hashVal common.Hash
		objVal  any
		u256Val uint256.Int
		u64Val  uint64
		ops     *ops
	}
)

func (c *entry) entryType() entryType {
	return c.ops.entryType()
}

var createObjectChangeOps = ops{
	revert: func(c entry, i *IntraBlockState) error {
		delete(i.stateObjects, c.account)
		delete(i.stateObjectsDirty, c.account)
		return nil
	},
	dirtied: func() bool {
		return true
	},
	entryType: func() entryType {
		return createObjectEntry
	},
}

// Changes to the account trie.
func createObjectChange(a common.Address) entry {
	return entry{
		account: a,
		ops:     &createObjectChangeOps,
	}
}

func resetObjectChange(account common.Address, prev *stateObject) entry {
	return entry{
		account: account,
		objVal:  prev,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				i.setStateObject(c.account, c.objVal.(*stateObject))
				return nil
			},
			dirtied: func() bool {
				return false
			},
			entryType: func() entryType {
				return resetObjectEntry
			},
		},
	}
}

func selfdestructChange(account common.Address, prev bool, prevbalance uint256.Int) entry {
	// whether account had already selfdestructed
	var u64Val uint64
	if prev {
		u64Val = 1
	}
	return entry{
		account: account,
		u256Val: prevbalance,
		u64Val:  u64Val,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				obj, err := i.getStateObject(c.account)
				if err != nil {
					return err
				}
				if obj != nil {
					obj.selfdestructed = c.u64Val > 0
					obj.setBalance(c.u256Val)
				}
				if i.versionMap != nil {
					if obj.original.Balance == c.u256Val {
						i.versionedWrites.Delete(c.account, AccountKey{Path: BalancePath})
					} else {
						if v, ok := i.versionedWrites[c.account][AccountKey{Path: BalancePath}]; ok {
							v.Val = c.u64Val > 0
						}
					}
					i.versionedWrites.Delete(c.account, AccountKey{Path: SelfDestructPath})
				}

				return nil
			},
			dirtied: func() bool {
				return true
			},
			entryType: func() entryType {
				return selfdestructEntry
			},
		},
	}
}

var ripemd = common.HexToAddress("0000000000000000000000000000000000000003")

var touchChangeOps = ops{
	revert: func(c entry, i *IntraBlockState) error {
		return nil
	},
	dirtied: func() bool {
		return true
	},
	entryType: func() entryType {
		return touchEntry
	},
}

func touchChange(a common.Address) entry {
	return entry{
		account: a,
		ops:     &touchChangeOps,
	}
}

// Changes to individual accounts.
func balanceChange(a common.Address, prev uint256.Int) entry {
	return entry{
		account: a,
		u256Val: prev,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				obj, err := i.getStateObject(c.account)
				if err != nil {
					return err
				}
				if traceAccount(c.account) {
					fmt.Printf("Revert Balance %x: %d, prev: %d, orig: %d\n", c.account, obj.data.Balance, &c.u256Val, obj.original.Balance)
				}
				obj.setBalance(c.u256Val)
				if i.versionMap != nil {
					if obj.original.Balance == c.u256Val {
						i.versionedWrites.Delete(c.account, AccountKey{Path: BalancePath})
						i.versionMap.Delete(c.account, BalancePath, common.Hash{}, i.txIndex, false)
					} else {
						if v, ok := i.versionedWrites[c.account][AccountKey{Path: BalancePath}]; ok {
							v.Val = c.u256Val
						}
					}
				}

				return nil
			},
			dirtied: func() bool {
				return true
			},
			entryType: func() entryType {
				return balanceChangeEntry
			},
		},
	}
}

func balanceIncrease(account common.Address, increase uint256.Int) entry {
	return entry{
		account: account,
		u256Val: increase,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				if bi, ok := i.balanceInc[c.account]; ok {
					bi.increase.Sub(&bi.increase, &c.u256Val)
					bi.count--
					if bi.count == 0 {
						delete(i.balanceInc, c.account)
					}
				}
				return nil
			},
			dirtied: func() bool {
				return true
			},
			entryType: func() entryType {
				return balanceIncreaseEntry
			},
		},
	}
}

func balanceIncreaseTransfer(bi *BalanceIncrease) entry {
	return entry{
		objVal: bi,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				c.objVal.(*BalanceIncrease).transferred = false
				return nil
			},
			dirtied: func() bool {
				return false
			},
			entryType: func() entryType {
				return balanceIncreaseTransferEntry
			},
		},
	}
}

func nonceChange(account common.Address, prev uint64) entry {
	return entry{
		account: account,
		u64Val:  prev,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				obj, err := i.getStateObject(c.account)
				if err != nil {
					return err
				}
				obj.setNonce(c.u64Val)
				if i.versionMap != nil {
					if obj.original.Nonce == c.u64Val {
						i.versionedWrites.Delete(c.account, AccountKey{Path: NoncePath})
					} else {
						if v, ok := i.versionedWrites[c.account][AccountKey{Path: NoncePath}]; ok {
							v.Val = c.u64Val
						}
					}
				}
				return nil
			},
			dirtied: func() bool {
				return true
			},
			entryType: func() entryType {
				return nonceChangeEntry
			},
		},
	}
}

func codeChange(account common.Address, prevcode []byte, prevhash common.Hash) entry {
	return entry{
		account: account,
		objVal:  prevcode,
		hashVal: prevhash,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				obj, err := i.getStateObject(c.account)
				if err != nil {
					return err
				}
				obj.setCode(c.hashVal, c.objVal.([]byte))
				if i.versionMap != nil {
					if obj.original.CodeHash == c.hashVal {
						i.versionedWrites.Delete(c.account, AccountKey{Path: CodePath})
						i.versionedWrites.Delete(c.account, AccountKey{Path: CodeHashPath})
					} else {
						if v, ok := i.versionedWrites[c.account][AccountKey{Path: CodePath}]; ok {
							v.Val = c.objVal
						}
						if v, ok := i.versionedWrites[c.account][AccountKey{Path: CodeHashPath}]; ok {
							v.Val = c.hashVal
						}
					}
				}
				return nil
			},
			dirtied: func() bool {
				return true
			},
			entryType: func() entryType {
				return codeChangeEntry
			},
		},
	}
}

func storageChange(account common.Address, key common.Hash, prevalue uint256.Int, wasCommited bool) entry {
	var u64Val uint64
	if wasCommited {
		u64Val = 1
	}
	return entry{
		account: account,
		hashVal: key,
		u256Val: prevalue,
		u64Val:  u64Val,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				obj, err := i.getStateObject(c.account)
				if err != nil {
					return err
				}

				if i.versionMap != nil {
					if c.u64Val > 0 {
						i.versionedWrites.Delete(c.account, AccountKey{Path: StatePath, Key: c.hashVal})
						i.versionMap.Delete(c.account, StatePath, c.hashVal, i.txIndex, false)
					} else {
						if v, ok := i.versionedWrites[c.account][AccountKey{Path: StatePath, Key: c.hashVal}]; ok {
							v.Val = c.u256Val
						}
					}
				}
				obj.setState(c.hashVal, c.u256Val)
				return nil
			},
			dirtied: func() bool {
				return true
			},
			entryType: func() entryType {
				return storageChangeEntry
			},
		},
	}
}

func fakeStorageChange(account common.Address, key common.Hash, prevalue uint256.Int) entry {
	return entry{
		account: account,
		hashVal: key,
		u256Val: prevalue,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				obj, err := i.getStateObject(c.account)
				if err != nil {
					return err
				}
				obj.fakeStorage[c.hashVal] = c.u256Val
				return nil
			},
			dirtied: func() bool {
				return true
			},
			entryType: func() entryType {
				return fakeStorageChangeEntry
			},
		},
	}
}

func transientStorageChange(account common.Address, key common.Hash, prevalue uint256.Int) entry {
	return entry{
		account: account,
		hashVal: key,
		u256Val: prevalue,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				i.setTransientState(c.account, c.hashVal, c.u256Val)
				return nil
			},
			dirtied: func() bool {
				return false
			},
			entryType: func() entryType {
				return transientStorageChangeEntry
			},
		},
	}
}

// Changes to other state values.
func refundChange(prev uint64) entry {
	return entry{
		u64Val: prev,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				i.refund = c.u64Val
				return nil
			},
			dirtied: func() bool {
				return false
			},
			entryType: func() entryType {
				return refundEntry
			},
		},
	}
}

func addLogChange(txIndex int) entry {
	return entry{
		u64Val: uint64(txIndex),
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				txIndex = int(c.u64Val)
				if txIndex >= len(i.logs) {
					panic(fmt.Sprintf("can't revert log index %v, max: %v", txIndex, len(i.logs)-1))
				}
				txnLogs := i.logs[txIndex]
				i.logs[txIndex] = txnLogs[:len(txnLogs)-1] // revert 1 log
				if len(i.logs[txIndex]) == 0 {
					i.logs = i.logs[:len(i.logs)-1] // revert txn
				}
				i.logSize--
				return nil
			},
			dirtied: func() bool {
				return false
			},
			entryType: func() entryType {
				return addLogEntry
			},
		},
	}
}

// Changes to the access list
func accessListAddAccountChange(address common.Address) entry {
	return entry{
		account: address,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				/*
					One important invariant here, is that whenever a (addr, slot) is added, if the
					addr is not already present, the add causes two journal entries:
					- one for the address,
					- one for the (address,slot)
					Therefore, when unrolling the change, we can always blindly delete the
					(addr) at this point, since no storage adds can remain when come upon
					a single (addr) change.
				*/
				i.accessList.DeleteAddress(c.account)
				return nil
			},
			dirtied: func() bool {
				return false
			},
			entryType: func() entryType {
				return accessListAddAccountEntry
			},
		},
	}
}

func accessListAddSlotChange(address common.Address, slot common.Hash) entry {
	return entry{
		account: address,
		hashVal: slot,
		ops: &ops{
			revert: func(c entry, i *IntraBlockState) error {
				i.accessList.DeleteSlot(c.account, c.hashVal)
				return nil
			},
			dirtied: func() bool {
				return false
			},
			entryType: func() entryType {
				return accessListAddSlotEntry
			},
		},
	}
}
