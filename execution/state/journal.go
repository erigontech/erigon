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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/holiman/uint256"
)

// journalEntry is a modification entry in the state change journal that can be
// reverted on demand.
type journalEntry interface {
	// revert undoes the changes introduced by this journal entry.
	revert(*IntraBlockState) error

	// dirtied returns the Ethereum address modified by this journal entry.
	dirtied() (common.Address, bool)
}

// journal contains the list of state modifications applied since the last state
// commit. These are tracked to be able to be reverted in case of an execution
// exception or revertal request.
type journal struct {
	entries []journalEntry         // Current changes tracked by the journal
	dirties map[common.Address]int // Dirty accounts and the number of changes
}

// newJournal create a new initialized journal.
func newJournal() *journal {
	return &journal{
		dirties: make(map[common.Address]int),
	}
}
func (j *journal) Reset() {
	j.entries = j.entries[:0]
	clear(j.dirties)
}

// append inserts a new modification entry to the end of the change journal.
func (j *journal) append(entry journalEntry) {
	j.entries = append(j.entries, entry)
	if addr, isditry := entry.dirtied(); isditry {
		j.dirties[addr]++
	}
}

// revert undoes a batch of journalled modifications along with any reverted
// dirty handling too.
func (j *journal) revert(statedb *IntraBlockState, snapshot int) {
	for i := len(j.entries) - 1; i >= snapshot; i-- {
		// Undo the changes made by the operation
		j.entries[i].revert(statedb)

		// Drop any dirty tracking induced by the change
		if addr, isdirty := j.entries[i].dirtied(); isdirty {
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
	// Changes to the account trie.
	createObjectChange struct {
		account common.Address
	}
	resetObjectChange struct {
		account common.Address
		prev    *stateObject
	}
	selfdestructChange struct {
		account     common.Address
		prev        bool // whether account had already selfdestructed
		prevbalance uint256.Int
		wasCommited bool
	}

	// Changes to individual accounts.
	balanceChange struct {
		account     common.Address
		prev        uint256.Int
		wasCommited bool
	}
	balanceIncrease struct {
		account  common.Address
		increase uint256.Int
	}
	balanceIncreaseTransfer struct {
		bi *BalanceIncrease
	}
	nonceChange struct {
		account     common.Address
		prev        uint64
		wasCommited bool
	}
	storageChange struct {
		account     common.Address
		key         common.Hash
		prevalue    uint256.Int
		wasCommited bool
	}
	fakeStorageChange struct {
		account  common.Address
		key      common.Hash
		prevalue uint256.Int
	}
	codeChange struct {
		account     common.Address
		prevcode    []byte
		prevhash    common.Hash
		wasCommited bool
	}

	// Changes to other state values.
	refundChange struct {
		prev uint64
	}
	addLogChange struct {
		txIndex int
	}
	touchChange struct {
		account common.Address
	}

	// Changes to the access list
	accessListAddAccountChange struct {
		address common.Address
	}
	accessListAddSlotChange struct {
		address common.Address
		slot    common.Hash
	}

	transientStorageChange struct {
		account  common.Address
		key      common.Hash
		prevalue uint256.Int
	}
)

//type journalEntry2 interface {
//	createObjectChange | resetObjectChange | selfdestructChange | balanceChange | balanceIncrease | balanceIncreaseTransfer |
//		nonceChange | storageChange | fakeStorageChange | codeChange |
//		refundChange | addLogChange | touchChange | accessListAddAccountChange | accessListAddSlotChange | transientStorageChange
//}

func (ch createObjectChange) revert(s *IntraBlockState) error {
	delete(s.stateObjects, ch.account)
	delete(s.stateObjectsDirty, ch.account)
	return nil
}

func (ch createObjectChange) dirtied() (common.Address, bool) {
	return ch.account, true
}

func (ch resetObjectChange) revert(s *IntraBlockState) error {
	s.setStateObject(ch.account, ch.prev)
	return nil
}

func (ch resetObjectChange) dirtied() (common.Address, bool) {
	return common.Address{}, false
}

func (ch selfdestructChange) revert(s *IntraBlockState) error {
	obj, err := s.getStateObject(ch.account)
	if err != nil {
		return err
	}
	if obj != nil {
		trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(ch.account))
		var tracePrefix string
		if trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
			fmt.Printf("%s Revert SelfDestruct %x: %v:%d, prev: %v:%d, commited: %v\n", tracePrefix, ch.account, ch.prev, &ch.prevbalance, obj.selfdestructed, &obj.data.Balance, ch.wasCommited)
		}
		obj.selfdestructed = ch.prev
		obj.setBalance(ch.prevbalance)

		if s.versionMap != nil {
			if ch.wasCommited {
				if trace {
					if v, ok := s.versionedWrites[ch.account][AccountKey{Path: SelfDestructPath}]; ok {
						fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, ch.account, v.Val, &ch.prev)
					}
					if v, ok := s.versionedWrites[ch.account][AccountKey{Path: BalancePath}]; ok {
						val := v.Val.(uint256.Int)
						fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, ch.account, &val, &ch.prevbalance)
					}
				}
				s.versionedWrites.Delete(ch.account, AccountKey{Path: BalancePath})
				s.versionedWrites.Delete(ch.account, AccountKey{Path: SelfDestructPath})
			} else {
				if v, ok := s.versionedWrites[ch.account][AccountKey{Path: SelfDestructPath}]; ok {
					fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, ch.account, v.Val, &ch.prev)
					v.Val = ch.prev
				}
				if v, ok := s.versionedWrites[ch.account][AccountKey{Path: BalancePath}]; ok {
					val := v.Val.(uint256.Int)
					fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, ch.account, &val, &ch.prevbalance)
					v.Val = ch.prevbalance
				}
			}
		}
	}

	return nil
}

func (ch selfdestructChange) dirtied() (common.Address, bool) {
	return ch.account, true
}

var ripemd = common.HexToAddress("0000000000000000000000000000000000000003")

func (ch touchChange) revert(s *IntraBlockState) error {
	return nil
}

func (ch touchChange) dirtied() (common.Address, bool) { return ch.account, true }

func (ch balanceChange) revert(s *IntraBlockState) error {
	obj, err := s.getStateObject(ch.account)
	if err != nil {
		return err
	}

	trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(ch.account))
	var tracePrefix string
	if trace {
		tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
		fmt.Printf("%s Revert Balance %x: %d, prev: %d, orig: %d, commited: %v\n", tracePrefix, ch.account, &obj.data.Balance, &ch.prev, &obj.original.Balance, ch.wasCommited)
	}
	obj.setBalance(ch.prev)
	if s.versionMap != nil {
		if ch.wasCommited {
			if trace {
				if v, ok := s.versionedWrites[ch.account][AccountKey{Path: BalancePath}]; ok {
					val := v.Val.(uint256.Int)
					fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, ch.account, &val, &ch.prev)
				}
			}
			s.versionedWrites.Delete(ch.account, AccountKey{Path: BalancePath})
		} else {
			if v, ok := s.versionedWrites[ch.account][AccountKey{Path: BalancePath}]; ok {
				if trace {
					val := v.Val.(uint256.Int)
					fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, ch.account, &val, &ch.prev)
				}
				v.Val = ch.prev
			}
		}
	}

	return nil
}

func (ch balanceChange) dirtied() (common.Address, bool) {
	return ch.account, true
}

func (ch balanceIncrease) revert(s *IntraBlockState) error {
	if bi, ok := s.balanceInc[ch.account]; ok {
		bi.increase.Sub(&bi.increase, &ch.increase)
		bi.count--
		if bi.count == 0 {
			delete(s.balanceInc, ch.account)
		}
	}
	return nil
}

func (ch balanceIncrease) dirtied() (common.Address, bool) {
	return ch.account, true
}

func (ch balanceIncreaseTransfer) dirtied() (common.Address, bool) {
	return common.Address{}, false
}

func (ch balanceIncreaseTransfer) revert(s *IntraBlockState) error {
	ch.bi.transferred = false
	return nil
}
func (ch nonceChange) revert(s *IntraBlockState) error {
	obj, err := s.getStateObject(ch.account)
	if err != nil {
		return err
	}

	trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(ch.account))
	var tracePrefix string
	if trace {
		tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
		fmt.Printf("%s Revert Nonce %x: %d, prev: %d, orig: %d, commited: %v\n", tracePrefix, ch.account, obj.data.Nonce, &ch.prev, obj.original.Nonce, ch.wasCommited)
	}
	obj.setNonce(ch.prev)
	if s.versionMap != nil {
		if ch.wasCommited {
			if trace {
				if v, ok := s.versionedWrites[ch.account][AccountKey{Path: NoncePath}]; ok {
					fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, ch.account, v.Val, ch.prev)
				}
			}
			s.versionedWrites.Delete(ch.account, AccountKey{Path: NoncePath})
		} else {
			if v, ok := s.versionedWrites[ch.account][AccountKey{Path: NoncePath}]; ok {
				if trace {
					fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, ch.account, v.Val, ch.prev)
				}
				v.Val = ch.prev
			}
		}
	}

	return nil
}

func (ch nonceChange) dirtied() (common.Address, bool) {
	return ch.account, true
}

func (ch codeChange) revert(s *IntraBlockState) error {
	obj, err := s.getStateObject(ch.account)
	if err != nil {
		return err
	}

	trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(ch.account))
	var tracePrefix string
	if trace {
		tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
		_, cs := printCode(obj.code)
		_, ps := printCode(ch.prevcode)
		fmt.Printf("%s Revert Code %x: %x:%s, prevHash: %x, origHash: %x, prevCode: %s, commited: %v\n", tracePrefix,
			ch.account, obj.data.CodeHash, cs, ch.prevhash, obj.original.CodeHash, ps, ch.wasCommited)
	}
	obj.setCode(ch.prevhash, ch.prevcode)
	if s.versionMap != nil {
		if ch.wasCommited {
			if trace {
				if v, ok := s.versionedWrites[ch.account][AccountKey{Path: CodeHashPath}]; ok {
					fmt.Printf("%s WRT Revert %x: %x -> %x\n", tracePrefix, ch.account, v.Val.(common.Hash), ch.prevhash)
				}
				if v, ok := s.versionedWrites[ch.account][AccountKey{Path: CodePath}]; ok {
					_, cs := printCode(v.Val.([]byte))
					_, ps := printCode(ch.prevcode)
					fmt.Printf("%s WRT Revert %x: %s -> %s\n", tracePrefix, ch.account, cs, ps)
				}
			}
			s.versionedWrites.Delete(ch.account, AccountKey{Path: CodeHashPath})
			s.versionedWrites.Delete(ch.account, AccountKey{Path: CodePath})
		} else {
			if v, ok := s.versionedWrites[ch.account][AccountKey{Path: CodePath}]; ok {
				if trace {
					_, cs := printCode(v.Val.([]byte))
					_, ps := printCode(ch.prevcode)
					fmt.Printf("%s WRT Revert %x: %s -> %s\n", tracePrefix, ch.account, cs, ps)
				}
				v.Val = ch.prevcode
			}
			if v, ok := s.versionedWrites[ch.account][AccountKey{Path: CodeHashPath}]; ok {
				if trace {
					fmt.Printf("%s WRT Revert %x: %x -> %x\n", tracePrefix, ch.account, v.Val, ch.prevhash)
				}
				v.Val = ch.prevhash
			}
		}
	}
	return nil
}

func (ch codeChange) dirtied() (common.Address, bool) {
	return ch.account, true
}

func (ch storageChange) revert(s *IntraBlockState) error {
	obj, err := s.getStateObject(ch.account)
	if err != nil {
		return err
	}

	trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(ch.account))
	var tracePrefix string
	var val uint256.Int
	if trace {
		tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
		var commited uint256.Int
		obj.GetState(ch.key, &val)
		obj.GetCommittedState(ch.key, &commited)
		fmt.Printf("%s Revert State %x %x: %d, prev: %d, orig: %d, commited: %v\n", tracePrefix, ch.account, ch.key, &val, &ch.prevalue, &commited, ch.wasCommited)
	}
	if s.versionMap != nil {
		if ch.wasCommited {
			if trace {
				if v, ok := s.versionedWrites[ch.account][AccountKey{Path: StatePath, Key: ch.key}]; ok {
					val := v.Val.(uint256.Int)
					fmt.Printf("%s WRT Revert %x: %x: %x -> %x\n", tracePrefix, ch.account, ch.key, &val, &ch.prevalue)
				}
			}
			s.versionedWrites.Delete(ch.account, AccountKey{Path: StatePath, Key: ch.key})
		} else {
			if v, ok := s.versionedWrites[ch.account][AccountKey{Path: StatePath, Key: ch.key}]; ok {
				if trace {
					val := v.Val.(uint256.Int)
					fmt.Printf("%s WRT Revert %x: %x: %d -> %d\n", tracePrefix, ch.account, ch.key, &val, &ch.prevalue)
				}
				v.Val = ch.prevalue
			}
		}
	}
	obj.setState(ch.key, ch.prevalue)
	return nil
}

func (ch storageChange) dirtied() (common.Address, bool) {
	return ch.account, true
}

func (ch fakeStorageChange) revert(s *IntraBlockState) error {
	obj, err := s.getStateObject(ch.account)
	if err != nil {
		return err
	}
	obj.fakeStorage[ch.key] = ch.prevalue
	return nil
}

func (ch fakeStorageChange) dirtied() (common.Address, bool) {
	return ch.account, true
}

func (ch transientStorageChange) revert(s *IntraBlockState) error {
	s.setTransientState(ch.account, ch.key, ch.prevalue)
	return nil
}

func (ch transientStorageChange) dirtied() (common.Address, bool) {
	return common.Address{}, false
}

func (ch refundChange) revert(s *IntraBlockState) error {
	s.refund = ch.prev
	return nil
}

func (ch refundChange) dirtied() (common.Address, bool) {
	return common.Address{}, false
}

func (ch addLogChange) revert(s *IntraBlockState) error {
	if ch.txIndex >= len(s.logs) {
		panic(fmt.Sprintf("can't revert log index %v, max: %v", ch.txIndex, len(s.logs)-1))
	}
	txnLogs := s.logs[ch.txIndex]
	s.logs[ch.txIndex] = txnLogs[:len(txnLogs)-1] // revert 1 log
	if len(s.logs[ch.txIndex]) == 0 {
		s.logs = s.logs[:len(s.logs)-1] // revert txn
	}
	s.logSize--
	return nil
}

func (ch addLogChange) dirtied() (common.Address, bool) {
	return common.Address{}, false
}

func (ch accessListAddAccountChange) revert(s *IntraBlockState) error {
	/*
		One important invariant here, is that whenever a (addr, slot) is added, if the
		addr is not already present, the add causes two journal entries:
		- one for the address,
		- one for the (address,slot)
		Therefore, when unrolling the change, we can always blindly delete the
		(addr) at this point, since no storage adds can remain when come upon
		a single (addr) change.
	*/
	s.accessList.DeleteAddress(ch.address)
	return nil
}

func (ch accessListAddAccountChange) dirtied() (common.Address, bool) {
	return common.Address{}, false
}

func (ch accessListAddSlotChange) revert(s *IntraBlockState) error {
	s.accessList.DeleteSlot(ch.address, ch.slot)
	return nil
}

func (ch accessListAddSlotChange) dirtied() (common.Address, bool) {
	return common.Address{}, false
}
