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
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// journalEntryKind is the discriminator for journal entry types.
type journalEntryKind uint8

const (
	kindCreateObject journalEntryKind = iota
	kindResetObject
	kindSelfDestruct
	kindBalanceChange
	kindBalanceIncrease
	kindBalanceIncreaseTransfer
	kindNonceChange
	kindStorageChange
	kindFakeStorageChange
	kindCodeChange
	kindRefundChange
	kindAddLog
	kindTouchAccount
	kindAccessListAddAccount
	kindAccessListAddSlot
	kindTransientStorage
)

// journalEntry is a discriminated union of all journal entry types.
// This avoids interface boxing allocations that would occur with []interface{}.
type journalEntry struct {
	kind journalEntryKind

	// Common fields used by most entry types
	account accounts.Address

	// For balance/nonce/storage changes
	prevBalance uint256.Int
	wasCommited bool

	// For storage changes
	key accounts.StorageKey

	// For nonce changes
	prevNonce uint64

	// For selfDestruct
	prevSelfDestructed bool

	// For code changes
	prevCode     []byte
	prevCodeHash accounts.CodeHash

	// For balance increase
	increase uint256.Int

	// For balance increase transfer
	bi *BalanceIncrease

	// For reset object
	prevObject *stateObject

	// For refund change
	prevRefund uint64

	// For add log
	txIndex int
}

var journalPool = sync.Pool{
	New: func() any {
		return &journal{
			dirties: make(map[accounts.Address]int),
			entries: make([]journalEntry, 0, 256),
		}
	},
}

// journal contains the list of state modifications applied since the last state
// commit. These are tracked to be able to be reverted in case of an execution
// exception or revertal request.
type journal struct {
	entries []journalEntry            // Current changes tracked by the journal
	dirties map[accounts.Address]int // Dirty accounts and the number of changes
}

// newJournal gets a journal from the pool.
func newJournal() *journal {
	return journalPool.Get().(*journal)
}

// release returns the journal to the pool after resetting it.
func (j *journal) release() {
	j.Reset()
	journalPool.Put(j)
}

func (j *journal) Reset() {
	// Clear entries but keep capacity
	for i := range j.entries {
		j.entries[i] = journalEntry{} // Clear to help GC
	}
	j.entries = j.entries[:0]
	clear(j.dirties)
}

// appendCreateObject adds a create object entry.
func (j *journal) appendCreateObject(account accounts.Address) {
	j.entries = append(j.entries, journalEntry{
		kind:    kindCreateObject,
		account: account,
	})
	j.dirties[account]++
}

// appendResetObject adds a reset object entry.
func (j *journal) appendResetObject(account accounts.Address, prev *stateObject) {
	j.entries = append(j.entries, journalEntry{
		kind:       kindResetObject,
		account:    account,
		prevObject: prev,
	})
	// resetObject doesn't dirty
}

// appendSelfDestruct adds a self destruct entry.
func (j *journal) appendSelfDestruct(account accounts.Address, prev bool, prevBalance uint256.Int, wasCommited bool) {
	j.entries = append(j.entries, journalEntry{
		kind:               kindSelfDestruct,
		account:            account,
		prevSelfDestructed: prev,
		prevBalance:        prevBalance,
		wasCommited:        wasCommited,
	})
	j.dirties[account]++
}

// appendBalanceChange adds a balance change entry.
func (j *journal) appendBalanceChange(account accounts.Address, prev uint256.Int, wasCommited bool) {
	j.entries = append(j.entries, journalEntry{
		kind:        kindBalanceChange,
		account:     account,
		prevBalance: prev,
		wasCommited: wasCommited,
	})
	j.dirties[account]++
}

// appendBalanceIncrease adds a balance increase entry.
func (j *journal) appendBalanceIncrease(account accounts.Address, increase uint256.Int) {
	j.entries = append(j.entries, journalEntry{
		kind:     kindBalanceIncrease,
		account:  account,
		increase: increase,
	})
	j.dirties[account]++
}

// appendBalanceIncreaseTransfer adds a balance increase transfer entry.
func (j *journal) appendBalanceIncreaseTransfer(bi *BalanceIncrease) {
	j.entries = append(j.entries, journalEntry{
		kind: kindBalanceIncreaseTransfer,
		bi:   bi,
	})
	// doesn't dirty
}

// appendNonceChange adds a nonce change entry.
func (j *journal) appendNonceChange(account accounts.Address, prev uint64, wasCommited bool) {
	j.entries = append(j.entries, journalEntry{
		kind:        kindNonceChange,
		account:     account,
		prevNonce:   prev,
		wasCommited: wasCommited,
	})
	j.dirties[account]++
}

// appendStorageChange adds a storage change entry.
func (j *journal) appendStorageChange(account accounts.Address, key accounts.StorageKey, prevalue uint256.Int, wasCommited bool) {
	j.entries = append(j.entries, journalEntry{
		kind:        kindStorageChange,
		account:     account,
		key:         key,
		prevBalance: prevalue, // reusing prevBalance for storage value
		wasCommited: wasCommited,
	})
	j.dirties[account]++
}

// appendFakeStorageChange adds a fake storage change entry.
func (j *journal) appendFakeStorageChange(account accounts.Address, key accounts.StorageKey, prevalue uint256.Int) {
	j.entries = append(j.entries, journalEntry{
		kind:        kindFakeStorageChange,
		account:     account,
		key:         key,
		prevBalance: prevalue, // reusing prevBalance for storage value
	})
	j.dirties[account]++
}

// appendCodeChange adds a code change entry.
func (j *journal) appendCodeChange(account accounts.Address, prevcode []byte, prevhash accounts.CodeHash, wasCommited bool) {
	j.entries = append(j.entries, journalEntry{
		kind:         kindCodeChange,
		account:      account,
		prevCode:     prevcode,
		prevCodeHash: prevhash,
		wasCommited:  wasCommited,
	})
	j.dirties[account]++
}

// appendRefundChange adds a refund change entry.
func (j *journal) appendRefundChange(prev uint64) {
	j.entries = append(j.entries, journalEntry{
		kind:       kindRefundChange,
		prevRefund: prev,
	})
	// doesn't dirty
}

// appendAddLog adds an add log entry.
func (j *journal) appendAddLog(txIndex int) {
	j.entries = append(j.entries, journalEntry{
		kind:    kindAddLog,
		txIndex: txIndex,
	})
	// doesn't dirty
}

// appendTouchAccount adds a touch account entry.
func (j *journal) appendTouchAccount(account accounts.Address) {
	j.entries = append(j.entries, journalEntry{
		kind:    kindTouchAccount,
		account: account,
	})
	j.dirties[account]++
}

// appendAccessListAddAccount adds an access list add account entry.
func (j *journal) appendAccessListAddAccount(address accounts.Address) {
	j.entries = append(j.entries, journalEntry{
		kind:    kindAccessListAddAccount,
		account: address,
	})
	// doesn't dirty
}

// appendAccessListAddSlot adds an access list add slot entry.
func (j *journal) appendAccessListAddSlot(address accounts.Address, slot accounts.StorageKey) {
	j.entries = append(j.entries, journalEntry{
		kind:    kindAccessListAddSlot,
		account: address,
		key:     slot,
	})
	// doesn't dirty
}

// appendTransientStorage adds a transient storage change entry.
func (j *journal) appendTransientStorage(account accounts.Address, key accounts.StorageKey, prevalue uint256.Int) {
	j.entries = append(j.entries, journalEntry{
		kind:        kindTransientStorage,
		account:     account,
		key:         key,
		prevBalance: prevalue, // reusing prevBalance for storage value
	})
	// doesn't dirty
}

// revert undoes a batch of journalled modifications along with any reverted
// dirty handling too.
func (j *journal) revert(statedb *IntraBlockState, snapshot int) {
	for i := len(j.entries) - 1; i >= snapshot; i-- {
		entry := &j.entries[i]
		// Undo the changes made by the operation
		revertEntry(entry, statedb)

		// Drop any dirty tracking induced by the change
		if addr, isdirty := entryDirtied(entry); isdirty {
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
func (j *journal) dirty(addr accounts.Address) {
	j.dirties[addr]++
}

// length returns the current number of entries in the journal.
func (j *journal) length() int {
	return len(j.entries)
}

// entryDirtied returns the address dirtied by this entry and whether it dirties.
func entryDirtied(e *journalEntry) (accounts.Address, bool) {
	switch e.kind {
	case kindCreateObject, kindSelfDestruct, kindBalanceChange, kindBalanceIncrease,
		kindNonceChange, kindStorageChange, kindFakeStorageChange, kindCodeChange, kindTouchAccount:
		return e.account, true
	default:
		return accounts.NilAddress, false
	}
}

var ripemd = accounts.InternAddress(common.HexToAddress("0000000000000000000000000000000000000003"))

// revertEntry reverts a single journal entry.
func revertEntry(e *journalEntry, s *IntraBlockState) error {
	switch e.kind {
	case kindCreateObject:
		if so, ok := s.stateObjects[e.account]; ok {
			so.release()
		}
		delete(s.stateObjects, e.account)
		delete(s.stateObjectsDirty, e.account)
		return nil

	case kindResetObject:
		if current, ok := s.stateObjects[e.account]; ok && current != e.prevObject {
			current.release()
		}
		s.setStateObject(e.account, e.prevObject)
		return nil

	case kindSelfDestruct:
		obj, err := s.getStateObject(e.account, false)
		if err != nil {
			return err
		}
		if obj != nil {
			trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(e.account.Handle()))
			var tracePrefix string
			if trace {
				tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
				fmt.Printf("%s Revert SelfDestruct %x: %v:%d, prev: %v:%d, commited: %v\n", tracePrefix, e.account, e.prevSelfDestructed, &e.prevBalance, obj.selfdestructed, &obj.data.Balance, e.wasCommited)
			}
			obj.selfdestructed = e.prevSelfDestructed
			obj.setBalance(e.prevBalance)

			if s.versionMap != nil {
				if e.wasCommited {
					if trace {
						if v, ok := s.versionedWrites[e.account][AccountKey{Path: SelfDestructPath}]; ok {
							fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, e.account, v.Val, &e.prevSelfDestructed)
						}
						if v, ok := s.versionedWrites[e.account][AccountKey{Path: BalancePath}]; ok {
							val := v.Val.(uint256.Int)
							fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, e.account, &val, &e.prevBalance)
						}
					}
					s.versionedWrites.Delete(e.account, AccountKey{Path: BalancePath})
					s.versionedWrites.Delete(e.account, AccountKey{Path: SelfDestructPath})
				} else {
					if v, ok := s.versionedWrites[e.account][AccountKey{Path: SelfDestructPath}]; ok {
						fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, e.account, v.Val, &e.prevSelfDestructed)
						v.Val = e.prevSelfDestructed
					}
					if v, ok := s.versionedWrites[e.account][AccountKey{Path: BalancePath}]; ok {
						val := v.Val.(uint256.Int)
						fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, e.account, &val, &e.prevBalance)
						v.Val = e.prevBalance
					}
				}
			}
		}
		return nil

	case kindBalanceChange:
		obj, err := s.getStateObject(e.account, false)
		if err != nil {
			return err
		}

		trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(e.account.Handle()))
		var tracePrefix string
		if trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
			fmt.Printf("%s Revert Balance %x: %d, prev: %d, orig: %d, commited: %v\n", tracePrefix, e.account, &obj.data.Balance, &e.prevBalance, &obj.original.Balance, e.wasCommited)
		}
		obj.setBalance(e.prevBalance)
		if s.versionMap != nil {
			if e.wasCommited {
				if trace {
					if v, ok := s.versionedWrites[e.account][AccountKey{Path: BalancePath}]; ok {
						val := v.Val.(uint256.Int)
						fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, e.account, &val, &e.prevBalance)
					}
				}
				s.versionedWrites.Delete(e.account, AccountKey{Path: BalancePath})
			} else {
				if v, ok := s.versionedWrites[e.account][AccountKey{Path: BalancePath}]; ok {
					if trace {
						val := v.Val.(uint256.Int)
						fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, e.account, &val, &e.prevBalance)
					}
					v.Val = e.prevBalance
				}
			}
		}
		return nil

	case kindBalanceIncrease:
		if bi, ok := s.balanceInc[e.account]; ok {
			bi.increase.Sub(&bi.increase, &e.increase)
			bi.count--
			if bi.count == 0 {
				delete(s.balanceInc, e.account)
			}
		}
		return nil

	case kindBalanceIncreaseTransfer:
		e.bi.transferred = false
		return nil

	case kindNonceChange:
		obj, err := s.getStateObject(e.account, false)
		if err != nil {
			return err
		}

		trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(e.account.Handle()))
		var tracePrefix string
		if trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
			fmt.Printf("%s Revert Nonce %x: %d, prev: %d, orig: %d, commited: %v\n", tracePrefix, e.account, obj.data.Nonce, &e.prevNonce, obj.original.Nonce, e.wasCommited)
		}
		obj.setNonce(e.prevNonce)
		if s.versionMap != nil {
			if e.wasCommited {
				if trace {
					if v, ok := s.versionedWrites[e.account][AccountKey{Path: NoncePath}]; ok {
						fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, e.account, v.Val, e.prevNonce)
					}
				}
				s.versionedWrites.Delete(e.account, AccountKey{Path: NoncePath})
			} else {
				if v, ok := s.versionedWrites[e.account][AccountKey{Path: NoncePath}]; ok {
					if trace {
						fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, e.account, v.Val, e.prevNonce)
					}
					v.Val = e.prevNonce
				}
			}
		}
		return nil

	case kindStorageChange:
		obj, err := s.getStateObject(e.account, false)
		if err != nil {
			return err
		}

		trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(e.account.Handle()))
		var tracePrefix string
		var val uint256.Int
		if trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
			val, _ = obj.GetState(e.key)
			commited, _ := obj.GetCommittedState(e.key)
			fmt.Printf("%s Revert State %x %x: %d, prev: %d, orig: %d, commited: %v\n", tracePrefix, e.account, e.key, &val, &e.prevBalance, &commited, e.wasCommited)
		}
		if s.versionMap != nil {
			if e.wasCommited {
				if trace {
					if v, ok := s.versionedWrites[e.account][AccountKey{Path: StoragePath, Key: e.key}]; ok {
						val := v.Val.(uint256.Int)
						fmt.Printf("%s WRT Revert %x: %x: %x -> %x\n", tracePrefix, e.account, e.key, &val, &e.prevBalance)
					}
				}
				s.versionedWrites.Delete(e.account, AccountKey{Path: StoragePath, Key: e.key})
			} else {
				if v, ok := s.versionedWrites[e.account][AccountKey{Path: StoragePath, Key: e.key}]; ok {
					if trace {
						val := v.Val.(uint256.Int)
						fmt.Printf("%s WRT Revert %x: %x: %d -> %d\n", tracePrefix, e.account, e.key, &val, &e.prevBalance)
					}
					v.Val = e.prevBalance
				}
			}
		}
		obj.setState(e.key, e.prevBalance)
		return nil

	case kindFakeStorageChange:
		obj, err := s.getStateObject(e.account, false)
		if err != nil {
			return err
		}
		obj.fakeStorage[e.key] = e.prevBalance
		return nil

	case kindCodeChange:
		obj, err := s.getStateObject(e.account, false)
		if err != nil {
			return err
		}

		trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(e.account.Handle()))
		var tracePrefix string
		if trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
			_, cs := printCode(obj.code)
			_, ps := printCode(e.prevCode)
			fmt.Printf("%s Revert Code %x: %x:%s, prevHash: %x, origHash: %x, prevCode: %s, commited: %v\n", tracePrefix,
				e.account, obj.data.CodeHash, cs, e.prevCodeHash, obj.original.CodeHash, ps, e.wasCommited)
		}
		obj.setCode(e.prevCodeHash, e.prevCode)
		if s.versionMap != nil {
			if e.wasCommited {
				if trace {
					if v, ok := s.versionedWrites[e.account][AccountKey{Path: CodeHashPath}]; ok {
						fmt.Printf("%s WRT Revert %x: %x -> %x\n", tracePrefix, e.account, v.Val.(accounts.CodeHash), e.prevCodeHash)
					}
					if v, ok := s.versionedWrites[e.account][AccountKey{Path: CodePath}]; ok {
						_, cs := printCode(v.Val.([]byte))
						_, ps := printCode(e.prevCode)
						fmt.Printf("%s WRT Revert %x: %s -> %s\n", tracePrefix, e.account, cs, ps)
					}
				}
				s.versionedWrites.Delete(e.account, AccountKey{Path: CodeHashPath})
				s.versionedWrites.Delete(e.account, AccountKey{Path: CodePath})
			} else {
				if v, ok := s.versionedWrites[e.account][AccountKey{Path: CodePath}]; ok {
					if trace {
						_, cs := printCode(v.Val.([]byte))
						_, ps := printCode(e.prevCode)
						fmt.Printf("%s WRT Revert %x: %s -> %s\n", tracePrefix, e.account, cs, ps)
					}
					v.Val = e.prevCode
				}
				if v, ok := s.versionedWrites[e.account][AccountKey{Path: CodeHashPath}]; ok {
					if trace {
						fmt.Printf("%s WRT Revert %x: %x -> %x\n", tracePrefix, e.account, v.Val, e.prevCodeHash)
					}
					v.Val = e.prevCodeHash
				}
			}
		}
		return nil

	case kindRefundChange:
		s.refund = e.prevRefund
		return nil

	case kindAddLog:
		if e.txIndex >= len(s.logs) {
			panic(fmt.Sprintf("can't revert log index %v, max: %v", e.txIndex, len(s.logs)-1))
		}
		txnLogs := s.logs[e.txIndex]
		s.logs[e.txIndex] = txnLogs[:len(txnLogs)-1] // revert 1 log
		if len(s.logs[e.txIndex]) == 0 {
			s.logs = s.logs[:len(s.logs)-1] // revert txn
		}
		s.logSize--
		return nil

	case kindTouchAccount:
		if reads, ok := s.versionedReads[e.account]; ok {
			if len(reads) == 1 {
				if _, ok := reads[AccountKey{Path: AddressPath}]; ok {
					if opts, ok := s.addressAccess[e.account]; !ok || opts.revertable {
						delete(s.versionedReads, e.account)
						delete(s.addressAccess, e.account)
					}
				}
			}
		}
		return nil

	case kindAccessListAddAccount:
		s.accessList.DeleteAddress(e.account)
		return nil

	case kindAccessListAddSlot:
		s.accessList.DeleteSlot(e.account, e.key)
		return nil

	case kindTransientStorage:
		s.setTransientState(e.account, e.key, e.prevBalance)
		return nil

	default:
		panic(fmt.Sprintf("unknown journal entry kind: %d", e.kind))
	}
}
