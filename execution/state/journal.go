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
	"github.com/erigontech/erigon/execution/types/accounts"
)

var journalPool = sync.Pool{
	New: func() any {
		return &journal{
			dirties: make(map[accounts.Address]int),
		}
	},
}

// entryKind discriminates the compact journal entry union below.
type entryKind uint8

const (
	kindCreateObject entryKind = iota
	kindResetObject
	kindSelfdestruct
	kindBalance
	kindBalanceIncrease
	kindBalanceIncreaseTransfer
	kindNonce
	kindStorage
	kindFakeStorage
	kindCode
	kindRefund
	kindAddLog
	kindTouch
	kindAccessListAddAccount
	kindAccessListAddSlot
	kindTransientStorage
)

const (
	flagCommitted                  uint8 = 1 << 0 // the reverted value was already committed to state
	flagSelfdestructPrev           uint8 = 1 << 1 // kindSelfdestruct: account had already selfdestructed
	flagSelfdestructHadIncarnation uint8 = 1 << 2 // kindSelfdestruct: a versioned incarnation write predated the destruct
	flagSelfdestructHadBalance     uint8 = 1 << 3 // kindSelfdestruct: a versioned balance write predated the destruct
)

// journalExtra holds the fields needed only by the infrequent entry kinds,
// kept out of line so the common entry stays small (see TestJournalEntrySize).
type journalExtra struct {
	prevObj              *stateObject         // kindResetObject
	prevWrites           *createWriteSnapshot // kindResetObject
	bi                   *BalanceIncrease     // kindBalanceIncreaseTransfer
	prevhash             accounts.CodeHash    // kindCode
	prevcode             []byte               // kindCode
	prevBalanceVersioned uint256.Int          // kindSelfdestruct, when flagSelfdestructHadBalance is set
}

// journalEntry is a compact tagged union stored inline in journal.entries.
// Fields are reused across kinds: value holds the reverted uint256, aux the
// reverted scalar (nonce/refund/log index/incarnation), flags the booleans,
// extra only what the infrequent kinds need.
type journalEntry struct {
	account accounts.Address
	key     accounts.StorageKey
	extra   *journalExtra
	value   uint256.Int
	aux     uint64
	kind    entryKind
	flags   uint8
}

// journal contains the list of state modifications applied since the last state
// commit. These are tracked to be able to be reverted in case of an execution
// exception or revertal request.
type journal struct {
	dirties map[accounts.Address]int // Dirty accounts and the number of changes
	entries []journalEntry           // Current changes tracked by the journal
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
	clear(j.entries)
	j.entries = j.entries[:0]
	clear(j.dirties)
}

// append inserts a new modification entry to the end of the change journal.
func (j *journal) append(e journalEntry) {
	j.entries = append(j.entries, e)
	if addr, isDirty := e.dirtied(); isDirty {
		j.dirties[addr]++
	}
}

// revert undoes a batch of journalled modifications along with any reverted
// dirty handling too.
func (j *journal) revert(statedb *IntraBlockState, snapshot int) {
	for i := len(j.entries) - 1; i >= snapshot; i-- {
		// Undo the changes made by the operation. A revert error means a
		// journalled account can no longer be loaded — the state would be left
		// half-reverted, so fail loudly rather than continue corrupt.
		if err := j.entries[i].revert(statedb); err != nil {
			panic(fmt.Sprintf("journal: revert of kind %d failed: %v", j.entries[i].kind, err))
		}

		// Drop any dirty tracking induced by the change
		if addr, isdirty := j.entries[i].dirtied(); isdirty {
			if j.dirties[addr]--; j.dirties[addr] == 0 {
				delete(j.dirties, addr)
			}
		}
	}
	clear(j.entries[snapshot:]) // release the reverted entries' extra pointers
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

func commitFlag(wasCommitted bool) uint8 {
	if wasCommitted {
		return flagCommitted
	}
	return 0
}

func (je *journalEntry) committed() bool { return je.flags&flagCommitted != 0 }

// --- entry constructors: one per modification kind, appended by the caller ---

func (j *journal) createObjectChange(account accounts.Address) {
	j.append(journalEntry{kind: kindCreateObject, account: account})
}

func (j *journal) resetObjectChange(account accounts.Address, prev *stateObject, prevWrites *createWriteSnapshot) {
	j.append(journalEntry{kind: kindResetObject, account: account, extra: &journalExtra{prevObj: prev, prevWrites: prevWrites}})
}

func (j *journal) selfdestructChange(account accounts.Address, prev bool, prevbalance uint256.Int, wasCommitted bool) {
	flags := commitFlag(wasCommitted)
	if prev {
		flags |= flagSelfdestructPrev
	}
	j.append(journalEntry{kind: kindSelfdestruct, account: account, value: prevbalance, flags: flags})
}

// selfdestructChangeVersioned records a self-destruct on the parallel path,
// capturing the versioned balance/incarnation writes it overwrites so a revert
// can restore them (see selfdestructChange.revert).
func (j *journal) selfdestructChangeVersioned(account accounts.Address, prev bool, prevbalance uint256.Int, wasCommitted, hadIncarnation bool, prevIncarnation uint64, hadBalance bool, prevBalanceVersioned uint256.Int) {
	flags := commitFlag(wasCommitted)
	if prev {
		flags |= flagSelfdestructPrev
	}
	if hadIncarnation {
		flags |= flagSelfdestructHadIncarnation
	}
	e := journalEntry{kind: kindSelfdestruct, account: account, value: prevbalance, aux: prevIncarnation, flags: flags}
	if hadBalance {
		e.flags |= flagSelfdestructHadBalance
		e.extra = &journalExtra{prevBalanceVersioned: prevBalanceVersioned}
	}
	j.append(e)
}

func (j *journal) balanceChange(account accounts.Address, prev uint256.Int, wasCommitted bool) {
	j.append(journalEntry{kind: kindBalance, account: account, value: prev, flags: commitFlag(wasCommitted)})
}

func (j *journal) balanceIncrease(account accounts.Address, increase uint256.Int) {
	j.append(journalEntry{kind: kindBalanceIncrease, account: account, value: increase})
}

func (j *journal) balanceIncreaseTransfer(bi *BalanceIncrease) {
	j.append(journalEntry{kind: kindBalanceIncreaseTransfer, extra: &journalExtra{bi: bi}})
}

func (j *journal) nonceChange(account accounts.Address, prev uint64, wasCommitted bool) {
	j.append(journalEntry{kind: kindNonce, account: account, aux: prev, flags: commitFlag(wasCommitted)})
}

func (j *journal) storageChange(account accounts.Address, key accounts.StorageKey, prevalue uint256.Int, wasCommitted bool) {
	j.append(journalEntry{kind: kindStorage, account: account, key: key, value: prevalue, flags: commitFlag(wasCommitted)})
}

func (j *journal) fakeStorageChange(account accounts.Address, key accounts.StorageKey, prevalue uint256.Int) {
	j.append(journalEntry{kind: kindFakeStorage, account: account, key: key, value: prevalue})
}

func (j *journal) codeChange(account accounts.Address, prevcode []byte, prevhash accounts.CodeHash, wasCommitted bool) {
	j.append(journalEntry{kind: kindCode, account: account, flags: commitFlag(wasCommitted), extra: &journalExtra{prevcode: prevcode, prevhash: prevhash}})
}

func (j *journal) refundChange(prev uint64) {
	j.append(journalEntry{kind: kindRefund, aux: prev})
}

func (j *journal) addLogChange(txIndex int) {
	j.append(journalEntry{kind: kindAddLog, aux: uint64(txIndex)})
}

func (j *journal) touchAccount(account accounts.Address, wasCommitted bool, prev uint256.Int) {
	j.append(journalEntry{kind: kindTouch, account: account, value: prev, flags: commitFlag(wasCommitted)})
}

func (j *journal) accessListAddAccountChange(address accounts.Address) {
	j.append(journalEntry{kind: kindAccessListAddAccount, account: address})
}

func (j *journal) accessListAddSlotChange(address accounts.Address, slot accounts.StorageKey) {
	j.append(journalEntry{kind: kindAccessListAddSlot, account: address, key: slot})
}

func (j *journal) transientStorageChange(account accounts.Address, key accounts.StorageKey, prevalue uint256.Int) {
	j.append(journalEntry{kind: kindTransientStorage, account: account, key: key, value: prevalue})
}

// dirtied returns the address modified by this entry, or (NilAddress, false) for
// entries that don't imply a dirty account. kindCreateObject and kindResetObject
// must both stay dirty: they place a stateObject at the same address, and
// dropping either loses a recreated account from dirties, diverging the state root.
func (je *journalEntry) dirtied() (accounts.Address, bool) {
	switch je.kind {
	case kindBalanceIncreaseTransfer, kindTransientStorage, kindRefund, kindAddLog, kindAccessListAddAccount, kindAccessListAddSlot:
		return accounts.NilAddress, false
	case kindCreateObject, kindResetObject, kindSelfdestruct, kindBalance, kindBalanceIncrease, kindNonce, kindStorage, kindFakeStorage, kindCode, kindTouch:
		return je.account, true
	}
	panic(fmt.Sprintf("dirtied: unknown journal entry kind %d", je.kind))
}

var ripemd = accounts.InternAddress(common.HexToAddress("0000000000000000000000000000000000000003"))

// revert undoes the change recorded by this entry.
func (je *journalEntry) revert(s *IntraBlockState) error {
	switch je.kind {
	case kindCreateObject:
		if so, ok := s.stateObjects[je.account]; ok {
			so.release()
		}
		delete(s.stateObjects, je.account)
		delete(s.stateObjectsDirty, je.account)
		// The account did not exist before this create, so all of its versioned
		// writes originate from the creation being reverted. Field-level entries
		// (balance/nonce/…) prune themselves on revert; the account-record writes
		// createObject emits (address/codeHash/…) have no field-level journal entry,
		// so drop them here to keep versionedWrites in step with the journal.
		if s.versionMap != nil {
			s.versionedWrites.deleteAddr(je.account)
		}
		return nil

	case kindResetObject:
		prev := je.extra.prevObj
		if current, ok := s.stateObjects[je.account]; ok && current != prev {
			current.release()
		}
		if s.noMaterialize {
			delete(s.stateObjects, je.account)
		} else {
			s.setStateObject(je.account, prev)
		}
		// Restore the account-record writes the recreation overwrote back to the
		// snapshot taken before it ran, so versionedWrites reflects prev's state
		// again (the field-level entries handle the fields creation doesn't write).
		if s.versionMap != nil {
			s.versionedWrites.restoreCreateFields(je.account, je.extra.prevWrites)
		}
		return nil

	case kindSelfdestruct:
		prev := je.flags&flagSelfdestructPrev != 0
		if so, ok := s.stateObjects[je.account]; ok {
			so.selfdestructed = prev
			so.setBalance(je.value)
		} else if s.versionMap == nil {
			obj, err := s.getStateObject(je.account, false)
			if err != nil {
				return err
			}
			if obj != nil {
				obj.selfdestructed = prev
				obj.setBalance(je.value)
			}
		}
		if s.versionMap != nil {
			if je.committed() {
				s.versionedWrites.DelSelfDestruct(je.account)
			} else if _, ok := s.versionedWrites.GetSelfDestruct(je.account); ok {
				s.versionedWrites.updateSelfDestruct(je.account, prev)
			}
			// The self-destruct records BalancePath=0; restore the pre-destruct
			// versioned balance write, or drop the cell if the self-destruct created
			// it. Gating this on committed (which describes SelfDestructPath, not
			// BalancePath) deleted balance writes that predated the snapshot.
			if je.flags&flagSelfdestructHadBalance != 0 {
				s.versionedWrites.updateBalance(je.account, je.extra.prevBalanceVersioned)
			} else {
				s.versionedWrites.DelBalance(je.account)
			}
			// selfdestructVersioned clears the incarnation cell on both paths. Restore
			// it to its pre-destruct versioned value, or drop the write if the
			// self-destruct created it.
			if je.flags&flagSelfdestructHadIncarnation != 0 {
				s.versionedWrites.updateIncarnation(je.account, je.aux)
			} else {
				s.versionedWrites.DelIncarnation(je.account)
			}
		}
		return nil

	case kindBalance:
		// Keep a materialized so.data in step (serial always has one; the parallel
		// path only when the account was materialized for some other reason). Never
		// materialize one just to revert on the parallel path — the cells below are
		// authoritative there.
		if so, ok := s.stateObjects[je.account]; ok {
			so.setBalance(je.value)
		} else if s.versionMap == nil {
			obj, err := s.getStateObject(je.account, false)
			if err != nil {
				return err
			}
			if obj != nil {
				obj.setBalance(je.value)
			}
		}
		if s.versionMap != nil {
			if je.committed() {
				s.versionedWrites.DelBalance(je.account)
			} else if _, ok := s.versionedWrites.GetBalance(je.account); ok {
				s.versionedWrites.updateBalance(je.account, je.value)
			}
		}
		return nil

	case kindBalanceIncrease:
		if bi, ok := s.balanceInc[je.account]; ok {
			bi.increase.Sub(&bi.increase, &je.value)
			bi.count--
			if bi.count == 0 {
				delete(s.balanceInc, je.account)
			}
		}
		return nil

	case kindBalanceIncreaseTransfer:
		je.extra.bi.transferred = false
		return nil

	case kindNonce:
		if so, ok := s.stateObjects[je.account]; ok {
			so.setNonce(je.aux)
		} else if s.versionMap == nil {
			obj, err := s.getStateObject(je.account, false)
			if err != nil {
				return err
			}
			if obj != nil {
				obj.setNonce(je.aux)
			}
		}
		if s.versionMap != nil {
			if je.committed() {
				s.versionedWrites.DelNonce(je.account)
			} else if _, ok := s.versionedWrites.GetNonce(je.account); ok {
				s.versionedWrites.updateNonce(je.account, je.aux)
			}
		}
		return nil

	case kindCode:
		prevcode := je.extra.prevcode
		prevhash := je.extra.prevhash
		if so, ok := s.stateObjects[je.account]; ok {
			so.setCode(accounts.Code{Hash: prevhash, Bytes: prevcode})
		} else if s.versionMap == nil {
			obj, err := s.getStateObject(je.account, false)
			if err != nil {
				return err
			}
			if obj != nil {
				obj.setCode(accounts.Code{Hash: prevhash, Bytes: prevcode})
			}
		}
		if s.versionMap != nil {
			if je.committed() {
				s.versionedWrites.DelCodeHash(je.account)
				s.versionedWrites.DelCode(je.account)
				s.versionedWrites.DelCodeSize(je.account)
			} else {
				if _, ok := s.versionedWrites.GetCode(je.account); ok {
					s.versionedWrites.updateCode(je.account, accounts.Code{Hash: prevhash, Bytes: prevcode})
				}
				if _, ok := s.versionedWrites.GetCodeHash(je.account); ok {
					s.versionedWrites.updateCodeHash(je.account, prevhash)
				}
				if _, ok := s.versionedWrites.GetCodeSize(je.account); ok {
					s.versionedWrites.updateCodeSize(je.account, len(prevcode))
				}
			}
		}
		return nil

	case kindStorage:
		if s.versionMap != nil {
			if je.committed() {
				s.versionedWrites.DelStorage(je.account, je.key)
			} else if _, ok := s.versionedWrites.GetStorage(je.account, je.key); ok {
				s.versionedWrites.updateStorage(je.account, je.key, je.value)
			}
		}
		if so, ok := s.stateObjects[je.account]; ok {
			so.setState(je.key, je.value)
		} else if s.versionMap == nil {
			obj, err := s.getStateObject(je.account, false)
			if err != nil {
				return err
			}
			if obj != nil {
				obj.setState(je.key, je.value)
			}
		}
		return nil

	case kindFakeStorage:
		obj, err := s.getStateObject(je.account, false)
		if err != nil {
			return err
		}
		obj.fakeStorage[je.key] = je.value
		return nil

	case kindTransientStorage:
		s.setTransientState(je.account, je.key, je.value)
		return nil

	case kindRefund:
		s.refund = je.aux
		return nil

	case kindAddLog:
		txIndex := int(je.aux)
		if txIndex+1 >= len(s.logs) {
			panic(fmt.Sprintf("can't revert log index %v, max: %v", txIndex, len(s.logs)-1))
		}
		txnLogs := s.logs[txIndex+1]
		s.logs[txIndex+1] = txnLogs[:len(txnLogs)-1] // revert 1 log
		if len(s.logs[txIndex+1]) == 0 {
			s.logs = s.logs[:len(s.logs)-1] // revert txn
		}
		s.logSize--
		return nil

	case kindTouch:
		// Do NOT delete versionedReads here.  Even though the touch is being
		// reverted (e.g. a CREATE that ran out of gas), the read that triggered
		// the touch already happened — the tx observed the account's state and
		// branched on it (e.g. Empty() returning true vs false).  Removing the
		// read-set entry causes ValidateVersion to miss the dependency, allowing
		// stale reads to pass validation and produce incorrect results.
		//
		// The touch's BalancePath=0 write must be undone, though: leaving it orphaned
		// lets Normalize's EIP-161 pass delete an account whose touch was rolled back.
		// Mirror kindBalance — drop the write if the touch created it, else restore
		// the prior value.
		if s.versionMap != nil {
			if je.committed() {
				s.versionedWrites.DelBalance(je.account)
			} else if _, ok := s.versionedWrites.GetBalance(je.account); ok {
				s.versionedWrites.updateBalance(je.account, je.value)
			}
		}
		return nil

	case kindAccessListAddAccount:
		/*
			One important invariant here, is that whenever a (addr, slot) is added, if the
			addr is not already present, the add causes two journal entries:
			- one for the address,
			- one for the (address,slot)
			Therefore, when unrolling the change, we can always blindly delete the
			(addr) at this point, since no storage adds can remain when come upon
			a single (addr) change.
		*/
		s.accessList.DeleteAddress(je.account)
		return nil

	case kindAccessListAddSlot:
		s.accessList.DeleteSlot(je.account, je.key)
		return nil
	}
	panic(fmt.Sprintf("revert: unknown journal entry kind %d", je.kind))
}
