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
	kindEnd // not a kind; bounds the range TestJournalDirtySymmetry must cover
)

const (
	flagCommitted                  uint8 = 1 << 0 // the reverted value was already committed to state
	flagSelfdestructPrev           uint8 = 1 << 1 // kindSelfdestruct: account had already selfdestructed
	flagSelfdestructHadIncarnation uint8 = 1 << 2 // kindSelfdestruct: a versioned incarnation write predated the destruct
	flagSelfdestructHadBalance     uint8 = 1 << 3 // kindSelfdestruct: a versioned balance write predated the destruct
)

type journalExtra struct {
	prevObj              *stateObject         // kindResetObject
	prevWrites           *createWriteSnapshot // kindResetObject
	bi                   *BalanceIncrease     // kindBalanceIncreaseTransfer
	prevhash             accounts.CodeHash    // kindCode
	prevcode             []byte               // kindCode
	prevBalanceVersioned uint256.Int          // kindSelfdestruct, when flagSelfdestructHadBalance is set
}

type journalEntry struct {
	account accounts.Address
	key     accounts.StorageKey
	extra   *journalExtra
	value   uint256.Int
	aux     uint64
	kind    entryKind
	flags   uint8
}

type journal struct {
	dirties map[accounts.Address]int
	entries []journalEntry
}

func newJournal() *journal {
	return journalPool.Get().(*journal)
}

func (j *journal) release() {
	j.Reset()
	clear(j.entries[:cap(j.entries)]) // [:cap] because Reset already resliced to zero
	journalPool.Put(j)
}
func (j *journal) Reset() {
	j.entries = j.entries[:0]
	clear(j.dirties)
}

func (j *journal) revert(statedb *IntraBlockState, snapshot int) {
	for i := len(j.entries) - 1; i >= snapshot; i-- {
		// A revert error would leave state half-reverted, so panic instead of continuing with corrupt state.
		if err := j.entries[i].revert(statedb); err != nil {
			panic(fmt.Sprintf("journal: revert of kind %d failed: %v", j.entries[i].kind, err))
		}

		if addr, isdirty := j.entries[i].dirtied(); isdirty {
			if j.dirties[addr]--; j.dirties[addr] == 0 {
				delete(j.dirties, addr)
			}
		}
	}
	j.entries = j.entries[:snapshot]
}

// dirty force-marks an address dirty for the RIPEMD consensus exception and for a resurrected address across revert.
func (j *journal) dirty(addr accounts.Address) {
	j.dirties[addr]++
}

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

func (j *journal) createObjectChange(account accounts.Address) {
	j.entries = append(j.entries, journalEntry{kind: kindCreateObject, account: account})
	j.dirties[account]++
}

func (j *journal) resetObjectChange(account accounts.Address, prev *stateObject, prevWrites *createWriteSnapshot) {
	j.entries = append(j.entries, journalEntry{kind: kindResetObject, account: account, extra: &journalExtra{prevObj: prev, prevWrites: prevWrites}})
	j.dirties[account]++
}

func (j *journal) selfdestructChange(account accounts.Address, prev bool, prevbalance uint256.Int, wasCommitted bool) {
	flags := commitFlag(wasCommitted)
	if prev {
		flags |= flagSelfdestructPrev
	}
	j.entries = append(j.entries, journalEntry{kind: kindSelfdestruct, account: account, value: prevbalance, flags: flags})
	j.dirties[account]++
}

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
	j.entries = append(j.entries, e)
	j.dirties[account]++
}

func (j *journal) balanceChange(account accounts.Address, prev uint256.Int, wasCommitted bool) {
	j.entries = append(j.entries, journalEntry{kind: kindBalance, account: account, value: prev, flags: commitFlag(wasCommitted)})
	j.dirties[account]++
}

func (j *journal) balanceIncrease(account accounts.Address, increase uint256.Int) {
	j.entries = append(j.entries, journalEntry{kind: kindBalanceIncrease, account: account, value: increase})
	j.dirties[account]++
}

func (j *journal) balanceIncreaseTransfer(bi *BalanceIncrease) {
	j.entries = append(j.entries, journalEntry{kind: kindBalanceIncreaseTransfer, extra: &journalExtra{bi: bi}})
}

func (j *journal) nonceChange(account accounts.Address, prev uint64, wasCommitted bool) {
	j.entries = append(j.entries, journalEntry{kind: kindNonce, account: account, aux: prev, flags: commitFlag(wasCommitted)})
	j.dirties[account]++
}

func (j *journal) storageChange(account accounts.Address, key accounts.StorageKey, prevalue uint256.Int, wasCommitted bool) {
	j.entries = append(j.entries, journalEntry{kind: kindStorage, account: account, key: key, value: prevalue, flags: commitFlag(wasCommitted)})
	j.dirties[account]++
}

func (j *journal) fakeStorageChange(account accounts.Address, key accounts.StorageKey, prevalue uint256.Int) {
	j.entries = append(j.entries, journalEntry{kind: kindFakeStorage, account: account, key: key, value: prevalue})
	j.dirties[account]++
}

func (j *journal) codeChange(account accounts.Address, prevcode []byte, prevhash accounts.CodeHash, wasCommitted bool) {
	j.entries = append(j.entries, journalEntry{kind: kindCode, account: account, flags: commitFlag(wasCommitted), extra: &journalExtra{prevcode: prevcode, prevhash: prevhash}})
	j.dirties[account]++
}

func (j *journal) refundChange(prev uint64) {
	j.entries = append(j.entries, journalEntry{kind: kindRefund, aux: prev})
}

func (j *journal) addLogChange(txIndex int) {
	j.entries = append(j.entries, journalEntry{kind: kindAddLog, aux: uint64(txIndex)})
}

func (j *journal) touchAccount(account accounts.Address, wasCommitted bool, prev uint256.Int) {
	j.entries = append(j.entries, journalEntry{kind: kindTouch, account: account, value: prev, flags: commitFlag(wasCommitted)})
	j.dirties[account]++
}

func (j *journal) accessListAddAccountChange(address accounts.Address) {
	j.entries = append(j.entries, journalEntry{kind: kindAccessListAddAccount, account: address})
}

func (j *journal) accessListAddSlotChange(address accounts.Address, slot accounts.StorageKey) {
	j.entries = append(j.entries, journalEntry{kind: kindAccessListAddSlot, account: address, key: slot})
}

func (j *journal) transientStorageChange(account accounts.Address, key accounts.StorageKey, prevalue uint256.Int) {
	j.entries = append(j.entries, journalEntry{kind: kindTransientStorage, account: account, key: key, value: prevalue})
}

// kindCreateObject and kindResetObject must both stay dirty: dropping either loses a recreated account and diverges the state root.
func (je *journalEntry) dirtied() (accounts.Address, bool) {
	switch je.kind {
	case kindBalanceIncreaseTransfer, kindTransientStorage, kindRefund, kindAddLog, kindAccessListAddAccount, kindAccessListAddSlot:
		return accounts.NilAddress, false
	case kindCreateObject, kindResetObject, kindSelfdestruct, kindBalance, kindBalanceIncrease, kindNonce, kindStorage, kindFakeStorage, kindCode, kindTouch:
		return je.account, true
	}
	panic("dirtied: unknown journal entry kind")
}

var ripemd = accounts.InternAddress(common.HexToAddress("0000000000000000000000000000000000000003"))

func (je *journalEntry) revert(s *IntraBlockState) error {
	switch je.kind {
	case kindCreateObject:
		if so, ok := s.stateObjects[je.account]; ok {
			so.release()
		}
		delete(s.stateObjects, je.account)
		delete(s.stateObjectsDirty, je.account)
		// The account didn't exist before this create, so drop its account-record writes here — they have no field-level journal entry.
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
		// Restores the account-record writes the recreation overwrote, so versionedWrites reflects prev again.
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
			// Restores the pre-destruct balance write, or drops it if self-destruct created it; committed here means SelfDestructPath, not BalancePath.
			if je.flags&flagSelfdestructHadBalance != 0 {
				s.versionedWrites.updateBalance(je.account, je.extra.prevBalanceVersioned)
			} else {
				s.versionedWrites.DelBalance(je.account)
			}
			// Same restore-or-drop pattern for the incarnation cell.
			if je.flags&flagSelfdestructHadIncarnation != 0 {
				s.versionedWrites.updateIncarnation(je.account, je.aux)
			} else {
				s.versionedWrites.DelIncarnation(je.account)
			}
		}
		return nil

	case kindBalance:
		// Never materialize a stateObject just to revert on the parallel path — the versioned cells below are authoritative there.
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
		s.logs.revertLast(int(je.aux))
		return nil

	case kindTouch:
		// Keep versionedReads: the read already happened and influenced execution.
		// Undo BalancePath except for RIPEMD-160, which EIP-161 must still sweep.
		if s.versionMap != nil && je.account != ripemd {
			if je.committed() {
				s.versionedWrites.DelBalance(je.account)
			} else if _, ok := s.versionedWrites.GetBalance(je.account); ok {
				s.versionedWrites.updateBalance(je.account, je.value)
			}
		}
		return nil

	case kindAccessListAddAccount:
		// Adding a (addr, slot) whose addr is not yet present emits address then slot, so unrolling can blindly delete the address here.
		s.accessList.DeleteAddress(je.account)
		return nil

	case kindAccessListAddSlot:
		s.accessList.DeleteSlot(je.account, je.key)
		return nil
	}
	panic(fmt.Sprintf("revert: unknown journal entry kind %d", je.kind))
}
