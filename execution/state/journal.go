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
	flagCommitted        uint8 = 1 << 0 // the reverted value was already committed to state
	flagSelfdestructPrev uint8 = 1 << 1 // selfdestructChange: account had already selfdestructed
)

// journalExtra holds the fields needed only by the infrequent entry kinds, kept
// out of line so the common entry stays at one machine-friendly 72-byte value
// with a single (usually nil) pointer for the GC to scan.
type journalExtra struct {
	prevObj  *stateObject      // kindResetObject
	bi       *BalanceIncrease  // kindBalanceIncreaseTransfer
	prevcode []byte            // kindCode
	prevhash accounts.CodeHash // kindCode
}

// journalEntry is a compact tagged union stored inline in journal.entries,
// replacing the old per-entry heap-boxed interface. Fields are reused across
// kinds: value holds the reverted uint256, aux the reverted scalar (nonce/refund/
// log index), flags the booleans, extra only what the infrequent kinds need.
type journalEntry struct {
	kind    entryKind
	flags   uint8
	account accounts.Address
	key     accounts.StorageKey
	value   uint256.Int
	aux     uint64
	extra   *journalExtra
}

// journal contains the list of state modifications applied since the last state
// commit. These are tracked to be able to be reverted in case of an execution
// exception or revertal request.
type journal struct {
	entries []journalEntry           // Current changes tracked by the journal
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

func (j *journal) resetObjectChange(account accounts.Address, prev *stateObject) {
	j.append(journalEntry{kind: kindResetObject, account: account, extra: &journalExtra{prevObj: prev}})
}

func (j *journal) selfdestructChange(account accounts.Address, prev bool, prevbalance uint256.Int, wasCommitted bool) {
	flags := commitFlag(wasCommitted)
	if prev {
		flags |= flagSelfdestructPrev
	}
	j.append(journalEntry{kind: kindSelfdestruct, account: account, value: prevbalance, flags: flags})
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

func (j *journal) touchAccount(account accounts.Address) {
	j.append(journalEntry{kind: kindTouch, account: account})
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
	default:
		return je.account, true
	}
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
		return nil

	case kindResetObject:
		prev := je.extra.prevObj
		if current, ok := s.stateObjects[je.account]; ok && current != prev {
			current.release()
		}
		s.setStateObject(je.account, prev)
		return nil

	case kindSelfdestruct:
		obj, err := s.getStateObject(je.account, false)
		if err != nil {
			return err
		}
		if obj != nil {
			prev := je.flags&flagSelfdestructPrev != 0
			wasCommitted := je.committed()
			trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(je.account.Handle()))
			var tracePrefix string
			if trace {
				tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
				fmt.Printf("%s Revert SelfDestruct %x: %v:%d, prev: %v:%d, commited: %v\n", tracePrefix, je.account, prev, &je.value, obj.selfdestructed, &obj.data.Balance, wasCommitted)
			}
			obj.selfdestructed = prev
			obj.setBalance(je.value)

			if s.versionMap != nil {
				if wasCommitted {
					if trace {
						if v, ok := s.versionedWrites.GetSelfDestruct(je.account); ok {
							sd := v.Val
							fmt.Printf("%s WRT Revert %x: %v -> %v\n", tracePrefix, je.account, sd, prev)
						}
						if v, ok := s.versionedWrites.GetBalance(je.account); ok {
							val := v.Val
							fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, je.account, &val, &je.value)
						}
					}
					s.versionedWrites.DelBalance(je.account)
					s.versionedWrites.DelSelfDestruct(je.account)
				} else {
					if v, ok := s.versionedWrites.GetSelfDestruct(je.account); ok {
						if trace {
							sd := v.Val
							fmt.Printf("%s WRT Revert %x: %v -> %v\n", tracePrefix, je.account, sd, prev)
						}
						s.versionedWrites.updateSelfDestruct(je.account, prev)
					}
					if v, ok := s.versionedWrites.GetBalance(je.account); ok {
						val := v.Val
						if trace {
							fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, je.account, &val, &je.value)
						}
						s.versionedWrites.updateBalance(je.account, je.value)
					}
				}
			}
		}
		return nil

	case kindBalance:
		obj, err := s.getStateObject(je.account, false)
		if err != nil {
			return err
		}
		wasCommitted := je.committed()
		trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(je.account.Handle()))
		var tracePrefix string
		if trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
			fmt.Printf("%s Revert Balance %x: %d, prev: %d, orig: %d, commited: %v\n", tracePrefix, je.account, &obj.data.Balance, &je.value, &obj.original.Balance, wasCommitted)
		}
		obj.setBalance(je.value)
		if s.versionMap != nil {
			if wasCommitted {
				if trace {
					if v, ok := s.versionedWrites.GetBalance(je.account); ok {
						val := v.Val
						fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, je.account, &val, &je.value)
					}
				}
				s.versionedWrites.DelBalance(je.account)
			} else {
				if v, ok := s.versionedWrites.GetBalance(je.account); ok {
					if trace {
						val := v.Val
						fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, je.account, &val, &je.value)
					}
					s.versionedWrites.updateBalance(je.account, je.value)
				}
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
		obj, err := s.getStateObject(je.account, false)
		if err != nil {
			return err
		}
		wasCommitted := je.committed()
		trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(je.account.Handle()))
		var tracePrefix string
		if trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
			fmt.Printf("%s Revert Nonce %x: %d, prev: %d, orig: %d, commited: %v\n", tracePrefix, je.account, obj.data.Nonce, je.aux, obj.original.Nonce, wasCommitted)
		}
		obj.setNonce(je.aux)
		if s.versionMap != nil {
			if wasCommitted {
				if trace {
					if v, ok := s.versionedWrites.GetNonce(je.account); ok {
						n := v.Val
						fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, je.account, n, je.aux)
					}
				}
				s.versionedWrites.DelNonce(je.account)
			} else {
				if v, ok := s.versionedWrites.GetNonce(je.account); ok {
					if trace {
						n := v.Val
						fmt.Printf("%s WRT Revert %x: %d -> %d\n", tracePrefix, je.account, n, je.aux)
					}
					s.versionedWrites.updateNonce(je.account, je.aux)
				}
			}
		}
		return nil

	case kindCode:
		obj, err := s.getStateObject(je.account, false)
		if err != nil {
			return err
		}
		prevcode := je.extra.prevcode
		prevhash := je.extra.prevhash
		wasCommitted := je.committed()
		trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(je.account.Handle()))
		var tracePrefix string
		if trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
			_, cs := printCode(obj.code.Bytes)
			_, ps := printCode(prevcode)
			fmt.Printf("%s Revert Code %x: %x:%s, prevHash: %x, origHash: %x, prevCode: %s, commited: %v\n", tracePrefix,
				je.account, obj.data.CodeHash, cs, prevhash, obj.original.CodeHash, ps, wasCommitted)
		}
		obj.setCode(accounts.Code{Hash: prevhash, Bytes: prevcode})
		if s.versionMap != nil {
			if wasCommitted {
				if trace {
					if v, ok := s.versionedWrites.GetCodeHash(je.account); ok {
						fmt.Printf("%s WRT Revert %x: %x -> %x\n", tracePrefix, je.account, v.Val, prevhash)
					}
					if v, ok := s.versionedWrites.GetCode(je.account); ok {
						_, cs := printCode(v.Val.Bytes)
						_, ps := printCode(prevcode)
						fmt.Printf("%s WRT Revert %x: %s -> %s\n", tracePrefix, je.account, cs, ps)
					}
				}
				s.versionedWrites.DelCodeHash(je.account)
				s.versionedWrites.DelCode(je.account)
				s.versionedWrites.DelCodeSize(je.account)
			} else {
				if v, ok := s.versionedWrites.GetCode(je.account); ok {
					if trace {
						_, cs := printCode(v.Val.Bytes)
						_, ps := printCode(prevcode)
						fmt.Printf("%s WRT Revert %x: %s -> %s\n", tracePrefix, je.account, cs, ps)
					}
					s.versionedWrites.updateCode(je.account, accounts.Code{Hash: prevhash, Bytes: prevcode})
				}
				if v, ok := s.versionedWrites.GetCodeHash(je.account); ok {
					if trace {
						h := v.Val
						fmt.Printf("%s WRT Revert %x: %x -> %x\n", tracePrefix, je.account, h, prevhash)
					}
					s.versionedWrites.updateCodeHash(je.account, prevhash)
				}
				if _, ok := s.versionedWrites.GetCodeSize(je.account); ok {
					s.versionedWrites.updateCodeSize(je.account, len(prevcode))
				}
			}
		}
		return nil

	case kindStorage:
		obj, err := s.getStateObject(je.account, false)
		if err != nil {
			return err
		}
		wasCommitted := je.committed()
		trace := dbg.TraceTransactionIO && (s.trace || dbg.TraceAccount(je.account.Handle()))
		var tracePrefix string
		if trace {
			tracePrefix = fmt.Sprintf("%d (%d.%d)", s.blockNum, s.txIndex, s.version)
			val, _ := obj.GetState(je.key)
			commited, _ := obj.GetCommittedState(je.key)
			fmt.Printf("%s Revert State %x %x: %d, prev: %d, orig: %d, commited: %v\n", tracePrefix, je.account, je.key, &val, &je.value, &commited, wasCommitted)
		}
		if s.versionMap != nil {
			if wasCommitted {
				if trace {
					if v, ok := s.versionedWrites.GetStorage(je.account, je.key); ok {
						val := v.Val
						fmt.Printf("%s WRT Revert %x: %x: %x -> %x\n", tracePrefix, je.account, je.key, &val, &je.value)
					}
				}
				s.versionedWrites.DelStorage(je.account, je.key)
			} else {
				if v, ok := s.versionedWrites.GetStorage(je.account, je.key); ok {
					if trace {
						val := v.Val
						fmt.Printf("%s WRT Revert %x: %x: %d -> %d\n", tracePrefix, je.account, je.key, &val, &je.value)
					}
					s.versionedWrites.updateStorage(je.account, je.key, je.value)
				}
			}
		}
		obj.setState(je.key, je.value)
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
