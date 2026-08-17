// Copyright 2024 The Erigon Authors
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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var codePathRecoveryHashMismatch = metrics.GetOrCreateCounter("exec3_codepath_recovery_hash_mismatch")

// Normalize produces a clean write set matching serial IBS MakeWriteSet:
// no-op filtering, self-destruct cascade, account-field resolution.
// domainStorageKeys must also cover storage committed before this batch.
func (writes *WriteSet) Normalize(vm *VersionMap, txIndex int, incarnation int, stateReader StateReader, domainStorageKeys func(addr accounts.Address) []accounts.StorageKey, emptyRemoval bool, isAura bool, eip8246 bool) (*WriteSet, error) {
	filtered := &WriteSet{}
	if writes == nil {
		return filtered, nil
	}

	// Deduped union of batch-written and pre-committed storage slots — the full set to DELETE when addr self-destructs.
	sdStorageSlots := func(addr accounts.Address) []accounts.StorageKey {
		seen := make(map[accounts.StorageKey]struct{})
		var out []accounts.StorageKey
		for _, k := range vm.StorageKeys(addr) {
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				out = append(out, k)
			}
		}
		if domainStorageKeys != nil {
			for _, k := range domainStorageKeys(addr) {
				if _, ok := seen[k]; !ok {
					seen[k] = struct{}{}
					out = append(out, k)
				}
			}
		}
		return out
	}

	// SD'd addresses drop their other account-field writes below, else
	// applyVersionedWrites takes cleanup-before-recreate over a pure delete.
	var sdSet map[accounts.Address]bool
	for addr, vw := range writes.selfDestruct {
		if vw.Version.Incarnation == incarnation && vw.Val {
			if sdSet == nil {
				sdSet = make(map[accounts.Address]bool)
			}
			sdSet[addr] = true
		}
	}

	for h := range writes.AllHeaders() {
		if sdSet[h.Address] {
			switch h.Path {
			case NoncePath, IncarnationPath, CodeHashPath, CodePath, StoragePath:
				continue
			case BalancePath:
				// EIP-8246 keeps the post-SD balance; pre-8246 drops it.
				if !eip8246 {
					continue
				}
			}
		}
		switch h.Path {
		case StoragePath:
			if h.Version.Incarnation != incarnation {
				continue
			}
			sw, ok := writes.GetStorage(h.Address, h.Key)
			if !ok {
				continue
			}
			writeVal := sw.Val
			// An earlier SD in this block zeroes addr's storage baseline, unseen
			// by the versionMap/domain — catch it here so a resurrect's
			// write-back isn't wrongly treated as a no-op.
			sdTxIdx, sdOk := -1, false
			if sdVer, ok := vm.FindDoneSelfDestructInRange(h.Address, 0, txIndex, true); ok {
				sdTxIdx, sdOk = sdVer.TxIndex, true
			}
			originVal, origin, originOK := vm.ReadStorage(h.Address, h.Key, txIndex)
			originValid := originOK && origin.Status() == MVReadResultDone &&
				!(sdOk && sdTxIdx > origin.Version().TxIndex)
			switch {
			case originValid:
				if writeVal.Eq(&originVal) {
					continue
				}
			case sdOk:
				if writeVal.IsZero() {
					continue
				}
			case stateReader != nil:
				// No SD found in range above: compare against pre-block value;
				// a pure CREATE (never SD'd) still no-ops same-value SSTOREs.
				if vm.AnyDoneSelfDestructEquals(h.Address, txIndex-1, true) {
					if writeVal.IsZero() {
						continue
					}
				} else {
					preVal, found, err := stateReader.ReadAccountStorage(h.Address, h.Key)
					if err != nil {
						return nil, err
					}
					if !found && writeVal.IsZero() {
						continue
					}
					if found && writeVal.Eq(&preVal) {
						continue
					}
				}
			}
			filtered.SetStorage(h.Address, h.Key, sw)
		case BalancePath, NoncePath, IncarnationPath, CodeHashPath:
			// Prefer the versionMap's accumulated value over the raw write.
			if !SetAccountFieldFromMap(filtered, vm, h.Address, h.Path, h.Version, txIndex+1) {
				switch h.Path {
				case BalancePath:
					if vw, ok := writes.GetBalance(h.Address); ok {
						filtered.SetBalance(h.Address, vw)
					}
				case NoncePath:
					if vw, ok := writes.GetNonce(h.Address); ok {
						filtered.SetNonce(h.Address, vw)
					}
				case IncarnationPath:
					if vw, ok := writes.GetIncarnation(h.Address); ok {
						filtered.SetIncarnation(h.Address, vw)
					}
				case CodeHashPath:
					if vw, ok := writes.GetCodeHash(h.Address); ok {
						filtered.SetCodeHash(h.Address, vw)
					}
				}
			}
		case CodePath:
			if h.Version.Incarnation != incarnation {
				continue
			}
			if vw, ok := writes.GetCode(h.Address); ok {
				filtered.SetCode(h.Address, vw)
			}
		case CreateContractPath:
			if h.Version.Incarnation != incarnation {
				continue
			}
			if vw, ok := writes.GetCreateContract(h.Address); ok {
				filtered.SetCreateContract(h.Address, vw)
			}
		case SelfDestructPath:
			if h.Version.Incarnation != incarnation {
				continue
			}
			sdw, ok := writes.GetSelfDestruct(h.Address)
			if !ok || !sdw.Val {
				continue
			}
			filtered.SetSelfDestruct(h.Address, sdw)
			for _, slot := range sdStorageSlots(h.Address) {
				filtered.SetStorage(h.Address, slot, &VersionedWrite[uint256.Int]{
					WriteHeader: WriteHeader{
						Address: h.Address,
						Path:    StoragePath,
						Key:     slot,
						Version: h.Version,
					},
				})
			}
		case AddressPath:
			// AddressPath is record-level — skip for field-level consumers.
		case CodeSizePath:
			// Derived from code bytes; not a domain field, so not carried into the write set.
		}
	}

	// Every dirty address needs account fields filled in, covering storage-only or all-no-op addresses.
	allAddresses := make(map[accounts.Address]bool)
	writes.forEachFieldAddr(func(addr accounts.Address) { allAddresses[addr] = true })

	for addr := range allAddresses {
		if sdSet[addr] {
			continue // same drop as above — don't resurrect pre-SD fields
		}
		ver := Version{TxIndex: txIndex, Incarnation: incarnation}

		sdEarlier := false
		if v, sd, _ := vm.ReadSelfDestruct(addr, txIndex); sd.Status() == MVReadResultDone && v {
			sdEarlier = true
		}

		// Post-SD defaults apply only on a CREATE/CREATE2 revival; a plain value-transfer revival inherits pre-SD fields instead.
		hasCreateContract := false
		if vw, ok := writes.GetCreateContract(addr); ok && vw.Val {
			hasCreateContract = true
		}

		// Decode the account once per address — every missing field's stateReader fallback reads the same one.
		var fallbackAcc *accounts.Account
		fallbackLoaded := false

		for _, path := range []AccountPath{BalancePath, NoncePath, IncarnationPath, CodeHashPath} {
			if filtered.Has(WriteHeader{Address: addr, Path: path}) {
				continue
			}
			if sdEarlier && hasCreateContract {
				SetAccountFieldZero(filtered, addr, path, ver)
				continue
			}
			if SetAccountFieldFromMap(filtered, vm, addr, path, ver, txIndex+1) {
				continue
			}
			if stateReader != nil {
				if !fallbackLoaded {
					acc, err := stateReader.ReadAccountData(addr)
					if err != nil {
						return nil, err
					}
					fallbackAcc = acc
					fallbackLoaded = true
				}
				// fallbackAcc == nil (new account): emits defaults, not a delete.
				SetAccountFieldFromAccount(filtered, addr, path, ver, fallbackAcc)
			}
		}
	}

	// A validated writeset can end up with a fresh codeHash but no CodePath (a re-executing 7702 short-circuit): recover the code and re-emit it.
	for addr, hvw := range filtered.codeHash {
		h := hvw.Val
		if h.IsEmpty() || sdSet[addr] {
			continue
		}
		if _, ok := filtered.code[addr]; ok {
			continue
		}
		// Prefer the versionMap; a 7702 short-circuit re-exec leaves nothing there, so fall back to stateReader's post-state.
		var code []byte
		if c, _, ok := vm.ReadCode(addr, txIndex+1); ok {
			code = c.Bytes
		}
		if len(code) == 0 && stateReader != nil {
			c, err := stateReader.ReadAccountCode(addr)
			if err != nil {
				return nil, err
			}
			code = c
		}
		// The 7702 short-circuit is the only path leaving uncommitted code without a
		// CodePath; a CREATE2/unchanged redeploy already has its code in CodeDomain.
		if _, ok := types.ParseDelegation(code); !ok {
			continue
		}
		// Recovered bytes can race and disagree with the emitted codeHash; only re-emit when they actually hash to it.
		recovered := accounts.NewCode(code)
		if recovered.Hash.Value() != h.Value() {
			codePathRecoveryHashMismatch.Inc()
			log.Warn("[exec3] BAL codePath recovery skipped: recovered bytes do not hash to emitted codeHash",
				"addr", addr, "txIndex", txIndex, "emittedHash", h.Value(), "recoveredHash", recovered.Hash.Value())
			continue
		}
		filtered.SetCode(addr, &VersionedWrite[accounts.Code]{
			WriteHeader: WriteHeader{
				Address: addr,
				Path:    CodePath,
				Version: Version{TxIndex: txIndex, Incarnation: incarnation},
			},
			Val: recovered,
		})
	}

	// EIP-161: Balance=0/Nonce=0/empty CodeHash means delete, not a zero-valued account.
	type acctState struct {
		balance  uint256.Int
		nonce    uint64
		codeHash accounts.CodeHash
		hasBal   bool
		hasNonce bool
		hasCode  bool
	}
	acctStates := make(map[accounts.Address]acctState, len(filtered.balance))
	for addr, vw := range filtered.balance {
		s := acctStates[addr]
		s.balance = vw.Val
		s.hasBal = true
		acctStates[addr] = s
	}
	for addr, vw := range filtered.nonce {
		s := acctStates[addr]
		s.nonce = vw.Val
		s.hasNonce = true
		acctStates[addr] = s
	}
	for addr, vw := range filtered.codeHash {
		s := acctStates[addr]
		s.codeHash = vw.Val
		s.hasCode = true
		acctStates[addr] = s
	}

	// Only convert to Delete when EIP-161 is active — pre-fork, a merely touched empty account is created and persists instead.
	var emptyAddrs []accounts.Address
	for addr, s := range acctStates {
		if EIP161EmptyRemoval(emptyRemoval, isAura, addr) &&
			s.hasBal && s.hasNonce && s.hasCode &&
			s.balance.IsZero() && s.nonce == 0 && s.codeHash.IsEmpty() {
			emptyAddrs = append(emptyAddrs, addr)
		}
	}

	for _, addr := range emptyAddrs {
		filtered.DeleteAccountFields(addr)
		filtered.SetSelfDestruct(addr, &VersionedWrite[bool]{
			WriteHeader: WriteHeader{
				Address: addr,
				Path:    SelfDestructPath,
				Version: Version{TxIndex: txIndex, Incarnation: incarnation},
			},
			Val: true,
		})
	}

	return filtered, nil
}
