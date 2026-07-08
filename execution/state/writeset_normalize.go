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

// Normalize produces a clean write set from the versionMap's WriteSet
// for a given TX. It matches the serial IBS MakeWriteSet behaviour:
//
//  1. Storage no-op filter: removes writes where the value equals the origin
//     (what this TX read at execution start). Matches applyStorageChanges
//     which skips keys where dirty == originStorage.
//
//  2. Incarnation filter: only includes writes from the validated incarnation.
//     Stale entries from prior incarnations are excluded.
//
//  3. Self-destruct: emits DELETE entries for all storage keys of self-destructed
//     accounts (matching DomainDelPrefix behaviour).
//
//  4. Account field resolution: resolves account field values from the versionMap
//     to get the correct accumulated values (not speculative worker values).
//
// The input is blockIO.WriteSet(txIndex) — the raw versionWritten output.
// The output is ready for applyVersionedWrites and TouchUpdates.
//
// domainStorageKeys, when non-nil, must return every storage slot currently
// committed for an address (from sd.mem + domain files) — used to emit the
// full StoragePath=0 cascade when the address self-destructs. vm.StorageKeys
// alone only covers slots written in the current batch; genesis-allocated or
// prior-block storage isn't there, so the calc would never delete those slots
// from the trie (wrong root in TestDeleteRecreateAccount / TestSelfDestructReceive
// / TestEIP161AccountRemoval, all of which SD a contract whose storage predates
// the block). Pass nil in unit tests that don't exercise pre-block storage.
// codePathRecoveryHashMismatch counts BAL codePath recoveries skipped because
// the recovered bytes didn't hash to the emitted codeHash; surfaced so the skip
// isn't silent.
var codePathRecoveryHashMismatch = metrics.GetOrCreateCounter("exec3_codepath_recovery_hash_mismatch")

func (writes *WriteSet) Normalize(vm *VersionMap, txIndex int, incarnation int, stateReader StateReader, domainStorageKeys func(addr accounts.Address) []accounts.StorageKey, emptyRemoval bool, isAura bool) *WriteSet {
	filtered := &WriteSet{}
	if writes == nil {
		return filtered
	}

	// sdStorageSlots returns the union of vm.StorageKeys (this batch) and
	// domainStorageKeys (committed before this batch), deduped — the complete
	// set of storage slots that must be DELETE'd when addr self-destructs.
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

	// Pre-scan for SD'd addresses. IBS.Selfdestruct emits 3 writes for the
	// SD'd account (IncarnationPath=preInc, SelfDestructPath=true, BalancePath=0).
	// If we forward all 3 to applyVersionedWrites, it sees d.balance != nil ||
	// d.incarnation != nil and routes into the "cleanup-before-recreate"
	// branch — which writes the account back with {Bal=0, Inc=preInc} encoding
	// instead of taking the pure-delete branch (DomainDel(Accounts)). The
	// account stays in sd.mem with non-zero incarnation, and a subsequent
	// block's CREATE2 at the same address sees a phantom existing account,
	// producing wrong execution / wrong trie root in TestRecreateAndRewind.
	// Drop the BalancePath/NoncePath/IncarnationPath/CodeHashPath writes for
	// SD'd addresses so applyVersionedWrites reaches the pure-delete branch.
	//
	// Two filters applied here:
	//   1. Validated-incarnation: mirror the `w.Version.Incarnation != incarnation`
	//      skip the SelfDestructPath case uses below — a stale SelfDestructPath=true
	//      from a non-validated incarnation must not mark the address as SD'd.
	//   2. Final-state: pre-Cancun a single tx can SELFDESTRUCT an address and then
	//      CREATE2-recreate at the same address; IBS emits SelfDestructPath=true
	//      (from Selfdestruct) followed later by SelfDestructPath=false (from
	//      CreateAccount, since the recreated object's selfdestructed flag is
	//      cleared). The address ends ALIVE, so its recreate-time account-field
	//      writes must survive — only mark sdSet when the LAST SelfDestructPath
	//      entry for the address (in emission order) is true. applyVersionedWrites
	//      already uses last-write-wins for d.selfDestruct, so this keeps the two
	//      in agreement. (EIP-6780 narrows this pattern post-Cancun but doesn't
	//      eliminate it; mainnet-rare, but cheap to get right.)
	sdSet := make(map[accounts.Address]bool)
	for addr, vw := range writes.SelfDestructs() {
		if vw.Version.Incarnation == incarnation && vw.Val {
			sdSet[addr] = true
		}
	}

	for h := range writes.AllHeaders() {
		// Drop account-field writes for SD'd addresses so applyVersionedWrites
		// takes the pure-delete branch instead of cleanup-before-recreate; drop
		// raw StoragePath writes too (the SelfDestructPath case re-emits an
		// explicit StoragePath=0 delete for every slot via sdStorageSlots).
		if sdSet[h.Address] {
			switch h.Path {
			case BalancePath, NoncePath, IncarnationPath, CodeHashPath, CodePath, StoragePath:
				continue
			}
		}
		switch h.Path {
		case StoragePath:
			// Only include writes from the current (validated) incarnation.
			if h.Version.Incarnation != incarnation {
				continue
			}
			sw, ok := writes.GetStorage(h.Address, h.Key)
			if !ok {
				continue
			}
			writeVal := sw.Val
			// If addr was self-destructed by an earlier TX in this block, its
			// storage was wiped — the effective baseline for any slot not
			// re-written since is 0, regardless of what the versionMap (prior
			// TX) or the domain (pre-block) still holds. The SD's per-slot
			// zeroing is only re-emitted into the calc's writeset below, never
			// flushed back to the versionMap, so without this a resurrect TX
			// that re-writes a slot to its pre-SD value is wrongly dropped as a
			// no-op (TestDeleteRecreateSlotsAcrossManyBlocks).
			sdTxIdx, sdOk := -1, false
			if v, sd, _ := vm.ReadSelfDestruct(h.Address, txIndex); sd.Status() == MVReadResultDone && v {
				sdTxIdx, sdOk = sd.Version().TxIndex, true
			}
			// No-op filter: compare against origin (what this TX would have read).
			// First check versionMap floor (prior TX's write in this block).
			// Then fall back to stateReader (pre-block value from domain).
			originVal, origin, originOK := vm.ReadStorage(h.Address, h.Key, txIndex)
			originValid := originOK && origin.Status() == MVReadResultDone &&
				!(sdOk && sdTxIdx > origin.Version().TxIndex)
			if originValid {
				if writeVal.Eq(&originVal) {
					continue // write-back same as prior TX's value — no-op
				}
			} else if sdOk {
				// SD'd earlier with no re-write since — baseline is 0.
				if writeVal.IsZero() {
					continue
				}
			} else if stateReader != nil {
				// SD-then-revival: latest SelfDestructPath may be false (a
				// later TxIdx revived), but the SD's per-slot DELETE cascade
				// already fixed the baseline at zero for any post-SD write.
				// History scan catches that; the sdOk latest-value read above
				// misses it. Narrower than an IncarnationPath probe: pure
				// CREATE (no prior SD=true) doesn't wipe pre-existing storage,
				// so its same-value SSTOREs still no-op against pre-block.
				if vm.AnyDoneSelfDestructEquals(h.Address, txIndex-1, true) {
					if writeVal.IsZero() {
						continue
					}
				} else {
					preVal, found, err := stateReader.ReadAccountStorage(h.Address, h.Key)
					if err == nil {
						if !found && writeVal.IsZero() {
							continue
						}
						if found && writeVal.Eq(&preVal) {
							continue
						}
					}
				}
			}
			filtered.SetStorage(h.Address, h.Key, sw)
		case BalancePath, NoncePath, IncarnationPath, CodeHashPath:
			// Account fields: prefer the versionMap's accumulated value; fall
			// back to the raw write when the map has none.
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
			// Only emit storage DELETE entries when the account was actually
			// self-destructed (val=true).
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
			// Code size is derived from the code bytes and isn't a domain field,
			// so it's intentionally not carried into the calc/apply write set (as
			// on serial). Cross-tx ReadCodeSize is served from the versionMap,
			// which the worker populates directly — independent of this pass.
		}
	}

	// For addresses that appear in the raw WriteSet but don't have account-level
	// writes in the output, emit account field entries. Serial's MakeWriteSet
	// always calls UpdateAccountData for every dirty object — the commitment
	// needs the full account  This covers:
	// - Addresses with only storage writes (no balance/nonce changes)
	// - Addresses whose storage writes were all filtered as no-ops
	//   (the object was still dirty in the IBS)
	//
	// Collect all addresses from the raw input (before filtering).
	allAddresses := make(map[accounts.Address]bool)
	for h := range writes.AllHeaders() {
		if h.Path != AddressPath {
			allAddresses[h.Address] = true
		}
	}

	// Track which fields each address already has in the output.
	addrFields := make(map[accounts.Address]map[AccountPath]bool)
	for h := range filtered.AllHeaders() {
		switch h.Path {
		case BalancePath, NoncePath, IncarnationPath, CodeHashPath:
			if addrFields[h.Address] == nil {
				addrFields[h.Address] = make(map[AccountPath]bool)
			}
			addrFields[h.Address][h.Path] = true
		}
	}

	for addr := range allAddresses {
		if sdSet[addr] {
			// Don't fill account fields for SD'd addresses — same rationale as
			// the sdSet drop in the filter loop above. Without this, the
			// stateReader fallback below would round-trip pre-SD account state
			// (Nonce, CodeHash, Incarnation) back into the writeset and undo
			// the SD when applyVersionedWrites picks the cleanup-then-recreate
			// branch.
			continue
		}
		ver := Version{TxIndex: txIndex, Incarnation: incarnation}
		fields := addrFields[addr]

		// If addr was self-destructed by an earlier TX in this block and this
		// TX re-creates it (it isn't in sdSet — this TX didn't re-destruct it),
		// missing account fields are the post-destruction defaults, NOT the
		// pre-SD values still sitting in the versionMap. IBS.Selfdestruct only
		// records SelfDestructPath/BalancePath/IncarnationPath, so a re-creation
		// via a plain value transfer (no CREATE) leaves the stale pre-SD nonce
		// and codeHash in the map — reading them back here resurrects a phantom
		// contract (wrong trie root: TestSelfDestructReceive, TestCVE2020_26265).
		// If a later TX between the SD and this one re-created addr via CREATE2,
		// vm.Read returns that recreate's SelfDestructPath=false, so we correctly
		// fall through to the normal versionMap lookup.
		sdEarlier := false
		if v, sd, _ := vm.ReadSelfDestruct(addr, txIndex); sd.Status() == MVReadResultDone && v {
			sdEarlier = true
		}

		// Only emit post-SD defaults when this TX created a new contract
		// (CREATE/CREATE2). A value-transfer resurrect (no CreateContractPath)
		// inherits the pre-SD account fields via the versionMap last-write-wins
		// chain — that matches GenerateChain's accumulate-across-txs behaviour
		// (no per-tx FinalizeTx). Forcing defaults here resets nonce/codeHash
		// against that canonical state (TestSelfDestructReceive).
		hasCreateContract := false
		if vw, ok := writes.GetCreateContract(addr); ok && vw.Val {
			hasCreateContract = true
		}

		// For each missing field, try versionMap then stateReader.
		for _, path := range []AccountPath{BalancePath, NoncePath, IncarnationPath, CodeHashPath} {
			if fields != nil && fields[path] {
				continue // already in output
			}
			if sdEarlier && hasCreateContract {
				SetAccountFieldZero(filtered, addr, path, ver)
				continue
			}
			if SetAccountFieldFromMap(filtered, vm, addr, path, ver, txIndex+1) {
				continue
			}
			// Fall back to stateReader for pre-block account
			if stateReader != nil {
				acc, err := stateReader.ReadAccountData(addr)
				if err == nil {
					// New account (acc == nil) — doesn't exist in domain yet.
					// SetAccountFieldFromAccount emits default values so the
					// commitment sees a full account (not a delete).
					SetAccountFieldFromAccount(filtered, addr, path, ver, acc)
				}
			}
		}
	}

	// CodePath must travel with CodeHashPath: the case above and the fill loop
	// recover an account's codeHash but not its code, so a validated writeset
	// lacking a fresh CodePath (e.g. a re-executing 7702 tx whose SetCode
	// short-circuited) would persist a codeHash with no code. Recover the code
	// (versionMap, else stateReader post-state) and re-emit CodePath, bounded to
	// 7702 designators so unchanged contract code isn't re-emitted. Forward-only:
	// it can't repair codeHash-no-code already collated into snapshots.
	codeInOutput := make(map[accounts.Address]bool)
	for addr := range filtered.Codes() {
		codeInOutput[addr] = true
	}
	codeHashInOutput := make(map[accounts.Address]accounts.CodeHash)
	for addr, vw := range filtered.CodeHashes() {
		codeHashInOutput[addr] = vw.Val
	}
	for addr, h := range codeHashInOutput {
		if h.IsEmpty() || codeInOutput[addr] || sdSet[addr] {
			continue
		}
		// Recover the code whose hash this tx emitted. Prefer the versionMap
		// (this batch's writes); on the SetCode short-circuit path — a
		// re-executing 7702 delegation whose code equals the already-committed
		// designator, so the validated incarnation writes no CodePath and the
		// prior incarnation's versionMap entry was invalidated on re-exec — the
		// versionMap holds nothing for this tx, so fall back to the post-state
		// via stateReader.
		var code []byte
		if c, _, ok := vm.ReadCode(addr, txIndex+1); ok {
			code = c.Bytes
		}
		if len(code) == 0 && stateReader != nil {
			if c, err := stateReader.ReadAccountCode(addr); err == nil {
				code = c
			}
		}
		// Gate recovery to 7702 designators: that SetCode short-circuit is the only
		// one that leaves uncommitted code without a CodePath. A regular deploy
		// writes CodePath with CodeHashPath; a CREATE2/unchanged redeploy already
		// has its code in CodeDomain. (Gating also never misattributes callee code.)
		if _, ok := types.ParseDelegation(code); !ok {
			continue
		}
		// The recovered bytes (ceiling versionMap read or stateReader post-state)
		// can race and disagree with the codeHash this tx emitted; only re-emit
		// when they hash to it, else we'd persist code that mismatches its hash.
		recovered := accounts.NewCode(code)
		if recovered.Hash.Value() != h.Value() {
			// Cannot repair: re-emitting would persist code mismatching its hash.
			// Skipping leaves codeHash-without-code, so signal rather than hide it.
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

	// EIP-161 empty account removal: if an account has Balance=0, Nonce=0,
	// and empty CodeHash, it should be deleted — not written as a regular
	// account with zero values. Serial's updateAccount checks Empty() and
	// calls DeleteAccount. We must match that behavior.
	//
	// The Nonce==0 check correctly excludes successful CREATE/CREATE2
	// (which sets Nonce to 1 per EIP-161) — including the
	// "constructor returned empty bytecode but wrote storage" case the
	// calc-side 3-way Deleted branch protects via incarnation tracking.
	// OOG-during-CREATE2 leaves Nonce==0 in the writeset, so it
	// correctly falls through to deletion here.
	type acctState struct {
		balance  uint256.Int
		nonce    uint64
		codeHash accounts.CodeHash
		hasBal   bool
		hasNonce bool
		hasCode  bool
	}
	acctStates := make(map[accounts.Address]*acctState)
	ensureAcctState := func(addr accounts.Address) *acctState {
		s := acctStates[addr]
		if s == nil {
			s = &acctState{}
			acctStates[addr] = s
		}
		return s
	}
	for addr, vw := range filtered.Balances() {
		s := ensureAcctState(addr)
		s.balance = vw.Val
		s.hasBal = true
	}
	for addr, vw := range filtered.Nonces() {
		s := ensureAcctState(addr)
		s.nonce = vw.Val
		s.hasNonce = true
	}
	for addr, vw := range filtered.CodeHashes() {
		s := ensureAcctState(addr)
		s.codeHash = vw.Val
		s.hasCode = true
	}

	// Check for empty accounts and replace with Delete. Only when EIP-161
	// (SpuriousDragon) is active — before that fork an empty account that's
	// merely touched (e.g. a 0-value transfer) is created and persists, so
	// converting it to a delete here would diverge from serial's trie root
	// (TestEIP161AccountRemoval block 1, pre-SpuriousDragon).
	emptyAddrs := make(map[accounts.Address]bool)
	for addr, s := range acctStates {
		if EIP161EmptyRemoval(emptyRemoval, isAura, addr) &&
			s.hasBal && s.hasNonce && s.hasCode &&
			s.balance.IsZero() && s.nonce == 0 && s.codeHash.IsEmpty() {
			emptyAddrs[addr] = true
		}
	}

	for addr := range emptyAddrs {
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

	return filtered
}
