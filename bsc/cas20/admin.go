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

package cas20

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/vm"
)

// DEFAULT_ADMIN is bytes32(0), so an unset role admin reads as DEFAULT_ADMIN.
var (
	roleDefaultAdmin = common.Hash{}
	roleMint         = keccakHash([]byte("MINT_ROLE"))
	roleBurn         = keccakHash([]byte("BURN_ROLE"))
	roleSeize        = keccakHash([]byte("SEIZE_ROLE"))
	rolePause        = keccakHash([]byte("PAUSE_ROLE"))
	roleUnpause      = keccakHash([]byte("UNPAUSE_ROLE"))
	roleMetadata     = keccakHash([]byte("METADATA_ROLE"))
)

var (
	selHasRole           = selector("hasRole(bytes32,address)")
	selGetRoleAdmin      = selector("getRoleAdmin(bytes32)")
	selGrantRole         = selector("grantRole(bytes32,address)")
	selRevokeRole        = selector("revokeRole(bytes32,address)")
	selRenounceRole      = selector("renounceRole(bytes32,address)")
	selSetRoleAdmin      = selector("setRoleAdmin(bytes32,bytes32)")
	selRenounceLastAdmin = selector("renounceLastAdmin()")

	selDefaultAdminRole = selector("DEFAULT_ADMIN_ROLE()")
	selMintRole         = selector("MINT_ROLE()")
	selBurnRole         = selector("BURN_ROLE()")
	selSeizeRole        = selector("SEIZE_ROLE()")
	selPauseRole        = selector("PAUSE_ROLE()")
	selUnpauseRole      = selector("UNPAUSE_ROLE()")
	selMetadataRole     = selector("METADATA_ROLE()")

	scopeTransferSender   = keccakHash([]byte("TRANSFER_SENDER_POLICY"))
	scopeTransferReceiver = keccakHash([]byte("TRANSFER_RECEIVER_POLICY"))
	scopeTransferExecutor = keccakHash([]byte("TRANSFER_EXECUTOR_POLICY"))
	scopeMintReceiver     = keccakHash([]byte("MINT_RECEIVER_POLICY"))
	scopeSeizeExempt      = keccakHash([]byte("SEIZE_EXEMPT_POLICY"))
	scopeSeizeReceiver    = keccakHash([]byte("SEIZE_RECEIVER_POLICY"))

	selTransferSenderScope   = selector("TRANSFER_SENDER_POLICY()")
	selTransferReceiverScope = selector("TRANSFER_RECEIVER_POLICY()")
	selTransferExecutorScope = selector("TRANSFER_EXECUTOR_POLICY()")
	selMintReceiverScope     = selector("MINT_RECEIVER_POLICY()")
	selSeizeExemptScope      = selector("SEIZE_EXEMPT_POLICY()")
	selSeizeReceiverScope    = selector("SEIZE_RECEIVER_POLICY()")
	selPolicyId              = selector("policyId(bytes32)")

	selIsPaused        = selector("isPaused(uint8)")
	selPause           = selector("pause(uint8[])")
	selUnpause         = selector("unpause(uint8[])")
	selMint            = selector("mint(address,uint256)")
	selBurn            = selector("burn(uint256)")
	selSeizeWithMemo   = selector("seizeWithMemo(address,address,uint256,bytes32)")
	selUpdateSupplyCap = selector("updateSupplyCap(uint256)")
	selUpdatePolicy    = selector("updatePolicy(bytes32,uint64)")

	cas20TopicRoleGranted      = eventTopic("RoleGranted(bytes32,address,address)")
	cas20TopicRoleRevoked      = eventTopic("RoleRevoked(bytes32,address,address)")
	cas20TopicRoleAdminChanged = eventTopic("RoleAdminChanged(bytes32,bytes32,bytes32)")
	cas20TopicSeized           = eventTopic("Seized(address,address,address,uint256)")

	cas20TopicLastAdminRenounced = eventTopic("LastAdminRenounced(address)")
	cas20TopicPolicyUpdated      = eventTopic("PolicyUpdated(bytes32,uint64,uint64)")
	cas20TopicPaused             = eventTopic("Paused(address,uint8[])")
	cas20TopicUnpaused           = eventTopic("Unpaused(address,uint8[])")
	cas20TopicSupplyCapUpdated   = eventTopic("SupplyCapUpdated(address,uint256,uint256)")
)

func (t cas20Token) dispatchAdmin(sel [4]byte, args []byte) (ret []byte, err error, ok bool) {
	switch sel {
	case selDefaultAdminRole:
		return hashBytes(roleDefaultAdmin), nil, true
	case selMintRole:
		return hashBytes(roleMint), nil, true
	case selBurnRole:
		return hashBytes(roleBurn), nil, true
	case selSeizeRole:
		return hashBytes(roleSeize), nil, true
	case selPauseRole:
		return hashBytes(rolePause), nil, true
	case selUnpauseRole:
		return hashBytes(roleUnpause), nil, true
	case selMetadataRole:
		return hashBytes(roleMetadata), nil, true

	case selHasRole:
		role, err := readWord(args, 0)
		if err != nil {
			return nil, err, true
		}
		acct, err := readAddress(args, 1)
		if err != nil {
			return nil, err, true
		}
		return encBool(t.s.hasRole(role, acct)), nil, true
	case selGetRoleAdmin:
		role, err := readWord(args, 0)
		if err != nil {
			return nil, err, true
		}
		admin := t.s.roleAdmin(role)
		return admin[:], nil, true

	case selGrantRole:
		role, acct, err := readRoleAccount(args)
		if err != nil {
			return nil, err, true
		}
		return nil, t.grantRole(role, acct), true
	case selRevokeRole:
		role, acct, err := readRoleAccount(args)
		if err != nil {
			return nil, err, true
		}
		return nil, t.revokeRole(role, acct), true
	case selRenounceRole:
		role, confirm, err := readRoleAccount(args)
		if err != nil {
			return nil, err, true
		}
		return nil, t.renounceRole(role, confirm), true
	case selSetRoleAdmin:
		role, err := readWord(args, 0)
		if err != nil {
			return nil, err, true
		}
		newAdmin, err := readWord(args, 1)
		if err != nil {
			return nil, err, true
		}
		return nil, t.setRoleAdmin(role, newAdmin), true
	case selRenounceLastAdmin:
		return nil, t.renounceLastAdmin(), true

	case selIsPaused:
		w, err := readWord(args, 0)
		if err != nil {
			return nil, err, true
		}
		if !isEnumWord(w, cas20PauseSeize) {
			return nil, vm.ErrExecutionReverted, true
		}
		return encBool(t.isPaused(uint(w[31]))), nil, true
	case selPause:
		return nil, t.setPause(args, true), true
	case selUnpause:
		return nil, t.setPause(args, false), true

	case selMint:
		to, err := readAddress(args, 0)
		if err != nil {
			return nil, err, true
		}
		amount, err := readU256(args, 1)
		if err != nil {
			return nil, err, true
		}
		return nil, t.mint(to, amount), true
	case selBurn:
		amount, err := readU256(args, 0)
		if err != nil {
			return nil, err, true
		}
		return nil, t.burn(t.ctx.Caller, amount), true
	case selSeizeWithMemo:
		from, err := readAddress(args, 0)
		if err != nil {
			return nil, err, true
		}
		to, err := readAddress(args, 1)
		if err != nil {
			return nil, err, true
		}
		amount, err := readU256(args, 2)
		if err != nil {
			return nil, err, true
		}
		memo, err := readWord(args, 3)
		if err != nil {
			return nil, err, true
		}
		if err := t.seizeWithMemo(from, to, amount, memo); err != nil {
			return nil, err, true
		}
		return encBool(true), nil, true

	case selUpdateSupplyCap:
		cap, err := readU256(args, 0)
		if err != nil {
			return nil, err, true
		}
		return nil, t.updateSupplyCap(cap), true
	case selUpdatePolicy:
		scope, err := readWord(args, 0)
		if err != nil {
			return nil, err, true
		}
		id, err := readU64(args, 1)
		if err != nil {
			return nil, err, true
		}
		return nil, t.updatePolicy(scope, id), true
	case selTransferSenderScope:
		return hashBytes(scopeTransferSender), nil, true
	case selTransferReceiverScope:
		return hashBytes(scopeTransferReceiver), nil, true
	case selTransferExecutorScope:
		return hashBytes(scopeTransferExecutor), nil, true
	case selMintReceiverScope:
		return hashBytes(scopeMintReceiver), nil, true
	case selSeizeExemptScope:
		return hashBytes(scopeSeizeExempt), nil, true
	case selSeizeReceiverScope:
		return hashBytes(scopeSeizeReceiver), nil, true
	case selPolicyId:
		scope, err := readWord(args, 0)
		if err != nil {
			return nil, err, true
		}
		id, ok := t.policyIdByScope(scope)
		if !ok {
			return nil, revCAS20("UnsupportedPolicyType(bytes32)", errSelUnsupportedScope, scope), true
		}
		return encU256(uint256.NewInt(id)), nil, true
	}
	return nil, nil, false
}

// One table for policyId and updatePolicy, so the two cannot disagree on a lane.
var cas20PolicyLanes = map[common.Hash]struct {
	slot    uint64
	byteOff uint
}{
	scopeTransferSender:   {cas20SlotTransferPolicies, cas20OffTransferSender},
	scopeTransferReceiver: {cas20SlotTransferPolicies, cas20OffTransferReceiver},
	scopeTransferExecutor: {cas20SlotTransferPolicies, cas20OffTransferExecutor},
	scopeMintReceiver:     {cas20SlotMintPolicy, cas20OffMintReceiver},
	scopeSeizeExempt:      {cas20SlotSeizePolicies, cas20OffSeizeExempt},
	scopeSeizeReceiver:    {cas20SlotSeizePolicies, cas20OffSeizeReceiver},
}

func (t cas20Token) policyIdByScope(scope common.Hash) (uint64, bool) {
	lane, ok := cas20PolicyLanes[scope]
	if !ok {
		return 0, false
	}
	return t.s.getPackedU64(lane.slot, lane.byteOff), true
}

func readRoleAccount(args []byte) (common.Hash, common.Address, error) {
	role, err := readWord(args, 0)
	if err != nil {
		return common.Hash{}, common.Address{}, err
	}
	acct, err := readAddress(args, 1)
	if err != nil {
		return common.Hash{}, common.Address{}, err
	}
	return role, acct, nil
}

// --- RoleManaged ------------------------------------------------------------

func (t cas20Token) grantRole(role common.Hash, account common.Address) error {
	if err := t.ensureRoleMutable(role); err != nil {
		return err
	}
	if !t.s.hasRole(role, account) {
		t.s.setRole(role, account, true)
		if role == roleDefaultAdmin {
			t.s.setAdminCount(new(uint256.Int).AddUint64(t.s.adminCount(), 1))
		}
		if !t.ctx.AddLog([]common.Hash{cas20TopicRoleGranted, role, addrKey(account), addrKey(t.ctx.Caller)}, nil) {
			return vm.ErrOutOfGas
		}
	}
	return nil
}

func (t cas20Token) revokeRole(role common.Hash, account common.Address) error {
	if err := t.ensureRoleMutable(role); err != nil {
		return err
	}
	if role == roleDefaultAdmin && t.s.hasRole(role, account) && t.s.adminCount().Eq(uint256.NewInt(1)) {
		return revCAS20("LastAdminCannotRenounce()", errSelLastAdminRenounce)
	}
	if !t.removeRole(role, account) {
		return vm.ErrOutOfGas
	}
	return nil
}

func (t cas20Token) renounceRole(role common.Hash, confirmation common.Address) error {
	if t.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	if confirmation != t.ctx.Caller {
		return revCAS20("AccessControlBadConfirmation()", errSelACBadConfirmation)
	}
	if role == roleDefaultAdmin && t.s.hasRole(role, t.ctx.Caller) && t.s.adminCount().Eq(uint256.NewInt(1)) {
		return revCAS20("LastAdminCannotRenounce()", errSelLastAdminRenounce)
	}
	if !t.removeRole(role, t.ctx.Caller) {
		return vm.ErrOutOfGas
	}
	return nil
}

func (t cas20Token) renounceLastAdmin() error {
	if t.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	// A stranger is unauthorized; NotSoleAdmin is for an admin who is not the last.
	if !t.s.hasRole(roleDefaultAdmin, t.ctx.Caller) {
		return revCAS20("AccessControlUnauthorizedAccount(address,bytes32)", errSelACUnauthorized,
			addrKey(t.ctx.Caller), roleDefaultAdmin)
	}
	if !t.s.adminCount().Eq(uint256.NewInt(1)) {
		return revCAS20("NotSoleAdmin()", errSelNotSoleAdmin)
	}
	t.s.setRole(roleDefaultAdmin, t.ctx.Caller, false)
	t.s.setAdminCount(new(uint256.Int))
	t.ctx.adminRenounced = true
	if !t.ctx.AddLog([]common.Hash{cas20TopicRoleRevoked, roleDefaultAdmin, addrKey(t.ctx.Caller), addrKey(t.ctx.Caller)}, nil) {
		return vm.ErrOutOfGas
	}
	if !t.ctx.AddLog([]common.Hash{cas20TopicLastAdminRenounced, addrKey(t.ctx.Caller)}, nil) {
		return vm.ErrOutOfGas
	}
	return nil
}

func (t cas20Token) setRoleAdmin(role, newAdminRole common.Hash) error {
	if err := t.ensureRoleMutable(role); err != nil {
		return err
	}
	prev := t.s.roleAdmin(role)
	t.s.setRoleAdmin(role, newAdminRole)
	if !t.ctx.AddLog([]common.Hash{cas20TopicRoleAdminChanged, role, prev, newAdminRole}, nil) {
		return vm.ErrOutOfGas
	}
	return nil
}

func (t cas20Token) removeRole(role common.Hash, account common.Address) bool {
	if !t.s.hasRole(role, account) {
		return true
	}
	t.s.setRole(role, account, false)
	if role == roleDefaultAdmin {
		t.s.setAdminCount(new(uint256.Int).SubUint64(t.s.adminCount(), 1))
	}
	return t.ctx.AddLog([]common.Hash{cas20TopicRoleRevoked, role, addrKey(account), addrKey(t.ctx.Caller)}, nil)
}

func (t cas20Token) ensureRole(role common.Hash) error {
	if t.privileged || t.s.hasRole(role, t.ctx.Caller) {
		return nil
	}
	return revCAS20("AccessControlUnauthorizedAccount(address,bytes32)", errSelACUnauthorized,
		addrKey(t.ctx.Caller), role)
}

func (t cas20Token) ensureRoleMutable(role common.Hash) error {
	if t.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	// The bootstrap window may configure an ownerless token, but not one this frame
	// has just renounced: that transition is permanent.
	if (!t.privileged || t.ctx.adminRenounced) && t.s.adminCount().IsZero() {
		return revCAS20("AccessControlUnauthorizedAccount(address,bytes32)", errSelACUnauthorized,
			addrKey(t.ctx.Caller), t.s.roleAdmin(role))
	}
	if !t.privileged && !t.s.hasRole(t.s.roleAdmin(role), t.ctx.Caller) {
		return revCAS20("AccessControlUnauthorizedAccount(address,bytes32)", errSelACUnauthorized,
			addrKey(t.ctx.Caller), t.s.roleAdmin(role))
	}
	return nil
}

// --- Pausable ---------------------------------------------------------------

func (t cas20Token) setPause(args []byte, on bool) error {
	features, err := readUint8Array(args)
	if err != nil {
		return err
	}
	if t.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	role := rolePause
	if !on {
		role = roleUnpause
	}
	if err := t.ensureRole(role); err != nil {
		return err
	}
	if len(features) == 0 {
		return revCAS20("EmptyFeatureSet()", errSelEmptyFeatureSet)
	}
	// Read before the caller-sized loop: an unpaid read must be out-of-gas, not an empty mask.
	p, ok := t.s.pausedChecked()
	if !ok {
		return vm.ErrOutOfGas
	}
	words := make([]common.Hash, len(features))
	for i, f := range features {
		if uint(f) > cas20PauseSeize {
			return vm.ErrExecutionReverted
		}
		words[i] = wU8(f)
		mask := new(uint256.Int).Lsh(uint256.NewInt(1), uint(f))
		if on {
			p.Or(p, mask)
		} else {
			p.And(p, mask.Not(mask))
		}
	}
	t.s.setPaused(p)
	topic := cas20TopicPaused
	if !on {
		topic = cas20TopicUnpaused
	}
	if !t.ctx.AddLog([]common.Hash{topic, addrKey(t.ctx.Caller)}, encodeTuple(abiWordArray(words))) {
		return vm.ErrOutOfGas
	}
	return nil
}

// --- Mintable / Burnable ----------------------------------------------------

func (t cas20Token) mint(to common.Address, amount *uint256.Int) error {
	if t.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	if t.isPaused(cas20PauseMint) {
		return revCAS20("ContractPaused(uint8)", errSelContractPaused, wU8(cas20PauseMint))
	}
	if err := t.ensureRole(roleMint); err != nil {
		return err
	}
	return t.mintCore(to, amount)
}

// mintCore assumes the caller has checked pause and role.
func (t cas20Token) mintCore(to common.Address, amount *uint256.Int) error {
	if to == (common.Address{}) {
		return revCAS20("InvalidReceiver(address)", errSelInvalidReceiver, addrKey(to))
	}
	// Enforced even during the privileged bootstrap.
	mintReceiver := t.s.mintReceiverPolicy()
	if !t.policyAllows(mintReceiver, to) {
		return revCAS20("PolicyForbids(bytes32,uint64)", errSelPolicyForbids,
			scopeMintReceiver, wU64(mintReceiver))
	}
	supply := t.s.totalSupply()
	newSupply := new(uint256.Int).Add(supply, amount)
	if newSupply.Lt(supply) {
		return revPanic(0x11)
	}
	if cap := t.s.supplyCap(); newSupply.Gt(cap) {
		return revCAS20("SupplyCapExceeded(uint256,uint256)", errSelSupplyCapExceeded,
			wU256(cap), wU256(newSupply))
	}
	toSlot := t.s.balanceSlot(to)
	t.s.setU256At(toSlot, new(uint256.Int).Add(t.s.getU256At(toSlot), amount))
	t.s.setTotalSupply(newSupply)
	if !t.emit(cas20TopicTransfer, common.Address{}, to, amount) {
		return vm.ErrOutOfGas
	}
	return nil
}

func (t cas20Token) burn(from common.Address, amount *uint256.Int) error {
	if t.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	if t.isPaused(cas20PauseBurn) {
		return revCAS20("ContractPaused(uint8)", errSelContractPaused, wU8(cas20PauseBurn))
	}
	if err := t.ensureRole(roleBurn); err != nil {
		return err
	}
	fromSlot := t.s.balanceSlot(from)
	bal := t.s.getU256At(fromSlot)
	if bal.Lt(amount) {
		return revCAS20("InsufficientBalance(address,uint256,uint256)", errSelInsufficientBalance,
			addrKey(from), wU256(bal), wU256(amount))
	}
	t.s.setU256At(fromSlot, new(uint256.Int).Sub(bal, amount))
	supply := t.s.totalSupply()
	if supply.Lt(amount) {
		return revPanic(0x11)
	}
	t.s.setTotalSupply(new(uint256.Int).Sub(supply, amount))
	if !t.emit(cas20TopicTransfer, from, common.Address{}, amount) {
		return vm.ErrOutOfGas
	}
	return nil
}

// Accounts authorized by SEIZE_EXEMPT are exempt from seizure.
func (t cas20Token) seizeWithMemo(from, to common.Address, amount *uint256.Int, memo common.Hash) error {
	if t.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	if t.isPaused(cas20PauseSeize) {
		return revCAS20("ContractPaused(uint8)", errSelContractPaused, wU8(cas20PauseSeize))
	}
	if err := t.ensureRole(roleSeize); err != nil {
		return err
	}
	// A self-seize is a no-op that would still emit Seized; a zero source is a
	// malformed argument, not an empty balance.
	if to == (common.Address{}) || from == to {
		return revCAS20("InvalidReceiver(address)", errSelInvalidReceiver, addrKey(to))
	}
	if from == (common.Address{}) {
		return revCAS20("InvalidSender(address)", errSelInvalidSender, addrKey(from))
	}
	seizeExempt, seizeReceiver := t.s.seizePolicies()
	if t.policyAllows(seizeExempt, from) {
		return revCAS20("AccountNotSeizable(address)", errSelAccountNotSeizable, addrKey(from))
	}
	if !t.policyAllows(seizeReceiver, to) {
		return revCAS20("PolicyForbids(bytes32,uint64)", errSelPolicyForbids,
			scopeSeizeReceiver, wU64(seizeReceiver))
	}
	fromSlot := t.s.balanceSlot(from)
	bal := t.s.getU256At(fromSlot)
	if bal.Lt(amount) {
		return revCAS20("InsufficientBalance(address,uint256,uint256)", errSelInsufficientBalance,
			addrKey(from), wU256(bal), wU256(amount))
	}
	t.s.setU256At(fromSlot, new(uint256.Int).Sub(bal, amount))
	toSlot := t.s.balanceSlot(to)
	t.s.setU256At(toSlot, new(uint256.Int).Add(t.s.getU256At(toSlot), amount))
	if !t.emit(cas20TopicTransfer, from, to, amount) {
		return vm.ErrOutOfGas
	}
	if !t.emitMemo(memo) {
		return vm.ErrOutOfGas
	}
	ab := amount.Bytes32()
	if !t.ctx.AddLog([]common.Hash{cas20TopicSeized, addrKey(t.ctx.Caller), addrKey(from), addrKey(to)}, ab[:]) {
		return vm.ErrOutOfGas
	}
	return nil
}

// --- Configurable (subset) --------------------------------------------------

func (t cas20Token) updateSupplyCap(newCap *uint256.Int) error {
	if t.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	if err := t.ensureRole(roleDefaultAdmin); err != nil {
		return err
	}
	if supply := t.s.totalSupply(); newCap.Lt(supply) || newCap.Gt(cas20NoSupplyCap) {
		return revCAS20("InvalidSupplyCap(uint256,uint256)", errSelInvalidSupplyCap,
			wU256(supply), wU256(newCap))
	}
	previous := t.s.supplyCap()
	t.s.setSupplyCap(newCap)
	if !t.ctx.AddLog([]common.Hash{cas20TopicSupplyCapUpdated, addrKey(t.ctx.Caller)},
		append(hashBytes(wU256(previous)), hashBytes(wU256(newCap))...)) {
		return vm.ErrOutOfGas
	}
	return nil
}

// updatePolicy rejects a never-created id so the read path's empty-set tolerance
// cannot be bound on purpose.
func (t cas20Token) updatePolicy(scope common.Hash, id uint64) error {
	if t.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	if err := t.ensureRole(roleDefaultAdmin); err != nil {
		return err
	}
	// Scope before id: an unknown scope is reported as such whatever id accompanies it.
	lane, ok := cas20PolicyLanes[scope]
	if !ok {
		return revCAS20("UnsupportedPolicyType(bytes32)", errSelUnsupportedScope, scope)
	}
	read := func() uint64 { return t.s.getPackedU64(lane.slot, lane.byteOff) }
	write := func(id uint64) { t.s.setPackedU64(lane.slot, lane.byteOff, id) }

	if !newPolicyReg(t.ctx).policyExists(id) {
		return revCAS20("PolicyNotFound(uint64)", errSelPolicyNotFoundID, wU64(id))
	}
	previous := read()
	write(id)
	if !t.ctx.AddLog([]common.Hash{cas20TopicPolicyUpdated, scope},
		append(hashBytes(wU64(previous)), hashBytes(wU64(id))...)) {
		return vm.ErrOutOfGas
	}
	return nil
}
