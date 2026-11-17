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

// PolicyRegistry: a chain-shared allow/block-list registry. A policy is a set
// of addresses plus a type, referenced by tokens via a self-describing uint64
// id (high byte = type, low 56 bits = global counter). Reads never revert (they
// sit on every transfer's hot path); writes are admin-gated.

const cas20PolicyNamespace = "bsc.policy_registry"

const (
	cas20PolicyBlocklist = 0
	cas20PolicyAllowlist = 1
	cas20PolicyUnion     = 2 // authorized by ANY child
	cas20PolicyIntersect = 3 // authorized by EVERY child
	cas20PolicyBatchMax  = 64

	cas20CompositeMinChildren = 2
	cas20CompositeMaxChildren = 4
	cas20PolicyFirstID        = 2 // counters 0 and 1 belong to the two sentinels

	cas20PolicyAlwaysAllow = 0                 // blocklist type, empty -> allow all
	cas20PolicyAlwaysBlock = uint64(1)<<56 | 1 // allowlist type, empty -> block all

	cas20PolicyCounterMax = uint64(1)<<56 - 1
)

// Storage layout. Slots are append-only across forks.
const (
	polSlotPolicies      = 0 // mapping(uint64 => packed word)
	polSlotMembers       = 1 // mapping(uint64 => mapping(address => bool))
	polSlotPendingAdmins = 2 // mapping(uint64 => address)
	polSlotCounter       = 3 // uint64
	polSlotChildren      = 4 // mapping(uint64 => uint64[])
)

// Existence and admin share one word: bit 255 exists, bits 159:0 the admin.
var polExistsBit = new(uint256.Int).Lsh(uint256.NewInt(1), 255)

func packPolicy(admin common.Address) common.Hash {
	w := new(uint256.Int).SetBytes(admin[:])
	return w.Or(w, polExistsBit).Bytes32()
}

func polWordExists(w common.Hash) bool          { return w[0]&0x80 != 0 }
func polWordAdmin(w common.Hash) common.Address { return common.BytesToAddress(w[12:]) }

func polIDType(id uint64) byte { return byte(id >> 56) }

func polIDWellFormed(id uint64) bool { return polIDType(id) <= cas20PolicyIntersect }

func isSentinelPolicy(id uint64) bool {
	return id == cas20PolicyAlwaysAllow || id == cas20PolicyAlwaysBlock
}

var cas20PolicyRoot = erc7201Root(cas20PolicyNamespace)

var (
	selCreatePolicy             = selector("createPolicy(address,uint8)")
	selCreatePolicyWithAccounts = selector("createPolicyWithAccounts(address,uint8,address[])")
	selCreateComposite          = selector("createCompositePolicy(address,uint8,uint64[])")
	selUpdateComposite          = selector("updateComposite(uint64,uint64[])")
	selCompositeChildIds        = selector("compositePolicyChildIds(uint64)")
	selMinCompositeChildren     = selector("MIN_COMPOSITE_CHILD_POLICIES()")
	selMaxCompositeChildren     = selector("MAX_COMPOSITE_CHILD_POLICIES()")
	selUpdateAllowlist          = selector("updateAllowlist(uint64,bool,address[])")
	selUpdateBlocklist          = selector("updateBlocklist(uint64,bool,address[])")
	selStageUpdateAdmin         = selector("stageUpdateAdmin(uint64,address)")
	selFinalizeUpdateAdmin      = selector("finalizeUpdateAdmin(uint64)")
	selRenounceAdmin            = selector("renounceAdmin(uint64)")
	selIsAuthorized             = selector("isAuthorized(uint64,address)")
	selPolicyExists             = selector("policyExists(uint64)")
	selPolicyAdmin              = selector("policyAdmin(uint64)")
	selPendingPolicyAdmin       = selector("pendingPolicyAdmin(uint64)")

	cas20TopicPolicyCreated      = eventTopic("PolicyCreated(uint64,address,uint8)")
	cas20TopicPolicyAdminStaged  = eventTopic("PolicyAdminStaged(uint64,address,address)")
	cas20TopicPolicyAdminUpdated = eventTopic("PolicyAdminUpdated(uint64,address,address)")
	cas20TopicCompositeUpdated   = eventTopic("CompositePolicyUpdated(uint64,address,uint64[])")
	cas20TopicAllowlistUpdated   = eventTopic("AllowlistUpdated(uint64,address,bool,address[])")
	cas20TopicBlocklistUpdated   = eventTopic("BlocklistUpdated(uint64,address,bool,address[])")
)

// One event for creation, handover and renunciation: an admin history is one filter.
func emitPolicyAdminUpdated(ctx *PrecompileContext, id uint64, previous, next common.Address) bool {
	return ctx.AddLog([]common.Hash{
		cas20TopicPolicyAdminUpdated, idKey(id), addrKey(previous), addrKey(next),
	}, nil)
}

func emitMembersUpdated(ctx *PrecompileContext, ptype byte, id uint64, updater common.Address, included bool, accounts []common.Hash) bool {
	topic := cas20TopicBlocklistUpdated
	if ptype == cas20PolicyAllowlist {
		topic = cas20TopicAllowlistUpdated
	}
	return ctx.AddLog(
		[]common.Hash{topic, idKey(id), addrKey(updater)},
		encodeTuple(abiWord(boolWord(included)), abiWordArray(accounts)),
	)
}

func boolWord(b bool) common.Hash {
	var w common.Hash
	if b {
		w[31] = 1
	}
	return w
}

// policyReg is a gas-metered view over the registry's storage.
type policyReg struct{ s cas20Storage }

func newPolicyReg(ctx *PrecompileContext) policyReg {
	return policyReg{s: newMeteredCAS20StorageAt(ctx, CAS20PolicyRegistryAddress)}
}

func polSlot(offset uint64) common.Hash { return offsetSlot(cas20PolicyRoot, offset) }

func idKey(id uint64) common.Hash { return uint256.NewInt(id).Bytes32() }

func isEnumWord(w common.Hash, max byte) bool {
	return wordFitsIn(w, 1) && w[31] <= max
}

func (p policyReg) counter() uint64 {
	return new(uint256.Int).SetBytes(hashBytes(p.s.getWord(polSlot(polSlotCounter)))).Uint64()
}

func (p policyReg) setCounter(v uint64) {
	p.s.setWord(polSlot(polSlotCounter), uint256.NewInt(v).Bytes32())
}

func (p policyReg) policyWord(id uint64) common.Hash {
	return p.s.getWord(p.s.mapSlot(polSlot(polSlotPolicies), idKey(id)))
}

func (p policyReg) setPolicyAdmin(id uint64, a common.Address) {
	p.s.setWord(p.s.mapSlot(polSlot(polSlotPolicies), idKey(id)), packPolicy(a))
}

func (p policyReg) exists(id uint64) bool          { return polWordExists(p.policyWord(id)) }
func (p policyReg) admin(id uint64) common.Address { return polWordAdmin(p.policyWord(id)) }

func (p policyReg) pending(id uint64) common.Address {
	return common.BytesToAddress(hashBytes(p.s.getWord(p.s.mapSlot(polSlot(polSlotPendingAdmins), idKey(id)))))
}

func (p policyReg) setPending(id uint64, a common.Address) {
	p.s.setWord(p.s.mapSlot(polSlot(polSlotPendingAdmins), idKey(id)), addrKey(a))
}

func (p policyReg) member(id uint64, account common.Address) bool {
	inner := p.s.mapSlot(polSlot(polSlotMembers), idKey(id))
	return p.s.getWord(p.s.mapSlot(inner, addrKey(account))) != (common.Hash{})
}

func (p policyReg) setMember(id uint64, account common.Address, in bool) {
	inner := p.s.mapSlot(polSlot(polSlotMembers), idKey(id))
	var v common.Hash
	if in {
		v[31] = 1
	}
	p.s.setWord(p.s.mapSlot(inner, addrKey(account)), v)
}

// isAuthorized never reverts: it sits on every transfer's path.
func (p policyReg) isAuthorized(id uint64, account common.Address) bool {
	if !polIDWellFormed(id) {
		return false
	}
	switch id {
	case cas20PolicyAlwaysAllow:
		return true
	case cas20PolicyAlwaysBlock:
		return false
	}
	switch polIDType(id) {
	case cas20PolicyUnion:
		for _, child := range p.children(id) {
			if p.isAuthorized(child, account) {
				return true
			}
		}
		return false
	case cas20PolicyIntersect:
		for _, child := range p.children(id) {
			if !p.isAuthorized(child, account) {
				return false
			}
		}
		return true
	}
	member := p.member(id, account)
	if polIDType(id) == cas20PolicyAllowlist {
		return member
	}
	return !member
}

// polIsComposite reports whether an id names a UNION or INTERSECT policy.
func polIsComposite(id uint64) bool {
	t := polIDType(id)
	return t == cas20PolicyUnion || t == cas20PolicyIntersect
}

func (p policyReg) childrenSlot(id uint64) common.Hash {
	return p.s.mapSlot(polSlot(polSlotChildren), idKey(id))
}

func (p policyReg) children(id uint64) []uint64 {
	slot := p.childrenSlot(id)
	n := new(uint256.Int).SetBytes(hashBytes(p.s.getWord(slot))).Uint64()
	if n == 0 || n > cas20CompositeMaxChildren {
		return nil
	}
	base := p.s.stringDataRoot(slot)
	out := make([]uint64, 0, n)
	for i := range n {
		if p.s.ctx != nil && p.s.ctx.OutOfGas() {
			return nil
		}
		w := p.s.getWord(new(uint256.Int).AddUint64(base, i/4).Bytes32())
		// Four uint64 lanes per word, LSB-first as Solidity packs them.
		lane := uint((i % 4) * 8)
		out = append(out, new(uint256.Int).Rsh(new(uint256.Int).SetBytes(w[:]), lane*8).Uint64())
	}
	return out
}

func (p policyReg) setChildren(id uint64, kids []uint64) {
	slot := p.childrenSlot(id)
	p.s.setWord(slot, uint256.NewInt(uint64(len(kids))).Bytes32())
	base := p.s.stringDataRoot(slot)
	// Words are rebuilt whole and the loop runs to the count the *maximum* set needs,
	// so a shrink's orphaned tail is cleared as Solidity's array assignment clears it;
	// otherwise the state root would diverge though every read agreed.
	maxWords := (cas20CompositeMaxChildren + 3) / 4
	for w := range maxWords {
		packed := new(uint256.Int)
		for lane := 0; lane < 4 && w*4+lane < len(kids); lane++ {
			packed.Or(packed, new(uint256.Int).Lsh(uint256.NewInt(kids[w*4+lane]), uint(lane)*64))
		}
		slotW := new(uint256.Int).AddUint64(base, uint64(w)).Bytes32()
		if packed.IsZero() && w*4 >= len(kids) {
			// Only clear a word that holds something: a fresh composite pays for no empty slots.
			if p.s.getWord(slotW) == (common.Hash{}) {
				continue
			}
		}
		p.s.setWord(slotW, packed.Bytes32())
	}
}

func (p policyReg) policyExists(id uint64) bool {
	if !polIDWellFormed(id) {
		return false
	}
	if isSentinelPolicy(id) {
		return true
	}
	return p.exists(id)
}

func (p policyReg) policyAdminOf(id uint64) common.Address {
	if !polIDWellFormed(id) {
		return common.Address{}
	}
	w := p.policyWord(id)
	if !polWordExists(w) {
		return common.Address{}
	}
	return polWordAdmin(w)
}

func (p policyReg) pendingPolicyAdminOf(id uint64) common.Address {
	if !polIDWellFormed(id) || isSentinelPolicy(id) {
		return common.Address{}
	}
	return p.pending(id)
}

// ensureInitialized gates on the counter, not the sentinel words, so a harness
// that pre-warms the account's bytecode cannot make the seeding skip.
func (p policyReg) ensureInitialized() uint64 {
	c := p.counter()
	if c >= cas20PolicyFirstID {
		return c
	}
	p.setPolicyAdmin(cas20PolicyAlwaysAllow, common.Address{})
	p.setPolicyAdmin(cas20PolicyAlwaysBlock, common.Address{})
	p.setCounter(cas20PolicyFirstID)
	return cas20PolicyFirstID
}

type cas20PolicyPrecompile struct{ cas20StatefulBase }

func (p *cas20PolicyPrecompile) Name() string { return "CAS20PolicyRegistry" }

func (p *cas20PolicyPrecompile) RunStateful(ctx *PrecompileContext, input []byte) ([]byte, error) {
	if err := cas20EnterCall(ctx, input); err != nil {
		return finishCAS20(nil, err)
	}
	ret, err := runCAS20Policy(ctx, input)
	return finishCAS20Metered(ctx, ret, err)
}

func runCAS20Policy(ctx *PrecompileContext, input []byte) ([]byte, error) {
	if len(input) < 4 {
		return nil, vm.ErrExecutionReverted
	}
	var sel [4]byte
	copy(sel[:], input[:4])
	args := input[4:]
	reg := newPolicyReg(ctx)

	switch sel {
	// reads (allowed in read-only frames, never revert on lookup)
	case selIsAuthorized:
		id, err := readU64(args, 0)
		if err != nil {
			return nil, err
		}
		acct, err := readAddress(args, 1)
		if err != nil {
			return nil, err
		}
		return encBool(reg.isAuthorized(id, acct)), nil
	case selPolicyExists:
		id, err := readU64(args, 0)
		if err != nil {
			return nil, err
		}
		return encBool(reg.policyExists(id)), nil
	case selPolicyAdmin:
		id, err := readU64(args, 0)
		if err != nil {
			return nil, err
		}
		return hashBytes(addrKey(reg.policyAdminOf(id))), nil
	case selPendingPolicyAdmin:
		id, err := readU64(args, 0)
		if err != nil {
			return nil, err
		}
		return hashBytes(addrKey(reg.pendingPolicyAdminOf(id))), nil
	case selMinCompositeChildren:
		return encU256(uint256.NewInt(cas20CompositeMinChildren)), nil
	case selMaxCompositeChildren:
		return encU256(uint256.NewInt(cas20CompositeMaxChildren)), nil
	case selCompositeChildIds:
		id, err := readU64(args, 0)
		if err != nil {
			return nil, err
		}
		kids := reg.children(id)
		words := make([]common.Hash, len(kids))
		for i, k := range kids {
			words[i] = wU64(k)
		}
		return encodeTuple(abiWordArray(words)), nil
	}

	// Writes: unknown selector, then static frame, then inactive feature, before
	// decoding arguments. The order is consensus-visible.
	switch sel {
	case selCreatePolicy, selCreatePolicyWithAccounts, selUpdateAllowlist,
		selUpdateBlocklist, selStageUpdateAdmin, selFinalizeUpdateAdmin, selRenounceAdmin,
		selCreateComposite, selUpdateComposite:
		if ctx.ReadOnly {
			return nil, vm.ErrWriteProtection
		}
		if err := ensureFeatureActivated(ctx, featurePolicyRegistry); err != nil {
			return nil, err
		}
	default:
		return nil, vm.ErrExecutionReverted // unknown selector
	}

	switch sel {
	case selCreatePolicy:
		return createPolicy(ctx, reg, args, false)
	case selCreatePolicyWithAccounts:
		return createPolicy(ctx, reg, args, true)
	case selCreateComposite:
		return createCompositePolicy(ctx, reg, args)
	case selUpdateComposite:
		return nil, updateComposite(ctx, reg, args)
	case selUpdateAllowlist:
		return nil, updateMembers(ctx, reg, args, cas20PolicyAllowlist)
	case selUpdateBlocklist:
		return nil, updateMembers(ctx, reg, args, cas20PolicyBlocklist)
	case selStageUpdateAdmin:
		return nil, stageUpdateAdmin(ctx, reg, args)
	case selFinalizeUpdateAdmin:
		return nil, finalizeUpdateAdmin(ctx, reg, args)
	case selRenounceAdmin:
		return nil, renounceAdmin(ctx, reg, args)
	}
	return nil, vm.ErrExecutionReverted // unreachable: the gate above is exhaustive
}

// validateChildren: the count, then existence over the whole set, then eligibility.
func validateChildren(reg policyReg, kids []common.Hash) ([]uint64, error) {
	if len(kids) < cas20CompositeMinChildren || len(kids) > cas20CompositeMaxChildren {
		return nil, revCAS20("ChildPoliciesOutsideOfRange()", errSelChildrenOutOfRange)
	}
	out := make([]uint64, 0, len(kids))
	for _, w := range kids {
		id, ok := u64FromWord(w)
		if !ok {
			return nil, vm.ErrExecutionReverted
		}
		out = append(out, id)
	}
	// Existence for every child before eligibility for any: the order is consensus.
	for _, id := range out {
		if !reg.policyExists(id) {
			return nil, revCAS20("PolicyNotFound()", errSelPolicyNotFound)
		}
	}
	for _, id := range out {
		if isSentinelPolicy(id) || polIsComposite(id) {
			return nil, revCAS20("InvalidChildPolicy(uint64)", errSelInvalidChildPolicy, wU64(id))
		}
	}
	return out, nil
}

func emitCompositeUpdated(ctx *PrecompileContext, id uint64, admin common.Address, kids []uint64) bool {
	words := make([]common.Hash, len(kids))
	for i, k := range kids {
		words[i] = wU64(k)
	}
	return ctx.AddLog([]common.Hash{cas20TopicCompositeUpdated, idKey(id), addrKey(admin)},
		encodeTuple(abiWordArray(words)))
}

func createCompositePolicy(ctx *PrecompileContext, reg policyReg, args []byte) ([]byte, error) {
	admin, err := readAddress(args, 0)
	if err != nil {
		return nil, err
	}
	ptypeWord, err := readWord(args, 1)
	if err != nil {
		return nil, err
	}
	if !isEnumWord(ptypeWord, cas20PolicyIntersect) {
		return nil, vm.ErrExecutionReverted
	}
	ptype := ptypeWord[31]
	if admin == (common.Address{}) {
		return nil, revCAS20("ZeroAddress()", errSelZeroAddress)
	}
	if !polIsComposite(uint64(ptype) << 56) {
		return nil, revCAS20("IncompatiblePolicyType()", errSelIncompatibleType)
	}
	rawKids, err := readWordArray(args, 2)
	if err != nil {
		return nil, err
	}
	kids, err := validateChildren(reg, rawKids)
	if err != nil {
		return nil, err
	}

	c := reg.ensureInitialized()
	if c >= cas20PolicyCounterMax {
		return nil, revPanic(0x11)
	}
	id := uint64(ptype)<<56 | c
	reg.setCounter(c + 1)
	reg.setPolicyAdmin(id, admin)
	reg.setChildren(id, kids)
	if !ctx.AddLog([]common.Hash{cas20TopicPolicyCreated, idKey(id), addrKey(ctx.Caller)}, hashBytes(wU8(ptype))) {
		return nil, vm.ErrOutOfGas
	}
	if !emitPolicyAdminUpdated(ctx, id, common.Address{}, admin) {
		return nil, vm.ErrOutOfGas
	}
	if !emitCompositeUpdated(ctx, id, admin, kids) {
		return nil, vm.ErrOutOfGas
	}
	return hashBytes(wU64(id)), nil
}

func updateComposite(ctx *PrecompileContext, reg policyReg, args []byte) error {
	id, err := readU64(args, 0)
	if err != nil {
		return err
	}
	if !reg.policyExists(id) {
		return revCAS20("PolicyNotFound()", errSelPolicyNotFound)
	}
	if !polIsComposite(id) {
		return revCAS20("IncompatiblePolicyType()", errSelIncompatibleType)
	}
	if admin := reg.admin(id); admin == (common.Address{}) || admin != ctx.Caller {
		return revCAS20("Unauthorized()", errSelUnauthorized)
	}
	rawKids, err := readWordArray(args, 1)
	if err != nil {
		return err
	}
	kids, err := validateChildren(reg, rawKids)
	if err != nil {
		return err
	}
	reg.setChildren(id, kids)
	if !emitCompositeUpdated(ctx, id, ctx.Caller, kids) {
		return vm.ErrOutOfGas
	}
	return nil
}

func createPolicy(ctx *PrecompileContext, reg policyReg, args []byte, withAccounts bool) ([]byte, error) {
	admin, err := readAddress(args, 0)
	if err != nil {
		return nil, err
	}
	ptypeWord, err := readWord(args, 1)
	if err != nil {
		return nil, err
	}
	ptype := ptypeWord[31]
	// The enum widened, so 2 and 3 decode; refused here after the zero-admin check.
	if !isEnumWord(ptypeWord, cas20PolicyIntersect) {
		return nil, vm.ErrExecutionReverted
	}
	if admin == (common.Address{}) {
		return nil, revCAS20("ZeroAddress()", errSelZeroAddress)
	}
	if polIsComposite(uint64(ptype) << 56) {
		return nil, revCAS20("IncompatiblePolicyType()", errSelIncompatibleType)
	}

	// Decoded and bounded before any write: a revert would not refund gas metered on premature writes.
	var accounts []common.Hash
	if withAccounts {
		if accounts, err = readWordArray(args, 2); err != nil {
			return nil, err
		}
		if len(accounts) > cas20PolicyBatchMax {
			return nil, revCAS20("BatchSizeTooLarge(uint256)", errSelBatchTooLarge, wU64(cas20PolicyBatchMax))
		}
	}

	c := reg.ensureInitialized()
	if c >= cas20PolicyCounterMax {
		return nil, revPanic(0x11)
	}
	id := uint64(ptype)<<56 | c
	reg.setCounter(c + 1)
	reg.setPolicyAdmin(id, admin)
	if !ctx.AddLog([]common.Hash{cas20TopicPolicyCreated, idKey(id), addrKey(ctx.Caller)}, hashBytes(wU8(ptype))) {
		return nil, vm.ErrOutOfGas
	}
	if !emitPolicyAdminUpdated(ctx, id, common.Address{}, admin) {
		return nil, vm.ErrOutOfGas
	}

	if withAccounts {
		for _, a := range accounts {
			addr, ok := addressFromWord(a)
			if !ok {
				return nil, vm.ErrExecutionReverted
			}
			reg.setMember(id, addr, true)
		}
		if !emitMembersUpdated(ctx, ptype, id, ctx.Caller, true, accounts) {
			return nil, vm.ErrOutOfGas
		}
	}
	return encU256(uint256.NewInt(id)), nil
}

func updateMembers(ctx *PrecompileContext, reg policyReg, args []byte, wantType byte) error {
	pid, err := readU64(args, 0)
	if err != nil {
		return err
	}
	inWord, err := readWord(args, 1)
	if err != nil {
		return err
	}
	// Decoded before the policy checks, as Solidity's external decoder would have.
	if !isEnumWord(inWord, 1) {
		return vm.ErrExecutionReverted
	}
	accounts, err := readWordArray(args, 2)
	if err != nil {
		return err
	}
	if err := requirePolicyExists(reg, pid); err != nil {
		return err
	}
	if polIDType(pid) != wantType {
		return revCAS20("IncompatiblePolicyType()", errSelIncompatibleType)
	}
	if err := requirePolicyAdmin(reg, pid, ctx.Caller); err != nil {
		return err
	}
	if len(accounts) > cas20PolicyBatchMax {
		return revCAS20("BatchSizeTooLarge(uint256)", errSelBatchTooLarge, wU64(cas20PolicyBatchMax))
	}
	in := inWord[31] == 1
	for _, a := range accounts {
		addr, ok := addressFromWord(a)
		if !ok {
			return vm.ErrExecutionReverted
		}
		reg.setMember(pid, addr, in)
	}
	if !emitMembersUpdated(ctx, wantType, pid, ctx.Caller, in, accounts) {
		return vm.ErrOutOfGas
	}
	return nil
}

func stageUpdateAdmin(ctx *PrecompileContext, reg policyReg, args []byte) error {
	id, err := readU64(args, 0)
	if err != nil {
		return err
	}
	newAdmin, err := readAddress(args, 1)
	if err != nil {
		return err
	}
	if err := requirePolicyAdmin(reg, id, ctx.Caller); err != nil {
		return err
	}
	reg.setPending(id, newAdmin)
	if !ctx.AddLog([]common.Hash{
		cas20TopicPolicyAdminStaged, idKey(id), addrKey(ctx.Caller), addrKey(newAdmin),
	}, nil) {
		return vm.ErrOutOfGas
	}
	return nil
}

func finalizeUpdateAdmin(ctx *PrecompileContext, reg policyReg, args []byte) error {
	pid, err := readU64(args, 0)
	if err != nil {
		return err
	}
	if err := requirePolicyExists(reg, pid); err != nil {
		return err
	}
	pending := reg.pending(pid)
	if pending == (common.Address{}) {
		return revCAS20("NoPendingAdmin()", errSelNoPendingAdmin)
	}
	if pending != ctx.Caller {
		return revCAS20("Unauthorized()", errSelUnauthorized)
	}
	previous := reg.admin(pid)
	reg.setPolicyAdmin(pid, ctx.Caller)
	reg.setPending(pid, common.Address{})
	if !emitPolicyAdminUpdated(ctx, pid, previous, ctx.Caller) {
		return vm.ErrOutOfGas
	}
	return nil
}

func renounceAdmin(ctx *PrecompileContext, reg policyReg, args []byte) error {
	pid, err := readU64(args, 0)
	if err != nil {
		return err
	}
	if err := requirePolicyAdmin(reg, pid, ctx.Caller); err != nil {
		return err
	}
	// Frozen, not deleted: the exists bit stays, so a renounced policy is not one never created.
	reg.setPolicyAdmin(pid, common.Address{})
	reg.setPending(pid, common.Address{})
	if !emitPolicyAdminUpdated(ctx, pid, ctx.Caller, common.Address{}) {
		return vm.ErrOutOfGas
	}
	return nil
}

func requirePolicyExists(reg policyReg, id uint64) error {
	if !reg.policyExists(id) {
		return revCAS20("PolicyNotFound()", errSelPolicyNotFound)
	}
	return nil
}

func requirePolicyAdmin(reg policyReg, id uint64, caller common.Address) error {
	if err := requirePolicyExists(reg, id); err != nil {
		return err
	}
	if admin := reg.admin(id); admin == (common.Address{}) || admin != caller {
		return revCAS20("Unauthorized()", errSelUnauthorized)
	}
	return nil
}
