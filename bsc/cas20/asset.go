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

// The Asset extension lives in its own ERC-7201 namespace, disjoint from the core layout.
const cas20AssetNamespace = "bsc.cas20.asset"

const (
	cas20AssetSlotDecimals      = 0
	cas20AssetSlotMultiplier    = 1
	cas20AssetSlotAnnouncements = 2 // mapping(string id => bool used)
	cas20AssetSlotExtraMeta     = 3 // mapping(string key => string value)
	cas20AssetSlotPending       = 4 // packed: multiplier (u128) | effectiveAt (u64)
)

// LSB-first, as Solidity packs {uint128 multiplier; uint64 effectiveAt}.
const cas20PendingWhenBits = 128

var (
	cas20AssetRoot = erc7201Root(cas20AssetNamespace)
	cas20WAD       = uint256.NewInt(1_000_000_000_000_000_000) // 1.0x
	cas20U64Max    = new(uint256.Int).SetUint64(^uint64(0))
	cas20U128Mask  = new(uint256.Int).Sub(new(uint256.Int).Lsh(uint256.NewInt(1), 128), uint256.NewInt(1))

	roleOperator = keccakHash([]byte("OPERATOR_ROLE"))

	selMultiplier       = selector("multiplier()")
	selWadPrecision     = selector("WAD_PRECISION()")
	selScaledBalanceOf  = selector("scaledBalanceOf(address)")
	selToScaledBalance  = selector("toScaledBalance(uint256)")
	selToRawBalance     = selector("toRawBalance(uint256)")
	selUpdateMultiplier = selector("updateMultiplier(uint256)")
	selOperatorRole     = selector("OPERATOR_ROLE()")
	selBatchMint        = selector("batchMint(address[],uint256[])")

	selAnnounce             = selector("announce(bytes[],string,string,string)")
	selIsAnnouncementIdUsed = selector("isAnnouncementIdUsed(string)")

	selExtraMetadata       = selector("extraMetadata(string)")
	selUpdateExtraMetadata = selector("updateExtraMetadata(string,string)")

	// ERC-8056 names; the earlier ones stay dialable as aliases.
	selUIMultiplier       = selector("uiMultiplier()")
	selToUIAmount         = selector("toUIAmount(uint256)")
	selFromUIAmount       = selector("fromUIAmount(uint256)")
	selBalanceOfUI        = selector("balanceOfUI(address)")
	selTotalSupplyUI      = selector("totalSupplyUI()")
	selNewUIMultiplier    = selector("newUIMultiplier()")
	selEffectiveAt        = selector("effectiveAt()")
	selUpdateUIMultiplier = selector("updateUIMultiplier(uint256,uint256)")
	selCancelUIMultiplier = selector("cancelUIMultiplierUpdate()")
	selMaxUIMultiplier    = selector("MAX_UI_MULTIPLIER()")
	selSupportsInterface  = selector("supportsInterface(bytes4)")

	cas20TopicMultiplierUpdated    = eventTopic("MultiplierUpdated(uint256)")
	cas20TopicAnnouncement         = eventTopic("Announcement(address,string,string,string)")
	cas20TopicEndAnnouncement      = eventTopic("EndAnnouncement(string)")
	cas20TopicExtraMetadataUpdated = eventTopic("ExtraMetadataUpdated(string,string)")

	cas20TopicUIMultiplierUpdated   = eventTopic("UIMultiplierUpdated(uint256,uint256,uint256)")
	cas20TopicUIMultiplierCancelled = eventTopic("UIMultiplierUpdateCancelled(uint256,uint256)")
)

// All four ERC-8056 interfaces are implemented, so all four are advertised.
var cas20AssetInterfaceIDs = map[[4]byte]bool{
	{0x01, 0xff, 0xc9, 0xa7}: true, // IERC165
	{0xa6, 0x0b, 0xf1, 0x3d}: true, // IScaledUIAmount
	{0x4b, 0xd2, 0x76, 0x48}: true, // IScaledUIAmountNewUIMultiplier
	{0xd8, 0x90, 0xfd, 0x71}: true, // IScaledUIAmountBalances
	{0x57, 0x85, 0x4f, 0xc3}: true, // IScaledUIAmountConversion
}

type assetExt struct{ s cas20Storage }

func newAssetExt(ctx *PrecompileContext) assetExt { return assetExt{s: newMeteredCAS20Storage(ctx)} }

func assetSlot(offset uint64) common.Hash { return offsetSlot(cas20AssetRoot, offset) }

func (e assetExt) decimals() uint8 {
	return uint8(new(uint256.Int).SetBytes(hashBytes(e.s.getWord(assetSlot(cas20AssetSlotDecimals)))).Uint64())
}

func (e assetExt) setDecimals(d uint8) {
	e.s.setWord(assetSlot(cas20AssetSlotDecimals), uint256.NewInt(uint64(d)).Bytes32())
}

func (e assetExt) multiplier() *uint256.Int {
	return new(uint256.Int).SetBytes(hashBytes(e.s.getWord(assetSlot(cas20AssetSlotMultiplier))))
}

func (e assetExt) setMultiplier(m *uint256.Int) {
	e.s.setWord(assetSlot(cas20AssetSlotMultiplier), m.Bytes32())
}

// --- ERC-8056 scheduled multiplier -------------------------------------------
//
// The effective multiplier flips from the stored value to the scheduled one at a
// timestamp, with no transaction and no event at the flip.

func (e assetExt) pendingSlot() common.Hash { return assetSlot(cas20AssetSlotPending) }

func (e assetExt) pending() (mul *uint256.Int, effectiveAt uint64) {
	w := new(uint256.Int).SetBytes(hashBytes(e.s.getWord(e.pendingSlot())))
	lane := new(uint256.Int).Rsh(w, cas20PendingWhenBits)
	return new(uint256.Int).And(w, cas20U128Mask), lane.Uint64()
}

func (e assetExt) setPending(mul *uint256.Int, effectiveAt uint64) {
	packed := new(uint256.Int).And(mul, cas20U128Mask)
	packed.Or(packed, new(uint256.Int).Lsh(uint256.NewInt(effectiveAt), cas20PendingWhenBits))
	e.s.setWord(e.pendingSlot(), packed.Bytes32())
}

func (e assetExt) clearPending() { e.s.setWord(e.pendingSlot(), common.Hash{}) }

// Reads cannot write, so a matured schedule is folded into the stored multiplier by
// the next write that reuses the slot; otherwise that write would revalue the token.
func (e assetExt) settleMatured(now uint64) {
	if mul, at := e.pending(); at != 0 && now >= at {
		e.setMultiplier(mul)
	}
}

func (e assetExt) effectiveMultiplier(now uint64) *uint256.Int {
	if mul, at := e.pending(); at != 0 && now >= at {
		return mul
	}
	return e.multiplier()
}

func (e assetExt) announcementSlot(id string) common.Hash {
	return e.s.strMapSlot(assetSlot(cas20AssetSlotAnnouncements), id)
}

func (e assetExt) announcementUsed(id string) (bool, bool) {
	w, ok := e.s.getWordChecked(e.announcementSlot(id))
	return w != (common.Hash{}), ok
}

func (e assetExt) markAnnouncement(id string) bool {
	var one common.Hash
	one[31] = 1
	return e.s.setWord(e.announcementSlot(id), one)
}

func (e assetExt) extraMetaSlot(key string) common.Hash {
	return e.s.strMapSlot(assetSlot(cas20AssetSlotExtraMeta), key)
}

func (e assetExt) extraMetadata(key string) (string, bool) {
	return e.s.getStringAt(e.extraMetaSlot(key))
}

func (e assetExt) setExtraMetadata(key, value string) bool {
	return e.s.setStringAt(e.extraMetaSlot(key), value)
}

func initAssetExtension(ctx *PrecompileContext, decimals byte) {
	e := newAssetExt(ctx)
	e.setDecimals(decimals)
	e.setMultiplier(cas20WAD)
}

func applyMultiplier(raw, mul *uint256.Int) (*uint256.Int, error) {
	p, overflow := new(uint256.Int).MulOverflow(raw, mul)
	if overflow {
		return nil, revPanic(0x11)
	}
	return p.Div(p, cas20WAD), nil
}

// uint256 division by zero yields zero; the setters reject a zero multiplier anyway.
func removeMultiplier(scaled, mul *uint256.Int) (*uint256.Int, error) {
	p, overflow := new(uint256.Int).MulOverflow(scaled, cas20WAD)
	if overflow {
		return nil, revPanic(0x11)
	}
	return p.Div(p, mul), nil
}

func assetDispatch(tok cas20Token, ext assetExt, input []byte) ([]byte, error) {
	if ret, err, ok := dispatchAsset(tok, ext, input); ok {
		return ret, err
	}
	return tok.dispatch(input)
}

func dispatchAsset(tok cas20Token, ext assetExt, input []byte) (ret []byte, err error, ok bool) {
	if len(input) < 4 {
		return nil, nil, false
	}
	var sel [4]byte
	copy(sel[:], input[:4])
	args := input[4:]

	effective := func() *uint256.Int { return ext.effectiveMultiplier(tok.ctx.BlockTime()) }

	switch sel {
	case selDecimals:
		return encU256(uint256.NewInt(uint64(ext.decimals()))), nil, true
	case selMultiplier, selUIMultiplier:
		return encU256(effective()), nil, true
	case selWadPrecision:
		return encU256(cas20WAD), nil, true
	case selMaxUIMultiplier:
		return encU256(cas20U128Mask), nil, true
	case selOperatorRole:
		return roleOperator[:], nil, true
	case selNewUIMultiplier:
		// With no live schedule it answers as uiMultiplier does (ERC-8056).
		mul, at := ext.pending()
		if at <= tok.ctx.BlockTime() {
			mul = effective()
		}
		return encU256(mul), nil, true
	case selEffectiveAt:
		_, at := ext.pending()
		return encU256(uint256.NewInt(at)), nil, true
	case selTotalSupplyUI:
		v, err := applyMultiplier(tok.s.totalSupply(), effective())
		if err != nil {
			return nil, err, true
		}
		return encU256(v), nil, true
	case selSupportsInterface:
		id, err := readWord(args, 0)
		if err != nil {
			return nil, err, true
		}
		var want [4]byte
		copy(want[:], id[:4])
		for _, b := range id[4:] {
			if b != 0 {
				return nil, vm.ErrExecutionReverted, true
			}
		}
		return encBool(cas20AssetInterfaceIDs[want]), nil, true
	case selCancelUIMultiplier:
		return nil, cancelUIMultiplier(tok, ext), true
	case selUpdateUIMultiplier:
		mul, err := readU256(args, 0)
		if err != nil {
			return nil, err, true
		}
		at, err := readU256(args, 1)
		if err != nil {
			return nil, err, true
		}
		return nil, updateUIMultiplier(tok, ext, mul, at), true
	case selScaledBalanceOf, selBalanceOfUI:
		a, err := readAddress(args, 0)
		if err != nil {
			return nil, err, true
		}
		v, err := applyMultiplier(tok.s.balanceOf(a), effective())
		if err != nil {
			return nil, err, true
		}
		return encU256(v), nil, true
	case selToScaledBalance, selToUIAmount:
		raw, err := readU256(args, 0)
		if err != nil {
			return nil, err, true
		}
		v, err := applyMultiplier(raw, effective())
		if err != nil {
			return nil, err, true
		}
		return encU256(v), nil, true
	case selToRawBalance, selFromUIAmount:
		scaled, err := readU256(args, 0)
		if err != nil {
			return nil, err, true
		}
		v, err := removeMultiplier(scaled, effective())
		if err != nil {
			return nil, err, true
		}
		return encU256(v), nil, true
	case selUpdateMultiplier:
		m, err := readU256(args, 0)
		if err != nil {
			return nil, err, true
		}
		return nil, updateMultiplier(tok, ext, m), true
	case selBatchMint:
		return nil, batchMint(tok, args), true
	case selIsAnnouncementIdUsed:
		id, err := readStringArg(args, 0)
		if err != nil {
			return nil, err, true
		}
		used, ok := ext.announcementUsed(id)
		if !ok {
			return nil, vm.ErrOutOfGas, true
		}
		return encBool(used), nil, true
	case selAnnounce:
		return nil, announce(tok, ext, args), true
	case selExtraMetadata:
		key, err := readStringArg(args, 0)
		if err != nil {
			return nil, err, true
		}
		v, ok := ext.extraMetadata(key)
		if !ok {
			return nil, vm.ErrOutOfGas, true
		}
		return encString(v), nil, true
	case selUpdateExtraMetadata:
		key, err := readStringArg(args, 0)
		if err != nil {
			return nil, err, true
		}
		value, err := readStringArg(args, 1)
		if err != nil {
			return nil, err, true
		}
		return nil, updateExtraMetadata(tok, ext, key, value), true
	}
	return nil, nil, false
}

func updateExtraMetadata(tok cas20Token, ext assetExt, key, value string) error {
	if tok.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	if err := tok.ensureRole(roleMetadata); err != nil {
		return err
	}
	if len(key) == 0 {
		return revCAS20("InvalidMetadataKey()", errSelInvalidMetadataKey)
	}
	if !ext.setExtraMetadata(key, value) {
		return vm.ErrOutOfGas
	}
	if !tok.ctx.AddLog([]common.Hash{cas20TopicExtraMetadataUpdated},
		encodeTuple(abiString(key), abiString(value))) {
		return vm.ErrOutOfGas
	}
	return nil
}

func announce(tok cas20Token, ext assetExt, args []byte) error {
	calls, err := readBytesArray(args, 0)
	if err != nil {
		return err
	}
	id, err := readStringArg(args, 1)
	if err != nil {
		return err
	}
	description, err := readStringArg(args, 2)
	if err != nil {
		return err
	}
	uri, err := readStringArg(args, 3)
	if err != nil {
		return err
	}
	if tok.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	if tok.inAnnounce {
		return revCAS20("AnnouncementInProgress()", errSelAnnounceInProgress)
	}
	if err := tok.ensureRole(roleOperator); err != nil {
		return err
	}
	used, ok := ext.announcementUsed(id)
	if !ok {
		return vm.ErrOutOfGas
	}
	if used {
		return revCAS20Bytes("AnnouncementIdAlreadyUsed(string)", errSelAnnounceIdUsed, []byte(id))
	}
	if !ext.markAnnouncement(id) {
		return vm.ErrOutOfGas
	}
	if !tok.ctx.AddLog([]common.Hash{cas20TopicAnnouncement, addrKey(tok.ctx.Caller)},
		encodeTuple(abiString(id), abiString(description), abiString(uri))) {
		return vm.ErrOutOfGas
	}

	tok.inAnnounce = true
	for _, c := range calls {
		if !tok.ctx.chargeInternalDispatch(c) {
			return vm.ErrOutOfGas
		}
		if len(c) < 4 {
			return revCAS20Bytes("InternalCallMalformed(bytes)", errSelInternalMalformed, c)
		}
		if _, err := assetDispatch(tok, ext, c); err != nil {
			return revCAS20Bytes("InternalCallFailed(bytes)", errSelInternalFailed, c)
		}
	}
	if !tok.ctx.AddLog([]common.Hash{cas20TopicEndAnnouncement}, encodeTuple(abiString(id))) {
		return vm.ErrOutOfGas
	}
	return nil
}

// Check order: role, value, both timestamp bounds, then a live schedule.
func updateUIMultiplier(tok cas20Token, ext assetExt, newMul, at *uint256.Int) error {
	if tok.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	if err := tok.ensureRole(roleOperator); err != nil {
		return err
	}
	if newMul.IsZero() || newMul.Gt(cas20U128Mask) {
		return revCAS20("InvalidMultiplier()", errSelInvalidMultiplier)
	}
	now := tok.ctx.BlockTime()
	if !at.GtUint64(now) {
		return revCAS20("EffectiveAtInPast(uint256)", errSelEffectiveAtInPast, wU256(at))
	}
	if at.Gt(cas20U64Max) {
		return revCAS20("EffectiveAtTooFar(uint256)", errSelEffectiveAtTooFar, wU256(at))
	}
	// Only a live schedule blocks a new one; a matured record is stale state, not a commitment.
	if _, existing := ext.pending(); existing > now {
		return revCAS20("UIMultiplierUpdateExists(uint256)", errSelUIMulExists, wU64(existing))
	}
	ext.settleMatured(now)
	previous := ext.multiplier()
	ext.setPending(newMul, at.Uint64())
	if !tok.ctx.AddLog([]common.Hash{cas20TopicUIMultiplierUpdated},
		append(append(hashBytes(wU256(previous)), hashBytes(wU256(newMul))...), hashBytes(wU256(at))...)) {
		return vm.ErrOutOfGas
	}
	return nil
}

// A matured schedule is already in force, so there is nothing to withdraw.
func cancelUIMultiplier(tok cas20Token, ext assetExt) error {
	if tok.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	if err := tok.ensureRole(roleOperator); err != nil {
		return err
	}
	mul, at := ext.pending()
	if at <= tok.ctx.BlockTime() {
		return revCAS20("UIMultiplierUpdateDoesNotExist()", errSelUIMulMissing)
	}
	ext.clearPending()
	if !tok.ctx.AddLog([]common.Hash{cas20TopicUIMultiplierCancelled},
		append(hashBytes(wU256(mul)), hashBytes(wU64(at))...)) {
		return vm.ErrOutOfGas
	}
	return nil
}

func updateMultiplier(tok cas20Token, ext assetExt, newMul *uint256.Int) error {
	if tok.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	if err := tok.ensureRole(roleOperator); err != nil {
		return err
	}
	if newMul.IsZero() || newMul.Gt(cas20U128Mask) {
		return revCAS20("InvalidMultiplier()", errSelInvalidMultiplier)
	}
	// The instant setter is the failsafe and overrides any schedule: a live one is
	// withdrawn loudly, a matured one is stale state and goes quietly.
	now := tok.ctx.BlockTime()
	previous := ext.effectiveMultiplier(now)
	if pendingMul, at := ext.pending(); at != 0 {
		ext.clearPending()
		if at > now {
			if !tok.ctx.AddLog([]common.Hash{cas20TopicUIMultiplierCancelled},
				append(hashBytes(wU256(pendingMul)), hashBytes(wU64(at))...)) {
				return vm.ErrOutOfGas
			}
		}
	}
	ext.setMultiplier(newMul)
	mb := newMul.Bytes32()
	if !tok.ctx.AddLog([]common.Hash{cas20TopicMultiplierUpdated}, mb[:]) {
		return vm.ErrOutOfGas
	}
	// Emitted by both setters so one stream carries every change.
	if !tok.ctx.AddLog([]common.Hash{cas20TopicUIMultiplierUpdated},
		append(append(hashBytes(wU256(previous)), hashBytes(wU256(newMul))...), hashBytes(wU256(uint256.NewInt(now)))...)) {
		return vm.ErrOutOfGas
	}
	return nil
}

func batchMint(tok cas20Token, args []byte) error {
	recipients, err := readWordArray(args, 0)
	if err != nil {
		return err
	}
	amounts, err := readWordArray(args, 1)
	if err != nil {
		return err
	}
	if tok.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	if tok.isPaused(cas20PauseMint) {
		return revCAS20("ContractPaused(uint8)", errSelContractPaused, wU8(cas20PauseMint))
	}
	if err := tok.ensureRole(roleMint); err != nil {
		return err
	}
	if len(recipients) != len(amounts) {
		return revCAS20("LengthMismatch(uint256,uint256)", errSelLengthMismatch,
			wU64(uint64(len(recipients))), wU64(uint64(len(amounts))))
	}
	if len(recipients) == 0 {
		return revCAS20("EmptyBatch()", errSelEmptyBatch)
	}
	for i := range recipients {
		// chargeGas only marks the frame, so without this an exhausted batch would
		// still run to completion before being discarded.
		if tok.ctx.OutOfGas() {
			return vm.ErrOutOfGas
		}
		to, ok := addressFromWord(recipients[i])
		if !ok {
			return vm.ErrExecutionReverted
		}
		amount := new(uint256.Int).SetBytes(hashBytes(amounts[i]))
		if err := tok.mintCore(to, amount); err != nil {
			return err
		}
	}
	return nil
}
