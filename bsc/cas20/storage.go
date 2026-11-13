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
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/protocol/params"
)

const cas20Namespace = "bsc.cas20"

const (
	cas20SlotName             = 0
	cas20SlotSymbol           = 1
	cas20SlotContractURI      = 2
	cas20SlotTotalSupply      = 3
	cas20SlotBalances         = 4
	cas20SlotAllowances       = 5
	cas20SlotRoles            = 6
	cas20SlotRoleAdmins       = 7
	cas20SlotAdminCount       = 8
	cas20SlotTransferPolicies = 9
	cas20SlotMintPolicy       = 10
	cas20SlotPaused           = 11
	cas20SlotSupplyCap        = 12
	cas20SlotNonces           = 13
	cas20SlotSeizePolicies    = 14
)

// The free lanes of each policy slot are reserved for that group, which is why
// the seize ids have a slot of their own rather than filling the mint slot.
const (
	cas20OffTransferSender   = 0
	cas20OffTransferReceiver = 8
	cas20OffTransferExecutor = 16
	cas20OffMintReceiver     = 0
	cas20OffSeizeExempt      = 0
	cas20OffSeizeReceiver    = 8
)

var cas20CoreRoot = erc7201Root(cas20Namespace)

func erc7201Root(namespace string) common.Hash {
	inner := new(uint256.Int).SetBytes(crypto.Keccak256([]byte(namespace)))
	inner.SubUint64(inner, 1)
	buf := inner.Bytes32()
	root := keccakHash(buf[:])
	root[31] = 0
	return root
}

func slotAt(offset uint64) common.Hash {
	s := new(uint256.Int).SetBytes(cas20CoreRoot[:])
	s.AddUint64(s, offset)
	return s.Bytes32()
}

func offsetSlot(root common.Hash, offset uint64) common.Hash {
	x := new(uint256.Int).SetBytes(root[:])
	x.AddUint64(x, offset)
	return x.Bytes32()
}

func mappingSlot(base, key common.Hash) common.Hash {
	return keccakHash(key[:], base[:])
}

func addrKey(a common.Address) common.Hash { return common.BytesToHash(a[:]) }

type cas20Storage struct {
	state StateDB
	token common.Address
	ctx   *PrecompileContext
}

// A refused charge yields the zero slot, which is harmless: the frame is out of
// gas by then, so every access through it is refused too.
func (s cas20Storage) mapSlot(base, key common.Hash) common.Hash {
	if s.ctx != nil && !s.ctx.chargeKeccak(64) {
		return common.Hash{}
	}
	return mappingSlot(base, key)
}

// Solidity hashes a string key's raw bytes ++ base, so the preimage and the charge
// are caller-sized.
func (s cas20Storage) strMapSlot(base common.Hash, key string) common.Hash {
	if s.ctx != nil && !s.ctx.chargeKeccak(len(key)+32) {
		return common.Hash{}
	}
	return keccakHash([]byte(key), base[:])
}

// Only for callers with no frame to charge: state queries
// and tests. Everything reached from a precompile call must be metered.
func newUnmeteredCAS20Storage(state StateDB, token common.Address) cas20Storage {
	return cas20Storage{state: state, token: token}
}

func newMeteredCAS20Storage(ctx *PrecompileContext) cas20Storage {
	return cas20Storage{state: ctx.StateDB, token: ctx.Self, ctx: ctx}
}

// A token consulting a registry pays as if the slot were its own: no
// account-access surcharge (BEP-702 3.14).
func newMeteredCAS20StorageAt(ctx *PrecompileContext, token common.Address) cas20Storage {
	return cas20Storage{state: ctx.StateDB, token: token, ctx: ctx}
}

func (s cas20Storage) chargeRead(slot common.Hash) bool {
	if s.ctx == nil {
		return true
	}
	if _, warm := s.state.SlotInAccessList(s.token, slot); warm {
		return s.ctx.chargeGas(params.WarmStorageReadCostEIP2929)
	}
	s.state.AddSlotToAccessList(s.token, slot)
	return s.ctx.chargeGas(params.ColdSloadCostEIP2929)
}

// getWordChecked is for reads whose value decides a branch: getWord's zero is safe
// only where the next charge stops the caller anyway, not where zero means "proceed".
func (s cas20Storage) getWordChecked(slot common.Hash) (common.Hash, bool) {
	if !s.chargeRead(slot) {
		return common.Hash{}, false
	}
	return s.state.GetState(s.token, slot), true
}

func (s cas20Storage) getWord(slot common.Hash) common.Hash {
	if !s.chargeRead(slot) {
		return common.Hash{}
	}
	return s.state.GetState(s.token, slot)
}

// The fixed-size wrappers below drop the result deliberately: the out-of-gas flag
// is sticky, so what dropping it costs is the bound on work, not correctness.
func (s cas20Storage) setWord(slot, val common.Hash) bool {
	if !s.chargeStorageWrite(slot, val) {
		return false
	}
	s.state.SetState(s.token, slot, val)
	return true
}

// --- fixed uint256 fields ---------------------------------------------------

func (s cas20Storage) getU256(offset uint64) *uint256.Int {
	return new(uint256.Int).SetBytes(hashBytes(s.getWord(slotAt(offset))))
}

func (s cas20Storage) setU256(offset uint64, v *uint256.Int) {
	s.setWord(slotAt(offset), v.Bytes32())
}

func (s cas20Storage) totalSupply() *uint256.Int     { return s.getU256(cas20SlotTotalSupply) }
func (s cas20Storage) setTotalSupply(v *uint256.Int) { s.setU256(cas20SlotTotalSupply, v) }
func (s cas20Storage) supplyCap() *uint256.Int       { return s.getU256(cas20SlotSupplyCap) }
func (s cas20Storage) setSupplyCap(v *uint256.Int)   { s.setU256(cas20SlotSupplyCap, v) }
func (s cas20Storage) adminCount() *uint256.Int      { return s.getU256(cas20SlotAdminCount) }
func (s cas20Storage) setAdminCount(v *uint256.Int)  { s.setU256(cas20SlotAdminCount, v) }

func (s cas20Storage) pausedChecked() (*uint256.Int, bool) {
	w, ok := s.getWordChecked(slotAt(cas20SlotPaused))
	if !ok {
		return new(uint256.Int), false
	}
	return new(uint256.Int).SetBytes(w[:]), true
}

func (s cas20Storage) paused() *uint256.Int     { return s.getU256(cas20SlotPaused) }
func (s cas20Storage) setPaused(v *uint256.Int) { s.setU256(cas20SlotPaused, v) }

// --- balances / allowances / nonces ----------------------------------------

// Deriving a mapping slot is a metered keccak, so a read-modify-write derives the
// slot once and reuses it, as Solidity would.

func (s cas20Storage) balanceSlot(a common.Address) common.Hash {
	return s.mapSlot(slotAt(cas20SlotBalances), addrKey(a))
}

func (s cas20Storage) allowanceSlot(owner, spender common.Address) common.Hash {
	return s.mapSlot(s.mapSlot(slotAt(cas20SlotAllowances), addrKey(owner)), addrKey(spender))
}

func (s cas20Storage) getU256At(slot common.Hash) *uint256.Int {
	return new(uint256.Int).SetBytes(hashBytes(s.getWord(slot)))
}

func (s cas20Storage) setU256At(slot common.Hash, v *uint256.Int) {
	s.setWord(slot, v.Bytes32())
}

func (s cas20Storage) balanceOf(a common.Address) *uint256.Int {
	return s.getU256At(s.balanceSlot(a))
}

func (s cas20Storage) setBalance(a common.Address, v *uint256.Int) {
	s.setU256At(s.balanceSlot(a), v)
}

func (s cas20Storage) allowance(owner, spender common.Address) *uint256.Int {
	return s.getU256At(s.allowanceSlot(owner, spender))
}

func (s cas20Storage) setAllowance(owner, spender common.Address, v *uint256.Int) {
	s.setU256At(s.allowanceSlot(owner, spender), v)
}

func (s cas20Storage) nonce(owner common.Address) *uint256.Int {
	slot := s.mapSlot(slotAt(cas20SlotNonces), addrKey(owner))
	return new(uint256.Int).SetBytes(hashBytes(s.getWord(slot)))
}

func (s cas20Storage) setNonce(owner common.Address, v *uint256.Int) {
	slot := s.mapSlot(slotAt(cas20SlotNonces), addrKey(owner))
	s.setWord(slot, v.Bytes32())
}

// --- roles ------------------------------------------------------------------

func (s cas20Storage) hasRole(role common.Hash, a common.Address) bool {
	inner := s.mapSlot(slotAt(cas20SlotRoles), role)
	slot := s.mapSlot(inner, addrKey(a))
	return s.getWord(slot) != (common.Hash{})
}

func (s cas20Storage) setRole(role common.Hash, a common.Address, enabled bool) {
	inner := s.mapSlot(slotAt(cas20SlotRoles), role)
	slot := s.mapSlot(inner, addrKey(a))
	var v common.Hash
	if enabled {
		v[31] = 1
	}
	s.setWord(slot, v)
}

func (s cas20Storage) roleAdmin(role common.Hash) common.Hash {
	return s.getWord(s.mapSlot(slotAt(cas20SlotRoleAdmins), role))
}

func (s cas20Storage) setRoleAdmin(role, admin common.Hash) {
	s.setWord(s.mapSlot(slotAt(cas20SlotRoleAdmins), role), admin)
}

// --- packed policy ids ------------------------------------------------------

func (s cas20Storage) getPackedU64(offset uint64, byteOff uint) uint64 {
	word := new(uint256.Int).SetBytes(hashBytes(s.getWord(slotAt(offset))))
	return word.Rsh(word, byteOff*8).Uint64()
}

func (s cas20Storage) setPackedU64(offset uint64, byteOff uint, v uint64) {
	slot := slotAt(offset)
	word := new(uint256.Int).SetBytes(hashBytes(s.getWord(slot)))
	lane := new(uint256.Int).Lsh(uint256.NewInt(0xffffffffffffffff), byteOff*8)
	word.And(word, lane.Not(lane))
	word.Or(word, new(uint256.Int).Lsh(uint256.NewInt(v), byteOff*8))
	s.setWord(slot, word.Bytes32())
}

func (s cas20Storage) transferPolicies() (sender, receiver, executor uint64) {
	w := s.getU256At(slotAt(cas20SlotTransferPolicies))
	return packedLane(w, cas20OffTransferSender), packedLane(w, cas20OffTransferReceiver),
		packedLane(w, cas20OffTransferExecutor)
}

func (s cas20Storage) seizePolicies() (exempt, receiver uint64) {
	w := s.getU256At(slotAt(cas20SlotSeizePolicies))
	return packedLane(w, cas20OffSeizeExempt), packedLane(w, cas20OffSeizeReceiver)
}

func packedLane(word *uint256.Int, byteOff uint) uint64 {
	return new(uint256.Int).Rsh(word, byteOff*8).Uint64()
}

func (s cas20Storage) transferSenderPolicy() uint64 {
	return s.getPackedU64(cas20SlotTransferPolicies, cas20OffTransferSender)
}

func (s cas20Storage) transferReceiverPolicy() uint64 {
	return s.getPackedU64(cas20SlotTransferPolicies, cas20OffTransferReceiver)
}

func (s cas20Storage) transferExecutorPolicy() uint64 {
	return s.getPackedU64(cas20SlotTransferPolicies, cas20OffTransferExecutor)
}

func (s cas20Storage) mintReceiverPolicy() uint64 {
	return s.getPackedU64(cas20SlotMintPolicy, cas20OffMintReceiver)
}

func (s cas20Storage) setTransferSenderPolicy(id uint64) {
	s.setPackedU64(cas20SlotTransferPolicies, cas20OffTransferSender, id)
}

func (s cas20Storage) setTransferReceiverPolicy(id uint64) {
	s.setPackedU64(cas20SlotTransferPolicies, cas20OffTransferReceiver, id)
}

func (s cas20Storage) setTransferExecutorPolicy(id uint64) {
	s.setPackedU64(cas20SlotTransferPolicies, cas20OffTransferExecutor, id)
}

func (s cas20Storage) setMintReceiverPolicy(id uint64) {
	s.setPackedU64(cas20SlotMintPolicy, cas20OffMintReceiver, id)
}

func (s cas20Storage) setSeizeExemptPolicy(id uint64) {
	s.setPackedU64(cas20SlotSeizePolicies, cas20OffSeizeExempt, id)
}

func (s cas20Storage) setSeizeReceiverPolicy(id uint64) {
	s.setPackedU64(cas20SlotSeizePolicies, cas20OffSeizeReceiver, id)
}

// --- strings (Solidity storage encoding) ------------------------------------

func (s cas20Storage) getString(offset uint64) (string, bool) { return s.getStringAt(slotAt(offset)) }

func (s cas20Storage) setString(offset uint64, str string) bool {
	return s.setStringAt(slotAt(offset), str)
}

func (s cas20Storage) stringDataRoot(slot common.Hash) *uint256.Int {
	if s.ctx != nil && !s.ctx.chargeKeccak(32) {
		return new(uint256.Int)
	}
	return new(uint256.Int).SetBytes(crypto.Keccak256(slot[:]))
}

const cas20MaxStringLen = 1 << 24

func (s cas20Storage) getStringAt(slot common.Hash) (string, bool) {
	word, ok := s.getWordChecked(slot)
	if !ok {
		return "", false
	}
	if word[31]&1 == 0 {
		n := int(word[31]) / 2
		if n > 31 {
			return "", true
		}
		return string(word[:n]), true
	}
	encoded := new(uint256.Int).SetBytes(word[:])
	if encoded.Gt(uint256.NewInt(2*cas20MaxStringLen + 1)) {
		return "", true
	}
	length := (encoded.Uint64() - 1) / 2
	if length < 32 {
		return "", true
	}
	base := s.stringDataRoot(slot)
	// Grown only behind paid reads, so the allocation is bounded by the gas spent.
	var out []byte
	for i := uint64(0); i < length; i += 32 {
		chunkSlot := new(uint256.Int).AddUint64(base, i/32).Bytes32()
		chunk, ok := s.getWordChecked(chunkSlot)
		if !ok {
			return "", false
		}
		out = append(out, chunk[:]...)
	}
	return string(out[:length]), true
}

func (s cas20Storage) setStringAt(slot common.Hash, str string) bool {
	b := []byte(str)
	oldChunks := s.stringChunks(slot)
	newChunks := uint64(0)
	if len(b) >= 32 {
		newChunks = uint64((len(b) + 31) / 32)
	}

	if len(b) < 32 {
		var word common.Hash
		copy(word[:], b)
		word[31] = byte(len(b) * 2)
		if !s.setWord(slot, word) {
			return false
		}
	} else if !s.setWord(slot, uint256.NewInt(uint64(len(b)*2+1)).Bytes32()) {
		return false
	}
	if newChunks == 0 && oldChunks == 0 {
		return true
	}
	base := s.stringDataRoot(slot)
	for i := uint64(0); i < newChunks; i++ {
		if s.ctx != nil && s.ctx.OutOfGas() {
			return false
		}
		var chunk common.Hash
		copy(chunk[:], b[i*32:])
		s.setWord(new(uint256.Int).AddUint64(base, i).Bytes32(), chunk)
	}
	// oldChunks comes from state, not calldata: without this guard a starved frame
	// would do work proportional to the old length and never pay for it.
	for i := newChunks; i < oldChunks; i++ {
		if s.ctx != nil && s.ctx.OutOfGas() {
			return false
		}
		s.setWord(new(uint256.Int).AddUint64(base, i).Bytes32(), common.Hash{})
	}
	return true
}

func (s cas20Storage) stringChunks(slot common.Hash) uint64 {
	word := s.getWord(slot)
	if word[31]&1 == 0 {
		return 0
	}
	// Same bounds as getStringAt: a word it reads as empty must not leave chunks
	// for the release loop to walk.
	encoded := new(uint256.Int).SetBytes(word[:])
	if encoded.Gt(uint256.NewInt(2*cas20MaxStringLen + 1)) {
		return 0
	}
	length := (encoded.Uint64() - 1) / 2
	if length < 32 {
		return 0
	}
	return (length + 31) / 32
}

func (s cas20Storage) name() (string, bool)         { return s.getString(cas20SlotName) }
func (s cas20Storage) setName(v string) bool        { return s.setString(cas20SlotName, v) }
func (s cas20Storage) symbol() (string, bool)       { return s.getString(cas20SlotSymbol) }
func (s cas20Storage) setSymbol(v string) bool      { return s.setString(cas20SlotSymbol, v) }
func (s cas20Storage) contractURI() (string, bool)  { return s.getString(cas20SlotContractURI) }
func (s cas20Storage) setContractURI(v string) bool { return s.setString(cas20SlotContractURI, v) }
