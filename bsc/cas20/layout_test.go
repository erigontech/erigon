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
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
)

// Expectations are spelled out from Solidity's rules, not obtained from the
// accessor under test, which would agree with itself whatever it did.

func solMap(slot common.Hash, key common.Hash) common.Hash {
	return keccakHash(key[:], slot[:])
}

func solSlot(base common.Hash, offset uint64) common.Hash {
	s := new(uint256.Int).SetBytes(base[:])
	s.AddUint64(s, offset)
	return common.Hash(s.Bytes32())
}

func TestCAS20PolicyMemberSlot(t *testing.T) {
	s := newTestStorage(t)
	reg := policyReg{s: s}

	const id = uint64(0x0100000000002a) // allowlist type, counter 42
	account := common.HexToAddress("0xbeef")

	reg.setMember(id, account, true)

	inner := solMap(solSlot(cas20PolicyRoot, polSlotMembers), idKey(id))
	want := solMap(inner, addrKey(account))
	if got := s.getWord(want); got == (common.Hash{}) {
		t.Errorf("members[%#x][%s] is not at %s", id, account.Hex(), want.Hex())
	}

	// The swapped order must be empty, or this asserts reachability rather than ordering.
	swappedInner := solMap(solSlot(cas20PolicyRoot, polSlotMembers), addrKey(account))
	if got := s.getWord(solMap(swappedInner, idKey(id))); got != (common.Hash{}) {
		t.Error("the account-then-id ordering also holds a value; the keys are interchangeable")
	}
}

// A dynamic key is hashed unpadded; padding it as a value-typed key would move every entry.
func TestCAS20StringKeyedSlots(t *testing.T) {
	ext := assetExt{s: newUnmeteredCAS20Storage(nil, common.Address{})}
	for _, tc := range []struct {
		name string
		slot uint64
		got  func(string) common.Hash
	}{
		{"extraMetadata", cas20AssetSlotExtraMeta, ext.extraMetaSlot},
		{"announcements", cas20AssetSlotAnnouncements, ext.announcementSlot},
	} {
		const key = "category"
		base := solSlot(cas20AssetRoot, tc.slot)
		if want, got := keccakHash([]byte(key), base[:]), tc.got(key); got != want {
			t.Errorf("%s slot for %q = %s, want keccak256(key . slot) = %s",
				tc.name, key, got.Hex(), want.Hex())
		}
		if reversed := keccakHash(base[:], []byte(key)); tc.got(key) == reversed {
			t.Errorf("%s: slot . key and key . slot agree; the order is not pinned", tc.name)
		}
	}

	// Otherwise a metadata key could mark an announcement id used.
	if ext.extraMetaSlot("x") == ext.announcementSlot("x") {
		t.Error("extraMetadata and announcements collide on a shared key")
	}
}

func TestCAS20ComplianceLanes(t *testing.T) {
	s := newTestStorage(t)
	const (
		mintRecv    = uint64(0x11)
		seizeExempt = uint64(0x22)
		seizeRecv   = uint64(0x33)
	)
	s.setMintReceiverPolicy(mintRecv)
	s.setSeizeExemptPolicy(seizeExempt)
	s.setSeizeReceiverPolicy(seizeRecv)

	if mintRecv == seizeExempt || seizeExempt == seizeRecv || mintRecv == seizeRecv {
		t.Fatal("the lane values must differ for a swap to be detectable")
	}

	lane := func(slot uint64, off int) uint64 {
		word := s.getU256At(slotAt(slot)).Bytes32()
		var got uint64
		for _, b := range word[32-off-8 : 32-off] {
			got = got<<8 | uint64(b)
		}
		return got
	}
	for _, tc := range []struct {
		name string
		slot uint64
		off  int
		want uint64
	}{
		{"mintReceiver", cas20SlotMintPolicy, 0, mintRecv},
		{"seizeExempt", cas20SlotSeizePolicies, 0, seizeExempt},
		{"seizeReceiver", cas20SlotSeizePolicies, 8, seizeRecv},
	} {
		if got := lane(tc.slot, tc.off); got != tc.want {
			t.Errorf("%s at slot %d byte offset %d = %#x, want %#x",
				tc.name, tc.slot, tc.off, got, tc.want)
		}
	}

	// The rest of the mint word is reserved, so a seize id straying into it is a
	// divergence even though both still read back.
	if word := s.getU256At(slotAt(cas20SlotMintPolicy)).Bytes32(); word != mintOnly(mintRecv) {
		t.Errorf("mint slot = %s, want only the receiver id in the low lane",
			common.Hash(word).Hex())
	}
}

func mintOnly(id uint64) [32]byte {
	var w [32]byte
	for i := range 8 {
		w[31-i] = byte(id >> (8 * i))
	}
	return w
}

// Slots are consensus constants and append-only; the accessors would agree with
// themselves whatever the numbers said.
func TestCAS20SlotNumbers(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  uint64
		want uint64
	}{
		{"core.name", cas20SlotName, 0},
		{"core.symbol", cas20SlotSymbol, 1},
		{"core.contractURI", cas20SlotContractURI, 2},
		{"core.totalSupply", cas20SlotTotalSupply, 3},
		{"core.balances", cas20SlotBalances, 4},
		{"core.allowances", cas20SlotAllowances, 5},
		{"core.roles", cas20SlotRoles, 6},
		{"core.roleAdmins", cas20SlotRoleAdmins, 7},
		{"core.adminCount", cas20SlotAdminCount, 8},
		{"core.transferPolicies", cas20SlotTransferPolicies, 9},
		{"core.mintPolicy", cas20SlotMintPolicy, 10},
		{"core.paused", cas20SlotPaused, 11},
		{"core.supplyCap", cas20SlotSupplyCap, 12},
		{"core.nonces", cas20SlotNonces, 13},
		{"core.seizePolicies", cas20SlotSeizePolicies, 14},

		{"activation.features", actSlotFeatures, 0},
		{"activation.admin", actSlotAdmin, 1},

		{"policy.policies", polSlotPolicies, 0},
		{"policy.members", polSlotMembers, 1},
		{"policy.pendingAdmins", polSlotPendingAdmins, 2},
		{"policy.counter", polSlotCounter, 3},

		{"asset.decimals", cas20AssetSlotDecimals, 0},
		{"asset.multiplier", cas20AssetSlotMultiplier, 1},
		{"asset.announcements", cas20AssetSlotAnnouncements, 2},
		{"asset.extraMeta", cas20AssetSlotExtraMeta, 3},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}

	for _, tc := range []struct {
		name string
		got  int
		want int
	}{
		{"transferSender", cas20OffTransferSender, 0},
		{"transferReceiver", cas20OffTransferReceiver, 8},
		{"transferExecutor", cas20OffTransferExecutor, 16},
		{"mintReceiver", cas20OffMintReceiver, 0},
		{"seizeExempt", cas20OffSeizeExempt, 0},
		{"seizeReceiver", cas20OffSeizeReceiver, 8},
	} {
		if tc.got != tc.want {
			t.Errorf("offset %s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}
}

// Bits 254:248 are where a later revision would add a flag.
func TestCAS20PolicyWordReservedBits(t *testing.T) {
	admin := common.HexToAddress("0xad4149")
	w := packPolicy(admin)

	if w[0] != 0x80 {
		t.Errorf("byte 0 = %#02x, want exactly 0x80: bit 255 set, bits 254:248 clear", w[0])
	}
	for i := 1; i < 12; i++ {
		if w[i] != 0 {
			t.Errorf("byte %d = %#02x, want zero (bits 247:160 are reserved)", i, w[i])
		}
	}
	if got := polWordAdmin(w); got != admin {
		t.Errorf("admin round-trip = %s, want %s", got.Hex(), admin.Hex())
	}
	if !polWordExists(w) {
		t.Error("packPolicy must set the existence bit")
	}
}

func TestCAS20PolicyLanePositions(t *testing.T) {
	if len(cas20PolicyLanes) != 6 {
		t.Fatalf("cas20PolicyLanes has %d entries, want the six scopes", len(cas20PolicyLanes))
	}
	tok := cas20Token{s: newTestStorage(t)}

	ids := map[common.Hash]uint64{
		scopeTransferSender:   0x11,
		scopeTransferReceiver: 0x22,
		scopeTransferExecutor: 0x33,
		scopeMintReceiver:     0x44,
		scopeSeizeExempt:      0x55,
		scopeSeizeReceiver:    0x66,
	}
	if len(ids) != len(cas20PolicyLanes) {
		t.Fatalf("this test names %d scopes, the table has %d", len(ids), len(cas20PolicyLanes))
	}
	seen := map[uint64]common.Hash{}
	for scope, lane := range cas20PolicyLanes {
		id, named := ids[scope]
		if !named {
			t.Fatalf("the table holds a scope this test does not name: %s", scope.Hex())
		}
		tok.s.setPackedU64(lane.slot, lane.byteOff, id)
		key := lane.slot<<8 | uint64(lane.byteOff)
		if other, dup := seen[key]; dup {
			t.Errorf("%s and %s share slot %d byte %d", scope.Hex(), other.Hex(),
				lane.slot, lane.byteOff)
		}
		seen[key] = scope
	}

	// Through the dispatcher and then straight from the word, so a self-consistently
	// wrong table entry still fails.
	for scope, want := range ids {
		got, ok := tok.policyIdByScope(scope)
		if !ok {
			t.Errorf("policyIdByScope(%s) reports unknown", scope.Hex())
			continue
		}
		if got != want {
			t.Errorf("policyIdByScope(%s) = %#x, want %#x", scope.Hex(), got, want)
		}
		lane := cas20PolicyLanes[scope]
		word := tok.s.getU256At(slotAt(lane.slot)).Bytes32()
		var raw uint64
		for _, b := range word[32-int(lane.byteOff)-8 : 32-int(lane.byteOff)] {
			raw = raw<<8 | uint64(b)
		}
		if raw != want {
			t.Errorf("%s: slot %d byte %d holds %#x, want %#x", scope.Hex(),
				lane.slot, lane.byteOff, raw, want)
		}
	}
}
