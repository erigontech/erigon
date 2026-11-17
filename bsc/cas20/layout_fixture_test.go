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
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
)

// BEP-702 3.17 is generated from testdata/cas20_layout.json. This pins agreement
// with the code, not values: a rename or renumbering fails here and says to
// regenerate the fixture.
func TestCAS20LayoutFixtureFollowsTheCode(t *testing.T) {
	live := map[string]map[string]uint64{
		cas20Namespace: {
			"name": cas20SlotName, "symbol": cas20SlotSymbol, "contractURI": cas20SlotContractURI,
			"totalSupply": cas20SlotTotalSupply, "balances": cas20SlotBalances,
			"allowances": cas20SlotAllowances, "roles": cas20SlotRoles,
			"roleAdmins": cas20SlotRoleAdmins, "adminCount": cas20SlotAdminCount,
			"transferPolicies": cas20SlotTransferPolicies, "mintPolicy": cas20SlotMintPolicy,
			"paused": cas20SlotPaused, "supplyCap": cas20SlotSupplyCap,
			"nonces": cas20SlotNonces, "seizePolicies": cas20SlotSeizePolicies,
		},
		cas20AssetNamespace: {
			"decimals": cas20AssetSlotDecimals, "multiplier": cas20AssetSlotMultiplier,
			"announcements": cas20AssetSlotAnnouncements, "extraMetadata": cas20AssetSlotExtraMeta,
			"pendingMultiplier": cas20AssetSlotPending,
		},
		cas20StablecoinNamespace: {"currency": cas20StablecoinSlotCurrency},
		cas20PolicyNamespace: {
			"policies": polSlotPolicies, "members": polSlotMembers,
			"pendingAdmins": polSlotPendingAdmins, "counter": polSlotCounter,
			"children": polSlotChildren,
		},
		cas20ActivationNamespace: {"features": actSlotFeatures, "admin": actSlotAdmin},
	}

	var ref struct {
		Namespaces []struct {
			Name   string `json:"name"`
			Root   string `json:"root"`
			Fields []struct {
				Slot uint64 `json:"slot"`
				Name string `json:"name"`
			} `json:"fields"`
		} `json:"namespaces"`
		Derivation struct {
			StringMaxLen uint64 `json:"string_max_len"`
		} `json:"derivation"`
	}
	raw, err := os.ReadFile("testdata/cas20_layout.json")
	if err != nil {
		t.Fatalf("read the layout fixture: %v", err)
	}
	if err := json.Unmarshal(raw, &ref); err != nil {
		t.Fatalf("parse the layout fixture: %v", err)
	}

	const regen = "update the fixture, and BEP-702 3.17 with it"

	seenNS := map[string]bool{}
	for _, ns := range ref.Namespaces {
		fields, known := live[ns.Name]
		if !known {
			t.Errorf("the fixture names namespace %q, which no constant declares — %s",
				ns.Name, regen)
			continue
		}
		seenNS[ns.Name] = true
		if want := erc7201Root(ns.Name).Hex(); ns.Root != want {
			t.Errorf("namespace %q: fixture root %s, the code derives %s — %s",
				ns.Name, ns.Root, want, regen)
		}
		seen := map[string]bool{}
		for _, f := range ns.Fields {
			seen[f.Name] = true
			slot, ok := fields[f.Name]
			if !ok {
				t.Errorf("%s: the fixture names field %q, which no constant declares — %s",
					ns.Name, f.Name, regen)
			} else if slot != f.Slot {
				t.Errorf("%s.%s is slot %d in the fixture and %d in the code — %s",
					ns.Name, f.Name, f.Slot, slot, regen)
			}
		}
		var missing []string
		for name := range fields {
			if !seen[name] {
				missing = append(missing, name)
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			t.Errorf("%s: the fixture does not document %s. An undocumented slot is one a "+
				"reimplementation cannot place — %s", ns.Name, strings.Join(missing, ", "), regen)
		}
	}
	for ns := range live {
		if !seenNS[ns] {
			t.Errorf("namespace %q is absent from the fixture, so BEP-702 3.17 will not "+
				"mention it — %s", ns, regen)
		}
	}
	if ref.Derivation.StringMaxLen != cas20MaxStringLen {
		t.Errorf("the fixture caps a string at %d, the code at %d — %s",
			ref.Derivation.StringMaxLen, cas20MaxStringLen, regen)
	}
}
