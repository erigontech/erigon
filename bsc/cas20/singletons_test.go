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

	"github.com/erigontech/erigon/common"
)

// Consensus constants (BEP-702 3.1); every other test reaches them through the variables.
func TestCAS20SingletonAddresses(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  common.Address
		want string
	}{
		{"CAS20Factory", CAS20FactoryAddress, "0xCA5F000000000000000000000000000000000000"},
		{"ActivationRegistry", CAS20ActivationRegistryAddress, "0x7020000000000000000000000000000000000001"},
		{"PolicyRegistry", CAS20PolicyRegistryAddress, "0x7020000000000000000000000000000000000002"},
	} {
		if want := common.HexToAddress(tc.want); tc.got != want {
			t.Errorf("%s = %s, want %s", tc.name, tc.got.Hex(), want.Hex())
		}
	}

	// Or creating a token could collide with the factory itself.
	if IsCAS20Address(CAS20FactoryAddress) {
		t.Error("the factory must fall outside the reserved token space")
	}
	for _, reg := range []common.Address{CAS20ActivationRegistryAddress, CAS20PolicyRegistryAddress} {
		if IsCAS20Address(reg) {
			t.Errorf("%s must fall outside the reserved token space", reg.Hex())
		}
	}
}
