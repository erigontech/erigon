// Copyright 2025 The Erigon Authors
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

package types

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/holiman/uint256"
)

// Tests that the correct signer is recovered from an Authorization object
// The data here was obtained from a pectra devnet
func TestRecoverSigner(t *testing.T) {
	t.Parallel()

	auth := Authorization{
		ChainID: *uint256.NewInt(7088110746),
		Address: common.Address{180, 125, 156, 99, 77, 80, 241, 96, 13, 77, 247, 103, 233, 71, 76, 37, 160, 48, 52, 40},
		Nonce:   1,
		YParity: 1,
		R:       uint256.Int{11238962557009670571, 14017651393191758745, 18358999445216475025, 5549385460848219779},
		S:       uint256.Int{6390522493159340108, 17630603794136184458, 14442462445950880280, 846710983706847255},
	}
	var b [32]byte
	data := bytes.NewBuffer(nil)
	authorityPtr, err := auth.RecoverSigner(data, b[:])
	if err != nil {
		t.Error(err)
	}
	expectedSigner := common.HexToAddress("0x8ED5ABe9DE62dB2F266b06b86203f71e4C1e357f")
	if *authorityPtr != expectedSigner {
		t.Errorf("mismatch in recovered signer: got %v, want %v", *authorityPtr, expectedSigner)
	}

}
