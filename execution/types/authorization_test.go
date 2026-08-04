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
	"errors"
	"math"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/holiman/uint256"
)

// The fixed vector pins the signing preimage independently of SignAuthorization:
// a bug in the shared encoding keeps the round-trip test passing but changes
// the address recovered from this externally signed vector.
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
	authorityPtr, err := auth.RecoverSigner()
	if err != nil {
		t.Fatal(err)
	}
	expectedSigner := common.HexToAddress("0x8ED5ABe9DE62dB2F266b06b86203f71e4C1e357f")
	if *authorityPtr != expectedSigner {
		t.Errorf("mismatch in recovered signer: got %v, want %v", *authorityPtr, expectedSigner)
	}
}

func TestSignAuthorizationRoundTrip(t *testing.T) {
	t.Parallel()

	privateKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	authority := crypto.PubkeyToAddress(privateKey.PublicKey)
	// Keep the delegation target distinct from the authority so the test checks
	// both roles independently.
	delegationTarget := authority
	delegationTarget[0] ^= 0xff
	chainID := *uint256.NewInt(7078815900)
	const nonce = uint64(7)

	auth, err := SignAuthorization(privateKey, chainID, delegationTarget, nonce)
	if err != nil {
		t.Fatal(err)
	}
	if auth.ChainID != chainID {
		t.Fatalf("unexpected chain ID: got %s, want %s", auth.ChainID.String(), chainID.String())
	}
	if auth.Address != delegationTarget {
		t.Fatalf("unexpected delegation target: got %s, want %s", auth.Address, delegationTarget)
	}
	if auth.Nonce != nonce {
		t.Fatalf("unexpected nonce: got %d, want %d", auth.Nonce, nonce)
	}

	recovered, err := auth.RecoverSigner()
	if err != nil {
		t.Fatal(err)
	}
	if *recovered != authority {
		t.Fatalf("unexpected authority: got %s, want %s", *recovered, authority)
	}
}

func BenchmarkAuthorizationRecoverSigner(b *testing.B) {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		b.Fatal(err)
	}
	auth, err := SignAuthorization(privateKey, *uint256.NewInt(7078815900), common.Address{0xaa}, 7)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := auth.RecoverSigner(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestSignAuthorizationRejectsMaxNonce(t *testing.T) {
	t.Parallel()

	privateKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}

	_, err = SignAuthorization(privateKey, uint256.Int{}, common.Address{}, math.MaxUint64)
	if !errors.Is(err, errAuthNonceOverflow) {
		t.Fatalf("expected maximum nonce to be rejected, got %v", err)
	}
}

func TestSignAuthorizationRejectsNilKey(t *testing.T) {
	t.Parallel()

	_, err := SignAuthorization(nil, uint256.Int{}, common.Address{}, 0)
	if err == nil {
		t.Fatal("expected nil private key to be rejected")
	}
}
