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
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
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
	authority, err := auth.RecoverSigner()
	if err != nil {
		t.Fatal(err)
	}
	expectedSigner := common.HexToAddress("0x8ED5ABe9DE62dB2F266b06b86203f71e4C1e357f")
	if authority != expectedSigner {
		t.Errorf("mismatch in recovered signer: got %v, want %v", authority, expectedSigner)
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
	if recovered != authority {
		t.Fatalf("unexpected authority: got %s, want %s", recovered, authority)
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
	if got, want := err.Error(), "authorization nonce has max value"; got != want {
		t.Fatalf("unexpected error: got %q, want %q", got, want)
	}
}

func TestSignAuthorizationRejectsNilKey(t *testing.T) {
	t.Parallel()

	_, err := SignAuthorization(nil, uint256.Int{}, common.Address{}, 0)
	if !errors.Is(err, errAuthNilPrivateKey) {
		t.Fatalf("expected nil private key to be rejected, got %v", err)
	}
}

func TestSignAuthorizationRejectsNilKeyBeforeMaxNonce(t *testing.T) {
	t.Parallel()

	_, err := SignAuthorization(nil, uint256.Int{}, common.Address{}, math.MaxUint64)
	if !errors.Is(err, errAuthNilPrivateKey) {
		t.Fatalf("expected nil private key to be rejected first, got %v", err)
	}
}

func TestRecoverSignersMatchesRecoverSigner(t *testing.T) {
	t.Parallel()

	auths := make([]Authorization, 100)
	for i := range auths {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		if auths[i], err = SignAuthorization(key, *uint256.NewInt(1), common.Address{byte(i)}, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	auths[3].Nonce = math.MaxUint64
	auths[50].YParity = 5
	auths[99].S.Clear()

	auths[7].ChainID = *uint256.NewInt(2)

	signers, errs := RecoverSigners(auths, uint256.NewInt(1))
	if signers[7] != (common.Address{}) || errs[7] != nil {
		t.Errorf("foreign-chain auth 7 was recovered: %v, %v", signers[7], errs[7])
	}
	for i := range auths {
		if i == 7 {
			continue
		}
		want, wantErr := auths[i].RecoverSigner()
		if signers[i] != want || (errs[i] == nil) != (wantErr == nil) {
			t.Errorf("auth %d: got %v, %v; want %v, %v", i, signers[i], errs[i], want, wantErr)
		}
	}
}

func TestSetCodeTxWarmAuthoritiesCaches(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	auth, err := SignAuthorization(key, *uint256.NewInt(1), common.Address{1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	tx := &SetCodeTransaction{Authorizations: []Authorization{auth}}
	if tx.authorities.Load() != nil {
		t.Fatal("authorities recovered before WarmAuthorities")
	}

	tx.WarmAuthorities(uint256.NewInt(1))
	got := tx.authorities.Load()
	if got == nil || got.Signers[0] != crypto.PubkeyToAddress(key.PublicKey) || got.Errs[0] != nil {
		t.Fatalf("recovered %+v, want %v", got, crypto.PubkeyToAddress(key.PublicKey))
	}
	tx.WarmAuthorities(uint256.NewInt(1))
	if tx.authorities.Load() != got {
		t.Error("a second WarmAuthorities replaced the published result")
	}
}

func TestMessageRecoveredAuthoritiesWaitsForInFlightRecovery(t *testing.T) {
	t.Parallel()

	tx := &SetCodeTransaction{}
	inFlight := &RecoveredAuthorities{done: make(chan struct{})}
	tx.authorities.Store(inFlight)
	msg := &Message{authorities: &tx.authorities}

	got := make(chan *RecoveredAuthorities)
	go func() { got <- msg.RecoveredAuthorities() }()
	select {
	case <-got:
		t.Fatal("returned before the in-flight recovery finished")
	case <-time.After(20 * time.Millisecond):
	}

	tx.WarmAuthorities(uint256.NewInt(1))
	inFlight.Signers, inFlight.Errs = []common.Address{{1}}, []error{nil}
	close(inFlight.done)
	if r := <-got; r != inFlight || r.Signers[0] != (common.Address{1}) {
		t.Fatalf("got %+v, want the in-flight result", r)
	}
}

func TestRecoverSignersNilChainIDIsChainZero(t *testing.T) {
	t.Parallel()

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := SignAuthorization(key, *uint256.NewInt(7), common.Address{1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	chain0, err := SignAuthorization(key, uint256.Int{}, common.Address{1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	signers, errs := RecoverSigners([]Authorization{foreign, chain0}, nil)
	if signers[0] != (common.Address{}) || errs[0] != nil {
		t.Errorf("foreign-chain auth was recovered: %v, %v", signers[0], errs[0])
	}
	if signers[1] != crypto.PubkeyToAddress(key.PublicKey) || errs[1] != nil {
		t.Errorf("chain-0 auth: got %v, %v", signers[1], errs[1])
	}
}
