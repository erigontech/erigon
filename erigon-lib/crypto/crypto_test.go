// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package crypto

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/u256"
)

var testAddrHex = "970e8128ab834e8eac17ab8e3812f010678cf791"
var testPrivHex = "289c2857d4598e37fb9647507e47a309d6133539bf21a8b9cb6df88fd5232032"
var testPubkeyHex = "7db227d7094ce215c3a0f57e1bcc732551fe351f94249471934567e0f5dc1bf795962b8cccb87a2eb56b29fbe37d614e2f4c3c45b789ae4f1f51f4cb21972ffd"

// These tests are sanity checks.
// They should ensure that we don't e.g. use Sha3-224 instead of Sha3-256
// and that the sha3 library uses keccak-f permutation.
func TestKeccak256Hash(t *testing.T) {
	msg := []byte("abc")
	exp, _ := hex.DecodeString("4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45")
	checkhash(t, "Sha3-256-array", func(in []byte) []byte { h := Keccak256Hash(in); return h[:] }, msg, exp)
}

func TestKeccak256Hasher(t *testing.T) {
	msg := []byte("abc")
	exp, _ := hex.DecodeString("4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45")
	hasher := NewKeccakState()
	checkhash(t, "Sha3-256-array", func(in []byte) []byte { h := HashData(hasher, in); return h[:] }, msg, exp)
}

func TestKeccak256HasherNew(t *testing.T) {
	msg := []byte("abc")
	exp, _ := hex.DecodeString("3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532")
	hasher := sha3.New256()
	hasher.Write(msg)
	var h common.Hash
	if !bytes.Equal(exp, hasher.Sum(h[:0])) {
		t.Fatalf("hash %s mismatch: want: %x have: %x", "new", exp, h[:])
	}
}

func TestKeccak256HasherMulti(t *testing.T) {
	exp1, _ := hex.DecodeString("d341f310fa772d37e6966b84b37ad760811d784729b641630f6a03f729e1e20e")
	exp2, _ := hex.DecodeString("6de9c0166df098306abb98b112c0834c29eedee6fcba804c7c4f4568204c9d81")
	hasher := NewKeccakState()
	d1, _ := hex.DecodeString("1234")
	hasher.Write(d1)
	d2, _ := hex.DecodeString("cafe")
	hasher.Write(d2)
	d3, _ := hex.DecodeString("babe")
	hasher.Write(d3)
	checkhash(t, "multi1", func(in []byte) []byte { var h common.Hash; return hasher.Sum(h[:0]) }, []byte{}, exp1)
	d4, _ := hex.DecodeString("5678")
	hasher.Write(d4)
	checkhash(t, "multi2", func(in []byte) []byte { var h common.Hash; return hasher.Sum(h[:0]) }, []byte{}, exp2)
}

func TestToECDSAErrors(t *testing.T) {
	if _, err := HexToECDSA("0000000000000000000000000000000000000000000000000000000000000000"); err == nil {
		t.Fatal("HexToECDSA should've returned error")
	}
	if _, err := HexToECDSA("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"); err == nil {
		t.Fatal("HexToECDSA should've returned error")
	}
}

func BenchmarkSha3(b *testing.B) {
	a := []byte("hello world")
	for i := 0; i < b.N; i++ {
		Keccak256(a)
	}
}

func TestUnmarshalPubkey(t *testing.T) {
	key, err := UnmarshalPubkey(nil)
	if !errors.Is(err, errInvalidPubkey) || key != nil {
		t.Fatalf("expected error, got %v, %v", err, key)
	}
	key, err = UnmarshalPubkey([]byte{1, 2, 3})
	if !errors.Is(err, errInvalidPubkey) || key != nil {
		t.Fatalf("expected error, got %v, %v", err, key)
	}

	var (
		enc, _ = hex.DecodeString("760c4460e5336ac9bbd87952a3c7ec4363fc0a97bd31c86430806e287b437fd1b01abc6e1db640cf3106b520344af1d58b00b57823db3e1407cbc433e1b6d04d")
		dec    = &ecdsa.PublicKey{
			Curve: S256(),
			X:     hexutil.MustDecodeBig("0x760c4460e5336ac9bbd87952a3c7ec4363fc0a97bd31c86430806e287b437fd1"),
			Y:     hexutil.MustDecodeBig("0xb01abc6e1db640cf3106b520344af1d58b00b57823db3e1407cbc433e1b6d04d"),
		}
	)
	key, err = UnmarshalPubkey(enc)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !reflect.DeepEqual(key, dec) {
		t.Fatal("wrong result")
	}
}

func TestMarshalPubkey(t *testing.T) {
	check := func(privateKeyHex, expectedPubkeyHex string) {
		key, err := HexToECDSA(privateKeyHex)
		if err != nil {
			t.Errorf("bad private key: %s", err)
			return
		}
		pubkeyHex := hex.EncodeToString(MarshalPubkey(&key.PublicKey))
		if pubkeyHex != expectedPubkeyHex {
			t.Errorf("unexpected public key: %s", pubkeyHex)
		}
	}

	check(testPrivHex, testPubkeyHex)
	check(
		"36a7edad64d51a568b00e51d3fa8cd340aa704153010edf7f55ab3066ca4ef21",
		"24bfa2cdce7c6a41184fa0809ad8d76969b7280952e9aa46179d90cfbab90f7d2b004928f0364389a1aa8d5166281f2ff7568493c1f719e8f6148ef8cf8af42d",
	)
}

func TestSign(t *testing.T) {
	key, _ := HexToECDSA(testPrivHex)
	addr := common.HexToAddress(testAddrHex)

	msg := Keccak256([]byte("foo"))
	sig, err := Sign(msg, key)
	if err != nil {
		t.Errorf("Sign error: %s", err)
	}
	recoveredPub, err := Ecrecover(msg, sig)
	if err != nil {
		t.Errorf("ECRecover error: %s", err)
	}
	pubKey, _ := UnmarshalPubkeyStd(recoveredPub)
	recoveredAddr := PubkeyToAddress(*pubKey)
	if addr != recoveredAddr {
		t.Errorf("Address mismatch: want: %x have: %x", addr, recoveredAddr)
	}

	// should be equal to SigToPub
	recoveredPub2, err := SigToPub(msg, sig)
	if err != nil {
		t.Errorf("ECRecover error: %s", err)
	}
	recoveredAddr2 := PubkeyToAddress(*recoveredPub2)
	if addr != recoveredAddr2 {
		t.Errorf("Address mismatch: want: %x have: %x", addr, recoveredAddr2)
	}
}

func TestInvalidSign(t *testing.T) {
	if _, err := Sign(make([]byte, 1), nil); err == nil {
		t.Errorf("expected sign with hash 1 byte to error")
	}
	if _, err := Sign(make([]byte, 33), nil); err == nil {
		t.Errorf("expected sign with hash 33 byte to error")
	}
}

func TestLoadECDSA(t *testing.T) {
	tests := []struct {
		input string
		err   string
	}{
		// good
		{input: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		{input: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n"},
		{input: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n\r"},
		{input: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\r\n"},
		{input: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n\n"},
		{input: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n\r"},
		// bad
		{
			input: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde",
			err:   "key file too short, want 64 hex characters",
		},
		{
			input: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde\n",
			err:   "key file too short, want 64 hex characters",
		},
		{
			input: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeX",
			err:   "invalid hex character 'X' in private key",
		},
		{
			input: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdefX",
			err:   "invalid character 'X' at end of key file",
		},
		{
			input: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n\n\n",
			err:   "key file too long, want 64 hex characters",
		},
	}

	for _, test := range tests {
		f, err := os.CreateTemp("", "loadecdsa_test.*.txt")
		if err != nil {
			t.Fatal(err)
		}
		filename := f.Name()
		f.WriteString(test.input)
		f.Close()

		_, err = LoadECDSA(filename)
		switch {
		case err != nil && test.err == "":
			t.Fatalf("unexpected error for input %q:\n  %v", test.input, err)
		case err != nil && err.Error() != test.err:
			t.Fatalf("wrong error for input %q:\n  %v", test.input, err)
		case err == nil && test.err != "":
			t.Fatalf("LoadECDSA did not return error for input %q", test.input)
		}
	}
}

func TestSaveECDSA(t *testing.T) {
	f, err := os.CreateTemp("", "saveecdsa_test.*.txt")
	if err != nil {
		t.Fatal(err)
	}
	file := f.Name()
	f.Close()
	defer dir.RemoveFile(file)

	key, _ := HexToECDSA(testPrivHex)
	if e := SaveECDSA(file, key); e != nil {
		t.Fatal(e)
	}
	loaded, err := LoadECDSA(file)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(key, loaded) {
		t.Fatal("loaded key not equal to saved key")
	}
}

func TestTransactionSignatureIsValid(t *testing.T) {
	check := func(expected bool, v byte, r, s *uint256.Int) {
		if TransactionSignatureIsValid(v, r, s, true) != expected {
			t.Errorf("mismatch for v: %d r: %d s: %d want: %v", v, r, s, expected)
		}
	}
	minusOne := uint256.NewInt(0).SetAllOne()
	one := u256.N1
	zero := u256.N0
	secp256k1nMinus1 := new(uint256.Int).Sub(secp256k1N, u256.N1)

	// correct v,r,s
	check(true, 0, one, one)
	check(true, 1, one, one)
	// incorrect v, correct r,s,
	check(false, 2, one, one)
	check(false, 3, one, one)

	// incorrect v, combinations of incorrect/correct r,s at lower limit
	check(false, 2, zero, zero)
	check(false, 2, zero, one)
	check(false, 2, one, zero)
	check(false, 2, one, one)

	// correct v for any combination of incorrect r,s
	check(false, 0, zero, zero)
	check(false, 0, zero, one)
	check(false, 0, one, zero)

	check(false, 1, zero, zero)
	check(false, 1, zero, one)
	check(false, 1, one, zero)

	// correct sig with max r,s
	check(true, 0, secp256k1nMinus1, secp256k1nMinus1)
	// correct v, combinations of incorrect r,s at upper limit
	check(false, 0, secp256k1N, secp256k1nMinus1)
	check(false, 0, secp256k1nMinus1, secp256k1N)
	check(false, 0, secp256k1N, secp256k1N)

	// current callers ensures r,s cannot be negative, but let's test for that too
	// as crypto package could be used stand-alone
	check(false, 0, minusOne, one)
	check(false, 0, one, minusOne)
}

func checkhash(t *testing.T, name string, f func([]byte) []byte, msg, exp []byte) {
	t.Helper()
	sum := f(msg)
	if !bytes.Equal(exp, sum) {
		t.Fatalf("hash %s mismatch: want: %x have: %x", name, exp, sum)
	}
}

// test to help Python team with integration of libsecp256k1
// skip but keep it after they are done
func TestPythonIntegration(t *testing.T) {
	kh := "289c2857d4598e37fb9647507e47a309d6133539bf21a8b9cb6df88fd5232032"
	k0, _ := HexToECDSA(kh)

	msg0 := Keccak256([]byte("foo"))
	sig0, _ := Sign(msg0, k0)

	msg1 := hexutil.FromHex("0000000000000000000000000000000000000000000000000000000000000000")
	sig1, _ := Sign(msg1, k0)

	t.Logf("msg: %x, privkey: %s sig: %x\n", msg0, kh, sig0)
	t.Logf("msg: %x, privkey: %s sig: %x\n", msg1, kh, sig1)
}
