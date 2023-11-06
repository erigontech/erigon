// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package crypto

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"hash"
	"io"
	"math/big"
	"os"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon/crypto/cryptopool"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/rlp"
)

// SignatureLength indicates the byte length required to carry a signature with recovery id.
const SignatureLength = 64 + 1 // 64 bytes ECDSA signature + 1 byte recovery id

// RecoveryIDOffset points to the byte offset within the signature that contains the recovery id.
const RecoveryIDOffset = 64

// DigestLength sets the signature digest exact length
const DigestLength = 32

var (
	secp256k1N     = new(uint256.Int).SetBytes(hexutil.MustDecode("0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141"))
	secp256k1NBig  = secp256k1N.ToBig()
	secp256k1halfN = new(uint256.Int).Div(secp256k1N, u256.Num2)
)

var errInvalidPubkey = errors.New("invalid secp256k1 public key")

// KeccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type KeccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

// NewKeccakState creates a new KeccakState
func NewKeccakState() KeccakState {
	return cryptopool.NewLegacyKeccak256().(KeccakState)
}

// HashData hashes the provided data using the KeccakState and returns a 32 byte hash
func HashData(kh KeccakState, data []byte) (h libcommon.Hash) {
	kh.Reset()
	//nolint:errcheck
	kh.Write(data)
	//nolint:errcheck
	kh.Read(h[:])
	return h
}

// Keccak256 calculates and returns the Keccak256 hash of the input data.
func Keccak256(data ...[]byte) []byte {
	b := make([]byte, 32)
	d := NewKeccakState()
	for _, b := range data {
		d.Write(b)
	}
	d.Read(b) //nolint:errcheck
	cryptopool.ReturnToPoolKeccak256(d)
	return b
}

// Keccak256Hash calculates and returns the Keccak256 hash of the input data,
// converting it to an internal Hash data structure.
func Keccak256Hash(data ...[]byte) (h libcommon.Hash) {
	d := NewKeccakState()
	for _, b := range data {
		d.Write(b)
	}
	d.Read(h[:]) //nolint:errcheck
	cryptopool.ReturnToPoolKeccak256(d)
	return h
}

// Keccak512 calculates and returns the Keccak512 hash of the input data.
func Keccak512(data ...[]byte) []byte {
	d := sha3.NewLegacyKeccak512()
	for _, b := range data {
		d.Write(b)
	}
	return d.Sum(nil)
}

// CreateAddress creates an ethereum address given the bytes and the nonce
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func CreateAddress(b libcommon.Address, nonce uint64) libcommon.Address {
	data, _ := rlp.EncodeToBytes([]interface{}{b, nonce})
	return libcommon.BytesToAddress(Keccak256(data)[12:])
}

// CreateAddress2 creates an ethereum address given the address bytes, initial
// contract code hash and a salt.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func CreateAddress2(b libcommon.Address, salt [32]byte, inithash []byte) libcommon.Address {
	return libcommon.BytesToAddress(Keccak256([]byte{0xff}, b.Bytes(), salt[:], inithash)[12:])
}

// ToECDSA creates a private key with the given D value.
func ToECDSA(d []byte) (*ecdsa.PrivateKey, error) {
	return toECDSA(d, true)
}

// ToECDSAUnsafe blindly converts a binary blob to a private key. It should almost
// never be used unless you are sure the input is valid and want to avoid hitting
// errors due to bad origin encoding (0 prefixes cut off).
func ToECDSAUnsafe(d []byte) *ecdsa.PrivateKey {
	priv, _ := toECDSA(d, false)
	return priv
}

// toECDSA creates a private key with the given D value. The strict parameter
// controls whether the key's length should be enforced at the curve size or
// it can also accept legacy encodings (0 prefixes).
func toECDSA(d []byte, strict bool) (*ecdsa.PrivateKey, error) {
	priv := new(ecdsa.PrivateKey)
	priv.PublicKey.Curve = S256()
	if strict && 8*len(d) != priv.Params().BitSize {
		return nil, fmt.Errorf("invalid length, need %d bits", priv.Params().BitSize)
	}
	priv.D = new(big.Int).SetBytes(d)

	// The priv.D must < N
	if priv.D.Cmp(secp256k1NBig) >= 0 {
		return nil, fmt.Errorf("invalid private key, >=N")
	}
	// The priv.D must not be zero or negative.
	if priv.D.Sign() <= 0 {
		return nil, fmt.Errorf("invalid private key, zero or negative")
	}

	priv.PublicKey.X, priv.PublicKey.Y = priv.PublicKey.Curve.ScalarBaseMult(d)
	if priv.PublicKey.X == nil {
		return nil, errors.New("invalid private key")
	}
	return priv, nil
}

// FromECDSA exports a private key into a binary dump.
func FromECDSA(priv *ecdsa.PrivateKey) []byte {
	if priv == nil {
		return nil
	}
	return math.PaddedBigBytes(priv.D, priv.Params().BitSize/8)
}

// UnmarshalPubkeyStd parses a public key from the given bytes in the standard "uncompressed" format.
// The input slice must be 65 bytes long and have this format: [4, X..., Y...]
// See MarshalPubkeyStd.
func UnmarshalPubkeyStd(pub []byte) (*ecdsa.PublicKey, error) {
	x, y := elliptic.Unmarshal(S256(), pub)
	if x == nil {
		return nil, errInvalidPubkey
	}
	return &ecdsa.PublicKey{Curve: S256(), X: x, Y: y}, nil
}

// MarshalPubkeyStd converts a public key into the standard "uncompressed" format.
// It returns a 65 bytes long slice that contains: [4, X..., Y...]
// Returns nil if the given public key is not initialized.
// See UnmarshalPubkeyStd.
func MarshalPubkeyStd(pub *ecdsa.PublicKey) []byte {
	if pub == nil || pub.X == nil || pub.Y == nil {
		return nil
	}
	return elliptic.Marshal(S256(), pub.X, pub.Y)
}

// UnmarshalPubkey parses a public key from the given bytes in the 64 bytes "uncompressed" format.
// The input slice must be 64 bytes long and have this format: [X..., Y...]
// See MarshalPubkey.
func UnmarshalPubkey(keyBytes []byte) (*ecdsa.PublicKey, error) {
	keyBytes = append([]byte{0x4}, keyBytes...)
	return UnmarshalPubkeyStd(keyBytes)
}

// MarshalPubkey converts a public key into a 64 bytes "uncompressed" format.
// It returns a 64 bytes long slice that contains: [X..., Y...]
// In the standard 65 bytes format the first byte is always constant (equal to 4),
// so it can be cut off and trivially recovered later.
// Returns nil if the given public key is not initialized.
// See UnmarshalPubkey.
func MarshalPubkey(pubkey *ecdsa.PublicKey) []byte {
	keyBytes := MarshalPubkeyStd(pubkey)
	if keyBytes == nil {
		return nil
	}
	return keyBytes[1:]
}

// HexToECDSA parses a secp256k1 private key.
func HexToECDSA(hexkey string) (*ecdsa.PrivateKey, error) {
	b, err := hex.DecodeString(hexkey)
	if byteErr, ok := err.(hex.InvalidByteError); ok {
		return nil, fmt.Errorf("invalid hex character %q in private key", byte(byteErr))
	} else if err != nil {
		return nil, errors.New("invalid hex data for private key")
	}
	return ToECDSA(b)
}

// LoadECDSA loads a secp256k1 private key from the given file.
func LoadECDSA(file string) (*ecdsa.PrivateKey, error) {
	fd, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	r := bufio.NewReader(fd)
	buf := make([]byte, 64)
	n, err := readASCII(buf, r)
	if err != nil {
		return nil, err
	} else if n != len(buf) {
		return nil, fmt.Errorf("key file too short, want 64 hex characters")
	}
	if err := checkKeyFileEnd(r); err != nil {
		return nil, err
	}

	return HexToECDSA(string(buf))
}

// readASCII reads into 'buf', stopping when the buffer is full or
// when a non-printable control character is encountered.
func readASCII(buf []byte, r *bufio.Reader) (n int, err error) {
	for ; n < len(buf); n++ {
		buf[n], err = r.ReadByte()
		switch {
		case errors.Is(err, io.EOF) || buf[n] < '!':
			return n, nil
		case err != nil:
			return n, err
		}
	}
	return n, nil
}

// checkKeyFileEnd skips over additional newlines at the end of a key file.
func checkKeyFileEnd(r *bufio.Reader) error {
	for i := 0; ; i++ {
		b, err := r.ReadByte()
		switch {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return err
		case b != '\n' && b != '\r':
			return fmt.Errorf("invalid character %q at end of key file", b)
		case i >= 2:
			return errors.New("key file too long, want 64 hex characters")
		}
	}
}

// SaveECDSA saves a secp256k1 private key to the given file with
// restrictive permissions. The key data is saved hex-encoded.
func SaveECDSA(file string, key *ecdsa.PrivateKey) error {
	k := hex.EncodeToString(FromECDSA(key))
	return os.WriteFile(file, []byte(k), 0600)
}

// GenerateKey generates a new private key.
func GenerateKey() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(S256(), rand.Reader)
}

// ValidateSignatureValues verifies whether the signature values are valid with
// the given chain rules. The v value is assumed to be either 0 or 1.
func ValidateSignatureValues(v byte, r, s *uint256.Int, homestead bool) bool {
	if r.IsZero() || s.IsZero() {
		return false
	}
	// reject upper range of s values (ECDSA malleability)
	// see discussion in secp256k1/libsecp256k1/include/secp256k1.h
	if homestead && s.Gt(secp256k1halfN) {
		return false
	}
	// Frontier: allow s to be in full N range
	return r.Lt(secp256k1N) && s.Lt(secp256k1N) && (v == 0 || v == 1)
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func PubkeyToAddress(p ecdsa.PublicKey) libcommon.Address {
	pubBytes := MarshalPubkey(&p)
	return libcommon.BytesToAddress(Keccak256(pubBytes)[12:])
}

func zeroBytes(bytes []byte) {
	for i := range bytes {
		bytes[i] = 0
	}
}
