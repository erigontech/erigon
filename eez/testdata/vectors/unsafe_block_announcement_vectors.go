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

//go:build ignore

// Command unsafe_block_announcement_vectors writes the App. D.6
// unsafe-block announcement vectors without Erigon's signing, hashing or RLP
// code: decred secp256k1 (RFC 6979 signatures, recovery),
// golang.org/x/crypto/sha3 Keccak-256 and RLP written out here. From the
// repository root:
//
//	go run ./eez/testdata/vectors/unsafe_block_announcement_vectors.go > eez/testdata/vectors/unsafe_block_announcement_vectors.json
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"golang.org/x/crypto/sha3"
)

const domain = "EEZ_ROLLUP0_UNSAFE_BLOCK_V1"

func keccak(parts ...[]byte) []byte {
	h := sha3.NewLegacyKeccak256()
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum(nil)
}

func word(v *big.Int) []byte {
	out := make([]byte, 32)
	v.FillBytes(out)
	return out
}

// rlpString encodes b as an RLP string.
func rlpString(b []byte) []byte {
	if len(b) == 1 && b[0] < 0x80 {
		return []byte{b[0]}
	}
	return append(header(0x80, len(b)), b...)
}

func header(base byte, n int) []byte {
	if n < 56 {
		return []byte{base + byte(n)}
	}
	l := big.NewInt(int64(n)).Bytes()
	return append([]byte{base + 55 + byte(len(l))}, l...)
}

// rlpUint encodes v as a canonical RLP integer.
func rlpUint(v *big.Int) []byte { return rlpString(v.Bytes()) }

func rlpList(items ...[]byte) []byte {
	var payload []byte
	for _, it := range items {
		payload = append(payload, it...)
	}
	return append(header(0xc0, len(payload)), payload...)
}

func message(version, chainID *big.Int, hash []byte) []byte {
	tag := keccak([]byte(domain))
	return keccak(tag, word(version), word(chainID), hash)
}

// sign returns r||s||v, v in {27, 28}.
func sign(key *secp256k1.PrivateKey, msg []byte) []byte {
	c := ecdsa.SignCompact(key, msg, false)
	out := make([]byte, 65)
	copy(out, c[1:])
	out[64] = c[0]
	return out
}

func address(pub *secp256k1.PublicKey) string {
	u := pub.SerializeUncompressed()
	return "0x" + hex.EncodeToString(keccak(u[1:])[12:])
}

// recoverAddr recovers the signer of r||s||v over msg.
func recoverAddr(sig, msg []byte) (string, error) {
	c := append([]byte{sig[64]}, sig[:64]...)
	pub, _, err := ecdsa.RecoverCompact(c, msg)
	if err != nil {
		return "", err
	}
	return address(pub), nil
}

func hx(b []byte) string { return "0x" + hex.EncodeToString(b) }

type valid struct {
	Name       string `json:"name"`
	PrivateKey string `json:"privateKey,omitempty"`
	Address    string `json:"address"`
	ChainID    string `json:"chainId"`
	BlockHash  string `json:"blockHash"`
	Message    string `json:"message"`
	Signature  string `json:"signature"`
	Envelope   string `json:"envelope"`
}

type invalid struct {
	Name      string `json:"name"`
	ChainID   string `json:"chainId"`
	BlockHash string `json:"blockHash"`
	Envelope  string `json:"envelope"`
}

type file struct {
	Spec      string    `json:"spec"`
	Domain    string    `json:"domain"`
	DomainTag string    `json:"domainTag"`
	Valid     []valid   `json:"valid"`
	Invalid   []invalid `json:"invalid"`
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	keyBytes, err := hex.DecodeString("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
	must(err)
	key := secp256k1.PrivKeyFromBytes(keyBytes)
	addr := address(key.PubKey())
	if addr != "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266" {
		panic(addr)
	}
	zero := big.NewInt(0)
	seq := make([]byte, 32)
	for i := range seq {
		seq[i] = byte(i)
	}
	emptyHash := keccak()
	big64 := new(big.Int).Lsh(big.NewInt(1), 64)

	envelope := func(version, chainID *big.Int, hash, sig []byte) []byte {
		return rlpList(rlpUint(version), rlpUint(chainID), rlpString(hash), rlpString(sig))
	}
	out := file{Spec: "specs-draft 2c6d346 docs/rollup0-spec/D-wire-formats.md, D.6", Domain: domain, DomainTag: hx(keccak([]byte(domain)))}
	add := func(name string, chainID *big.Int, hash []byte) []byte {
		msg := message(zero, chainID, hash)
		sig := sign(key, msg)
		got, err := recoverAddr(sig, msg)
		must(err)
		if got != addr {
			panic(name)
		}
		env := envelope(zero, chainID, hash, sig)
		out.Valid = append(out.Valid, valid{
			Name: name, PrivateKey: hx(keyBytes), Address: addr, ChainID: "0x" + chainID.Text(16), BlockHash: hx(hash),
			Message: hx(msg), Signature: hx(sig), Envelope: hx(env),
		})
		return sig
	}
	devnet := big.NewInt(6290)
	sig := add("eez-devnet chain", devnet, seq)
	add("chain id 1", big.NewInt(1), emptyHash)
	add("chain id 2^64", big64, seq)

	// The same r and s with the other v recover another key: under App. D.6
	// that is a valid announcement by another producer.
	msg := message(zero, devnet, seq)
	flipped := append([]byte{}, sig...)
	flipped[64] = 55 - sig[64]
	other, err := recoverAddr(flipped, msg)
	must(err)
	if other == addr {
		panic("flipped v recovers the key")
	}
	out.Valid = append(out.Valid, valid{
		Name: "v flipped: another producer", Address: other, ChainID: "0x" + devnet.Text(16), BlockHash: hx(seq),
		Message: hx(msg), Signature: hx(flipped), Envelope: hx(envelope(zero, devnet, seq, flipped)),
	})

	n := secp256k1.Params().N
	p := secp256k1.Params().P
	bad := func(name string, env []byte) {
		out.Invalid = append(out.Invalid, invalid{Name: name, ChainID: "0x" + devnet.Text(16), BlockHash: hx(seq), Envelope: hx(env)})
	}
	withSig := func(f func(s []byte)) []byte {
		s := append([]byte{}, sig...)
		f(s)
		return envelope(zero, devnet, seq, s)
	}
	// Each case breaks one rule; the rest of the envelope is as signed.
	v1msg := message(big.NewInt(1), devnet, seq)
	bad("version 1", envelope(big.NewInt(1), devnet, seq, sign(key, v1msg)))
	bad("version 0 as byte 0x00", rlpList([]byte{0x00}, rlpUint(devnet), rlpString(seq), rlpString(sig)))
	bad("chain id with a leading zero", rlpList(rlpUint(zero), rlpString([]byte{0x00, 0x18, 0x92}), rlpString(seq), rlpString(sig)))
	bad("chain id of 33 bytes", rlpList(rlpUint(zero), rlpUint(new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 256), devnet)), rlpString(seq), rlpString(sig)))
	bad("chain id as a list", rlpList(rlpUint(zero), rlpList(rlpUint(devnet)), rlpString(seq), rlpString(sig)))
	otherChain := big.NewInt(6291)
	bad("another chain", envelope(zero, otherChain, seq, sign(key, message(zero, otherChain, seq))))
	otherHash := keccak(seq)
	bad("another block", envelope(zero, devnet, otherHash, sign(key, message(zero, devnet, otherHash))))
	bad("block hash of 31 bytes", rlpList(rlpUint(zero), rlpUint(devnet), rlpString(seq[:31]), rlpString(sig)))
	bad("block hash with a long-form header", rlpList(rlpUint(zero), rlpUint(devnet), append([]byte{0xb8, 0x20}, seq...), rlpString(sig)))
	bad("block hash of 33 bytes", rlpList(rlpUint(zero), rlpUint(devnet), rlpString(append(append([]byte{}, seq...), 0)), rlpString(sig)))
	bad("signature of 64 bytes", rlpList(rlpUint(zero), rlpUint(devnet), rlpString(seq), rlpString(sig[:64])))
	bad("signature of 66 bytes", rlpList(rlpUint(zero), rlpUint(devnet), rlpString(seq), rlpString(append(append([]byte{}, sig...), 0))))
	for _, v := range []byte{0, 1, 29} {
		bad(fmt.Sprintf("v %d", v), withSig(func(s []byte) { s[64] = v }))
	}
	// N - s with the other v is the high-s twin: it recovers the same key.
	highS := withSig(func(s []byte) {
		copy(s[32:64], word(new(big.Int).Sub(n, new(big.Int).SetBytes(s[32:64]))))
		s[64] = 55 - s[64]
	})
	bad("high s", highS)
	bad("r zero", withSig(func(s []byte) { clear(s[:32]) }))
	bad("s zero", withSig(func(s []byte) { clear(s[32:64]) }))
	bad("r equal to the curve order", withSig(func(s []byte) { copy(s[:32], word(n)) }))
	// The least x above r with no curve point: nothing recovers from it.
	x := new(big.Int).SetBytes(sig[:32])
	for {
		x.Add(x, big.NewInt(1))
		y2 := new(big.Int).Exp(x, big.NewInt(3), p)
		y2.Add(y2, big.NewInt(7)).Mod(y2, p)
		if new(big.Int).ModSqrt(y2, p) == nil {
			break
		}
	}
	bad("r off the curve", withSig(func(s []byte) { copy(s[:32], word(x)) }))
	items := [][]byte{rlpUint(zero), rlpUint(devnet), rlpString(seq), rlpString(sig)}
	bad("three items", rlpList(items[:3]...))
	bad("five items", rlpList(append(append([][]byte{}, items...), rlpUint(zero))...))
	valid0 := rlpList(items...)
	bad("trailing byte", append(append([]byte{}, valid0...), 0x80))
	payload := valid0[2:]
	bad("non-canonical list length", append([]byte{0xf9, 0x00, byte(len(payload))}, payload...))
	bad("a string, not a list", rlpString(payload))
	bad("empty", []byte{})

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	must(enc.Encode(out))
}
