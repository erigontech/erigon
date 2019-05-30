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

package types

import (
	"fmt"
	"hash"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"golang.org/x/crypto/sha3"
)

type bytesBacked interface {
	Bytes() []byte
}

const (
	// BloomByteLength represents the number of bytes used in a header log bloom.
	BloomByteLength = 256

	// BloomBitLength represents the number of bits used in a header log bloom.
	BloomBitLength = 8 * BloomByteLength
)

// Bloom represents a 2048 bit bloom filter.
type Bloom [BloomByteLength]byte

// BytesToBloom converts a byte slice to a bloom filter.
// It panics if b is not of suitable size.
func BytesToBloom(b []byte) Bloom {
	var bloom Bloom
	bloom.SetBytes(b)
	return bloom
}

// SetBytes sets the content of b to the given bytes.
// It panics if d is not of suitable size.
func (b *Bloom) SetBytes(d []byte) {
	if len(b) < len(d) {
		panic(fmt.Sprintf("bloom bytes too big %d %d", len(b), len(d)))
	}
	copy(b[BloomByteLength-len(d):], d)
}

// Add adds d to the filter. Future calls of Test(d) will return true.
func (b *Bloom) Add(d *big.Int) {
	bin := new(big.Int).SetBytes(b[:])
	bin.Or(bin, Bloom9(d.Bytes()))
	b.SetBytes(bin.Bytes())
}

// Big converts b to a big integer.
func (b Bloom) Big() *big.Int {
	return new(big.Int).SetBytes(b[:])
}

func (b Bloom) Bytes() []byte {
	return b[:]
}

func (b Bloom) Test(test *big.Int) bool {
	return BloomLookup(b, test)
}

func (b Bloom) TestBytes(test []byte) bool {
	return b.Test(new(big.Int).SetBytes(test))

}

// MarshalText encodes b as a hex string with 0x prefix.
func (b Bloom) MarshalText() ([]byte, error) {
	return hexutil.Bytes(b[:]).MarshalText()
}

// UnmarshalText b as a hex string with 0x prefix.
func (b *Bloom) UnmarshalText(input []byte) error {
	return hexutil.UnmarshalFixedText("Bloom", input, b[:])
}

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

func CreateBloom(receipts Receipts) Bloom {
	var buf [6]byte
	sha3 := sha3.NewLegacyKeccak256().(keccakState)
	bin := new(big.Int)
	for _, receipt := range receipts {
		bin.Or(bin, logsBloom(receipt.Logs, buf, sha3))
	}

	return BytesToBloom(bin.Bytes())
}

func LogsBloom(logs []*Log) *big.Int {
	var buf [6]byte
	h := sha3.NewLegacyKeccak256().(keccakState)
	return logsBloom(logs, buf, h)
}

func logsBloom(logs []*Log, buf [6]byte, h keccakState) *big.Int {
	bin := new(big.Int)
	for _, log := range logs {
		bin.Or(bin, bloom9(log.Address.Bytes(), buf, h))
		for _, b := range log.Topics {
			bin.Or(bin, bloom9(b[:], buf, h))
		}
	}

	return bin
}

func bloom9(b []byte, buf [6]byte, h keccakState) *big.Int {
	h.Reset()
	h.Write(b[:])
	h.Read(buf[:]) // It only uses 6 bytes

	r := new(big.Int)

	for i := 0; i < 6; i += 2 {
		t := big.NewInt(1)
		bt := (uint(buf[i+1]) + (uint(buf[i]) << 8)) & 2047
		r.Or(r, t.Lsh(t, bt))
	}

	return r
}

func Bloom9(b []byte) *big.Int {
	var buf [6]byte
	h := sha3.NewLegacyKeccak256().(keccakState)
	return bloom9(b, buf, h)
}

func BloomLookup(bin Bloom, topic bytesBacked) bool {
	bloom := bin.Big()
	cmp := Bloom9(topic.Bytes())
	return bloom.And(bloom, cmp).Cmp(cmp) == 0
}
