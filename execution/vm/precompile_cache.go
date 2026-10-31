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

package vm

import (
	"bytes"
	"hash/maphash"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	libkzg "github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/cache"
)

const (
	maxCachedPrecompileInput = 64 * 1024
)

var (
	precompileCacheEnabled = dbg.EnvBool("PRECOMPILE_CACHE", true)

	precompileSeed, precompileCheckSeed = maphash.MakeSeed(), maphash.MakeSeed()
	precompileResults                   = cache.NewByteLRU(8*datasize.MB, func(_ uint64, r precompileResult) int64 {
		return int64(unsafe.Sizeof(r)) + int64(len(r.output)) + cache.ByteLRUEntryOverheadBytes
	})
)

// precompileResult identifies its input by a second hash with an independent per-process seed: the
// 128 bits of key and check cannot be targeted from outside, and the input is not kept, so entries stay small.
type precompileResult struct {
	domain [21]byte
	check  uint64
	output []byte
}

type cachedPrecompile struct {
	PrecompiledContract
	normalize  func(input []byte) ([]byte, bool)
	domain     [21]byte
	domainHash uint64
}

func cachePrecompile(p PrecompiledContract, tier forkTier, address common.Address) PrecompiledContract {
	normalize := precompileCacheKey(p)
	if !precompileCacheEnabled || normalize == nil {
		return p
	}
	var domain [21]byte
	domain[0] = byte(tier)
	copy(domain[1:], address[:])
	return newCachedPrecompile(p, normalize, domain)
}

func newCachedPrecompile(p PrecompiledContract, normalize func([]byte) ([]byte, bool), domain [21]byte) *cachedPrecompile {
	return &cachedPrecompile{PrecompiledContract: p, normalize: normalize, domain: domain, domainHash: maphash.Bytes(precompileSeed, domain[:])}
}

func (c *cachedPrecompile) Run(input []byte) ([]byte, error) {
	in, ok := c.normalize(input)
	// Below its key length a call costs less than the insert a miss pays, and the precompiles that
	// zero-pad answer an all-zero input without computing.
	if !ok || len(in) == 0 || len(in) > maxCachedPrecompileInput || c.RequiredGas(input) < uint64(len(in)) {
		return c.PrecompiledContract.Run(input)
	}
	key, check := maphash.Bytes(precompileSeed, in)^c.domainHash, maphash.Bytes(precompileCheckSeed, in)
	if r, ok := precompileResults.Get(key); ok && r.domain == c.domain && r.check == check {
		return bytes.Clone(r.output), nil
	}
	output, err := c.PrecompiledContract.Run(input)
	if err == nil && len(output) <= 1024 {
		precompileResults.Add(key, precompileResult{domain: c.domain, check: check, output: bytes.Clone(output)})
	}
	return output, err
}

// precompileCacheKey returns how a cached precompile narrows its input to the bytes that decide the
// result (inputs that narrow alike must run to the same output, and false rejects a length Run fails
// on), or nil for a precompile that is not cached.
func precompileCacheKey(p PrecompiledContract) func([]byte) ([]byte, bool) {
	switch p.(type) {
	case *ecrecover:
		return zeroPaddedKey(128)
	case *bigModExp:
		return modExpKey
	case *bn254AddIstanbul, *bn254AddByzantium:
		return zeroPaddedKey(128)
	case *bn254ScalarMulIstanbul, *bn254ScalarMulByzantium:
		return zeroPaddedKey(96)
	case *bn254PairingIstanbul, *bn254PairingByzantium:
		return func(input []byte) ([]byte, bool) { return input, len(input)%192 == 0 }
	case *blake2F:
		return lengthKey(blake2FInputLength)
	case *bls12381G1Add:
		return lengthKey(256)
	case *bls12381G2Add:
		return lengthKey(512)
	case *bls12381G1MultiExp:
		return lengthMultipleKey(160)
	case *bls12381G2MultiExp:
		return lengthMultipleKey(288)
	case *bls12381Pairing:
		return lengthMultipleKey(384)
	case *bls12381MapFpToG1:
		return lengthKey(64)
	case *bls12381MapFp2ToG2:
		return lengthKey(128)
	case *p256Verify:
		return lengthKey(160)
	case *pointEvaluation:
		return lengthKey(libkzg.PrecompileInputLength)
	default:
		return nil
	}
}

func lengthKey(n int) func([]byte) ([]byte, bool) {
	return func(input []byte) ([]byte, bool) { return input, len(input) == n }
}

func lengthMultipleKey(n int) func([]byte) ([]byte, bool) {
	return func(input []byte) ([]byte, bool) { return input, len(input) != 0 && len(input)%n == 0 }
}

// zeroPaddedKey drops what a precompile that reads a fixed prefix and zero-extends a shorter input
// never sees: bytes past the prefix and trailing zeros inside it.
func zeroPaddedKey(prefix int) func([]byte) ([]byte, bool) {
	return func(input []byte) ([]byte, bool) {
		input = input[:min(len(input), prefix)]
		end := len(input)
		for end > 0 && input[end-1] == 0 {
			end--
		}
		return input[:end], true
	}
}

const modExpHeaderLength = 96

// modExpKey drops everything past the operands the header declares.
func modExpKey(input []byte) ([]byte, bool) {
	baseLen := new(uint256.Int).SetBytes(getData(input, 0, 32))
	expLen := new(uint256.Int).SetBytes(getData(input, 32, 32))
	modLen := new(uint256.Int).SetBytes(getData(input, 64, 32))
	// A zero modulus answers from the header faster than a lookup.
	if modLen.IsZero() || !baseLen.IsUint64() || !expLen.IsUint64() || !modLen.IsUint64() {
		return nil, false
	}
	if len(input) <= modExpHeaderLength {
		return input, true
	}
	end := new(uint256.Int).AddUint64(baseLen, modExpHeaderLength)
	end.Add(end, expLen)
	end.Add(end, modLen)
	if !end.IsUint64() || end.Uint64() >= uint64(len(input)) {
		return input, true
	}
	return input[:end.Uint64()], true
}
