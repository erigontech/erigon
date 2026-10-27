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
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
)

// Port of go-ethereum's core/vm/precompile_cache.go: one byte-bounded LRU per precompile per fork,
// keyed by the normalized input, so results never cross a repricing and a cheap precompile cannot
// evict an expensive one.
const (
	maxCacheablePrecompileInput  = 8192
	maxCacheablePrecompileOutput = 1024
	maxCacheablePrecompileBytes  = 1024 * 1024
	precompileEntryOverhead      = 150
)

var precompileCacheEnabled = dbg.EnvBool("PRECOMPILE_CACHE", true)

type precompileResultLRU struct {
	mu   sync.Mutex
	lru  *simplelru.LRU[string, []byte]
	size uint64
}

func newPrecompileResultLRU() *precompileResultLRU {
	l, _ := simplelru.NewLRU[string, []byte](1<<30, nil)
	return &precompileResultLRU{lru: l}
}

func precompileEntrySize(key string, value []byte) uint64 {
	return uint64(len(key)+len(value)) + precompileEntryOverhead
}

func (c *precompileResultLRU) get(key []byte) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Get(string(key))
}

func (c *precompileResultLRU) add(key []byte, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := string(key)
	if old, ok := c.lru.Peek(k); ok {
		c.size -= precompileEntrySize(k, old)
	}
	c.lru.Add(k, value)
	c.size += precompileEntrySize(k, value)
	for c.size > maxCacheablePrecompileBytes {
		ek, ev, ok := c.lru.RemoveOldest()
		if !ok {
			break
		}
		c.size -= precompileEntrySize(ek, ev)
	}
}

type cachedPrecompile struct {
	PrecompiledContract
	normalize func(input []byte) ([]byte, bool)
	results   *precompileResultLRU
}

// cachePrecompile wraps the precompiles go-ethereum enrolls in its result cache; the rest run as is.
func cachePrecompile(p PrecompiledContract) PrecompiledContract {
	if !precompileCacheEnabled {
		return p
	}
	normalize := precompileNormalizer(p)
	if normalize == nil {
		return p
	}
	return &cachedPrecompile{PrecompiledContract: p, normalize: normalize, results: newPrecompileResultLRU()}
}

func (c *cachedPrecompile) Run(input []byte) ([]byte, error) {
	key, ok := c.normalize(input)
	if !ok || len(key) > maxCacheablePrecompileInput {
		return c.PrecompiledContract.Run(input)
	}
	if output, ok := c.results.get(key); ok {
		return bytes.Clone(output), nil
	}
	output, err := c.PrecompiledContract.Run(input)
	if err == nil && len(output) <= maxCacheablePrecompileOutput {
		c.results.add(key, bytes.Clone(output))
	}
	return output, err
}

func asIs(input []byte) ([]byte, bool) { return input, true }

func lengthIs(n int) func([]byte) ([]byte, bool) {
	return func(input []byte) ([]byte, bool) { return input, len(input) == n }
}

func lengthMultipleOf(n int) func([]byte) ([]byte, bool) {
	return func(input []byte) ([]byte, bool) { return input, len(input) != 0 && len(input)%n == 0 }
}

func zeroPadded(prefix int) func([]byte) ([]byte, bool) {
	return func(input []byte) ([]byte, bool) {
		if len(input) > prefix {
			input = input[:prefix]
		}
		end := len(input)
		for end > 0 && input[end-1] == 0 {
			end--
		}
		return input[:end], true
	}
}

func precompileNormalizer(p PrecompiledContract) func([]byte) ([]byte, bool) {
	switch p.(type) {
	case *ecrecover:
		return zeroPadded(128)
	case *sha256hash, *ripemd160hash:
		return asIs
	case *bigModExp:
		return normalizeModExpInput
	case *bn254AddIstanbul, *bn254AddByzantium:
		return zeroPadded(128)
	case *bn254ScalarMulIstanbul, *bn254ScalarMulByzantium:
		return zeroPadded(96)
	case *bn254PairingIstanbul, *bn254PairingByzantium:
		return func(input []byte) ([]byte, bool) { return input, len(input)%192 == 0 }
	case *blake2F:
		return lengthIs(blake2FInputLength)
	case *bls12381G1Add:
		return lengthIs(256)
	case *bls12381G1MultiExp:
		return lengthMultipleOf(160)
	case *bls12381G2Add:
		return lengthIs(512)
	case *bls12381G2MultiExp:
		return lengthMultipleOf(288)
	case *bls12381Pairing:
		return lengthMultipleOf(384)
	case *bls12381MapFpToG1:
		return lengthIs(64)
	case *bls12381MapFp2ToG2:
		return lengthIs(128)
	case *p256Verify:
		return lengthIs(160)
	default:
		return nil
	}
}

const modExpHeaderLength = 96

// normalizeModExpInput drops everything past the operands the header declares.
func normalizeModExpInput(input []byte) ([]byte, bool) {
	if len(input) <= modExpHeaderLength {
		return input, true
	}
	baseLen := new(uint256.Int).SetBytes(getData(input, 0, 32))
	expLen := new(uint256.Int).SetBytes(getData(input, 32, 32))
	modLen := new(uint256.Int).SetBytes(getData(input, 64, 32))
	if !baseLen.IsUint64() || !expLen.IsUint64() || !modLen.IsUint64() {
		return nil, false
	}
	if baseLen.IsZero() && modLen.IsZero() {
		return input[:modExpHeaderLength], true
	}
	end := new(uint256.Int).AddUint64(baseLen, modExpHeaderLength)
	end.Add(end, expLen)
	end.Add(end, modLen)
	if !end.IsUint64() || end.Uint64() >= uint64(len(input)) {
		return input, true
	}
	return input[:end.Uint64()], true
}
