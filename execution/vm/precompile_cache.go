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

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/cache"
)

var (
	precompileSeed, precompileCheckSeed = maphash.MakeSeed(), maphash.MakeSeed()
	precompileResults                   = cache.NewByteLRU(32*datasize.MB, func(_ uint64, r precompileResult) int64 {
		return int64(len(r.output) + 64)
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
	domain     [21]byte
	domainHash uint64
}

func cachePrecompile(p PrecompiledContract, tier forkTier, address common.Address) PrecompiledContract {
	switch p.(type) {
	case *bigModExp, *bn254ScalarMulByzantium, *bn254ScalarMulIstanbul,
		*bn254PairingByzantium, *bn254PairingIstanbul, *blake2F, *pointEvaluation,
		*bls12381G1MultiExp, *bls12381G2MultiExp, *bls12381Pairing,
		*bls12381MapFpToG1, *bls12381MapFp2ToG2, *p256Verify:
		c := &cachedPrecompile{PrecompiledContract: p}
		c.domain[0] = byte(tier)
		copy(c.domain[1:], address[:])
		c.domainHash = maphash.Bytes(precompileSeed, c.domain[:])
		return c
	default:
		return p
	}
}

func (c *cachedPrecompile) Run(input []byte) ([]byte, error) {
	if c.RequiredGas(input) < uint64(len(input)) {
		return c.PrecompiledContract.Run(input)
	}
	if len(input) > 64*1024 {
		output, err := c.PrecompiledContract.Run(input)
		return bytes.Clone(output), err
	}
	key, check := maphash.Bytes(precompileSeed, input)^c.domainHash, maphash.Bytes(precompileCheckSeed, input)
	if r, ok := precompileResults.Get(key); ok && r.domain == c.domain && r.check == check {
		return bytes.Clone(r.output), nil
	}
	output, err := c.PrecompiledContract.Run(input)
	if err == nil && len(output) <= 1024 {
		precompileResults.Add(key, precompileResult{domain: c.domain, check: check, output: bytes.Clone(output)})
		return bytes.Clone(output), nil
	}
	return output, err
}
