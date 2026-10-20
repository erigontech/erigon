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
	"crypto/sha256"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/cache"
)

var precompileResults = cache.NewHashByteLRU[[]byte](8*datasize.MB, func(output []byte) int64 { return int64(len(output)) + 24 })

type cachedPrecompile struct {
	PrecompiledContract
	domain [21]byte
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
		return c
	default:
		return p
	}
}

func (c *cachedPrecompile) Run(input []byte) ([]byte, error) {
	if len(input) > 64*1024 {
		return c.PrecompiledContract.Run(input)
	}
	var keyData [53]byte
	copy(keyData[:21], c.domain[:])
	inputHash := sha256.Sum256(input)
	copy(keyData[21:], inputHash[:])
	key := common.Hash(sha256.Sum256(keyData[:]))
	if output, ok := precompileResults.Get(key); ok {
		return bytes.Clone(output), nil
	}
	output, err := c.PrecompiledContract.Run(input)
	if err == nil && len(output) <= 1024 {
		precompileResults.Add(key, bytes.Clone(output))
		return bytes.Clone(output), nil
	}
	return output, err
}
