// Copyright 2020 The go-ethereum Authors
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

package ethtest

import (
	"crypto/rand"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

// largeNumber returns a very large big.Int.
func largeNumber(megabytes int) *uint256.Int { //nolint:unparam
	buf := make([]byte, megabytes*1024*1024)
	//nolint:errcheck
	rand.Read(buf)
	bigint := new(uint256.Int)
	bigint.SetBytes(buf)
	return bigint
}

// largeBuffer returns a very large buffer.
func largeBuffer(megabytes int) []byte { //nolint:unparam
	buf := make([]byte, megabytes*1024*1024)
	//nolint:errcheck
	rand.Read(buf)
	return buf
}

// largeString returns a very large string.
func largeString(megabytes int) string { //nolint:unparam
	buf := make([]byte, megabytes*1024*1024)
	//nolint:errcheck
	rand.Read(buf)
	return hexutil.Encode(buf)
}

func largeBlock() *types.Block {
	return types.NewBlockWithHeader(largeHeader())
}

// Returns a random hash
func randHash() common.Hash {
	var h common.Hash
	//nolint:errcheck
	rand.Read(h[:])
	return h
}

func largeHeader() *types.Header {
	return &types.Header{
		MixDigest:   randHash(),
		ReceiptHash: randHash(),
		TxHash:      randHash(),
		Nonce:       types.BlockNonce{},
		Extra:       []byte{},
		Bloom:       types.Bloom{},
		GasUsed:     0,
		Coinbase:    common.Address{},
		GasLimit:    0,
		UncleHash:   randHash(),
		Time:        1337,
		ParentHash:  randHash(),
		Root:        randHash(),
		Number:      largeNumber(2).ToBig(),
		Difficulty:  largeNumber(2).ToBig(),
	}
}
