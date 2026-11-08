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

// Package announce signs the unsafe-block announcements of Rollup0 App. D.6
// and keeps those of a sequencer's blocks until L1 finality covers them.
package announce

import (
	"crypto/ecdsa"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
)

// Version is the announcement envelope version.
const Version = 0

// Tag is the unsafe-block domain tag, keccak256("EEZ_ROLLUP0_UNSAFE_BLOCK_V1").
var Tag = crypto.Keccak256Hash([]byte("EEZ_ROLLUP0_UNSAFE_BLOCK_V1"))

// envelope is an announcement, rlp([version, chainId, blockHash, signature]).
type envelope struct {
	Version   uint64
	ChainID   uint256.Int
	BlockHash common.Hash
	// Signature is r || s || v, v 27 or 28.
	Signature [65]byte
}

// Message returns the hash the producer of block hash on chain chainID signs:
// keccak256(abi.encode(Tag, Version, chainID, hash)).
func Message(chainID *uint256.Int, hash common.Hash) common.Hash {
	var buf [4 * 32]byte
	copy(buf[:32], Tag[:])
	uint256.NewInt(Version).PutUint256(buf[32:64])
	chainID.PutUint256(buf[64:96])
	copy(buf[96:], hash[:])
	return crypto.Keccak256Hash(buf[:])
}

// Sign returns the encoded announcement of block hash on chain chainID,
// signed with key.
func Sign(key *ecdsa.PrivateKey, chainID *uint256.Int, hash common.Hash) ([]byte, error) {
	m := Message(chainID, hash)
	sig, err := crypto.Sign(m[:], key)
	if err != nil {
		return nil, err
	}
	e := envelope{Version: Version, ChainID: *chainID, BlockHash: hash}
	copy(e.Signature[:], sig)
	e.Signature[64] += 27
	return rlp.EncodeToBytes(&e)
}
