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

// Package announce signs and verifies the unsafe-block announcements of
// Rollup0 App. D.6, and keeps those of a sequencer's blocks until L1 finality
// passes them.
package announce

import (
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
)

// Version is the announcement envelope version.
const Version = 0

// ErrInvalid reports an announcement that does not authenticate the block.
var ErrInvalid = errors.New("invalid unsafe-block announcement")

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

// Verify returns the producer that signed env, the announcement of block hash
// on chain chainID.
func Verify(env []byte, chainID *uint256.Int, hash common.Hash) (common.Address, error) {
	var e envelope
	if err := rlp.DecodeBytes(env, &e); err != nil {
		return common.Address{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	switch {
	case e.Version != Version:
		return common.Address{}, fmt.Errorf("%w: version %d", ErrInvalid, e.Version)
	case !e.ChainID.Eq(chainID):
		return common.Address{}, fmt.Errorf("%w: chain %s, not %s", ErrInvalid, &e.ChainID, chainID)
	case e.BlockHash != hash:
		return common.Address{}, fmt.Errorf("%w: block %x, not %x", ErrInvalid, e.BlockHash, hash)
	}
	sig := e.Signature
	r, s := new(uint256.Int).SetBytes(sig[:32]), new(uint256.Int).SetBytes(sig[32:64])
	// v-27 is 0 or 1 only for a v of 27 or 28.
	if !crypto.TransactionSignatureIsValid(sig[64]-27, r, s, false) {
		return common.Address{}, fmt.Errorf("%w: signature values out of range or s not low", ErrInvalid)
	}
	sig[64] -= 27
	m := Message(chainID, hash)
	pub, err := crypto.SigToPub(m[:], sig[:])
	if err != nil {
		return common.Address{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return crypto.PubkeyToAddress(*pub), nil
}
