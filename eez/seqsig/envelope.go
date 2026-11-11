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

package seqsig

import (
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// SignatureLength is the length of an r ‖ s ‖ v signature.
const SignatureLength = 65

var (
	ErrWrongRollup        = errors.New("wrong rollup")
	ErrBadSignature       = errors.New("bad signature")
	ErrUnauthorizedSigner = errors.New("unauthorized signer")
	ErrBlockDecode        = errors.New("block decode failed")
	ErrBlockMismatch      = errors.New("block does not match the head")
	ErrBodyMismatch       = errors.New("block body does not match its header")
)

// Envelope is a signed head as it travels between nodes.
type Envelope struct {
	Head Head
	// Signature is r ‖ s ‖ v over Head.SigningHash.
	Signature []byte
	// Block is the consensus RLP of the head block, empty for none: a head
	// that rewinds to a block peers already have carries none.
	Block []byte
}

// VerifiedHead is a head signed by the authorized signer, with its block if
// the envelope carried one.
type VerifiedHead struct {
	Head   Head
	Signer common.Address
	Block  *types.Block
}

// Sign returns the r ‖ s ‖ v signature of h in domain d, v 27 or 28.
func Sign(h Head, d Domain, key *ecdsa.PrivateKey) ([]byte, error) {
	digest := h.SigningHash(d)
	sig, err := crypto.Sign(digest[:], key)
	if err != nil {
		return nil, err
	}
	sig[64] += 27
	return sig, nil
}

// Recover returns the address that signed h in domain d.
//
// The parity byte may be 0 or 1, 27 or 28, or an EIP-155 value, and s may be
// high: every encoding of one signature recovers the same signer.
func Recover(h Head, d Domain, sig []byte) (common.Address, error) {
	if len(sig) != SignatureLength {
		return common.Address{}, fmt.Errorf("%w: %d bytes, want %d", ErrBadSignature, len(sig), SignatureLength)
	}
	v := sig[64]
	var parity byte
	switch {
	case v <= 1:
		parity = v
	case v == 27 || v == 28:
		parity = v - 27
	case v >= 35:
		parity = (v - 35) & 1
	default:
		return common.Address{}, fmt.Errorf("%w: parity byte %d", ErrBadSignature, v)
	}
	var buf [SignatureLength]byte
	copy(buf[:], sig)
	buf[64] = parity
	digest := h.SigningHash(d)
	pub, err := crypto.SigToPub(digest[:], buf[:])
	if err != nil {
		return common.Address{}, fmt.Errorf("%w: %w", ErrBadSignature, err)
	}
	return crypto.PubkeyToAddress(*pub), nil
}

// Verify checks that e names d's rollup, is signed by authorized, and, when
// it carries a block, that the block is the head's: its hash, parent and
// number, with a body that matches its header.
func (e Envelope) Verify(d Domain, authorized common.Address) (*VerifiedHead, error) {
	if e.Head.RollupID != d.RollupID {
		return nil, fmt.Errorf("%w: %d, want %d", ErrWrongRollup, e.Head.RollupID, d.RollupID)
	}
	signer, err := Recover(e.Head, d, e.Signature)
	if err != nil {
		return nil, err
	}
	if signer != authorized {
		return nil, fmt.Errorf("%w %s", ErrUnauthorizedSigner, signer)
	}
	v := &VerifiedHead{Head: e.Head, Signer: signer}
	if len(e.Block) == 0 {
		return v, nil
	}
	b, err := DecodeBlock(e.Block)
	if err != nil {
		return nil, err
	}
	switch {
	case b.Hash() != e.Head.BlockHash:
		return nil, fmt.Errorf("%w: hash %x, head %x", ErrBlockMismatch, b.Hash(), e.Head.BlockHash)
	case b.ParentHash() != e.Head.ParentHash:
		return nil, fmt.Errorf("%w: parent %x, head %x", ErrBlockMismatch, b.ParentHash(), e.Head.ParentHash)
	case b.NumberU64() != e.Head.Number:
		return nil, fmt.Errorf("%w: number %d, head %d", ErrBlockMismatch, b.NumberU64(), e.Head.Number)
	}
	v.Block = b
	return v, nil
}

// DecodeBlock decodes a consensus-RLP block whose body matches its header, so
// that its hash binds the whole block.
func DecodeBlock(enc []byte) (*types.Block, error) {
	var b types.Block
	if err := rlp.DecodeBytes(enc, &b); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBlockDecode, err)
	}
	if err := b.Body().MatchesHeader(b.HeaderNoCopy()); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBodyMismatch, err)
	}
	return &b, nil
}
