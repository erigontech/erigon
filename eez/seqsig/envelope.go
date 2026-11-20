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
	"bytes"
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/protocol"
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
	// Block is the consensus RLP of the head block, which every envelope
	// carries.
	Block []byte
}

// VerifiedHead is a head signed by the authorized signer, with its block.
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
// Only v 27 or 28 with low s is taken. A relay may re-encode a signature,
// and nodes relay the first copy of a head only: a copy one node takes and
// another refuses would hold the head back from the stricter.
func Recover(h Head, d Domain, sig []byte) (common.Address, error) {
	if len(sig) != SignatureLength {
		return common.Address{}, fmt.Errorf("%w: %d bytes, want %d", ErrBadSignature, len(sig), SignatureLength)
	}
	// A v other than 27 or 28 leaves a parity other than 0 or 1, which fails.
	parity := sig[64] - 27
	if !crypto.TransactionSignatureIsValid(parity, new(uint256.Int).SetBytes(sig[:32]), new(uint256.Int).SetBytes(sig[32:64]), false) {
		return common.Address{}, fmt.Errorf("%w: v %d, or r or s out of range", ErrBadSignature, sig[64])
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

// Verify checks that e names d's rollup, is signed by authorized, and
// carries the head's block: its hash, parent and number, with a body that
// matches its header.
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
		return nil, fmt.Errorf("%w: no block", ErrBlockDecode)
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
//
// Only the canonical encoding is taken: the transactions root commits to the
// decoded transactions, not to their bytes, so another encoding has the same
// hash and could hold the head back from a node that refuses it.
func DecodeBlock(enc []byte) (*types.Block, error) {
	var b types.Block
	if err := rlp.DecodeBytes(enc, &b); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBlockDecode, err)
	}
	if canonical, err := rlp.EncodeToBytes(&b); err != nil || !bytes.Equal(canonical, enc) {
		return nil, fmt.Errorf("%w: not the canonical encoding", ErrBlockDecode)
	}
	// The decoder also takes a number or base fee above 64 bits, and an AuRa
	// step and seal in place of the mix hash and nonce.
	if h := b.HeaderNoCopy(); !h.Number.IsUint64() || h.BaseFee != nil && !h.BaseFee.IsUint64() || len(h.AuRaSeal) > 0 {
		return nil, fmt.Errorf("%w: header outside the Ethereum shape", ErrBlockDecode)
	}
	for i, tx := range b.Transactions() {
		if !blockTx(tx) {
			return nil, fmt.Errorf("%w: transaction %d of type %#x", ErrBlockDecode, i, tx.Type())
		}
	}
	if err := b.Body().MatchesHeader(b.HeaderNoCopy()); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBodyMismatch, err)
	}
	return &b, nil
}

// blockTx reports whether tx is of a type a head's block may carry. The
// registry also decodes the types of other rule sets.
func blockTx(tx types.Transaction) bool {
	switch tx.Type() {
	case types.LegacyTxType, types.AccessListTxType, types.DynamicFeeTxType, types.BlobTxType, types.SetCodeTxType:
		return true
	case protocol.SystemTxType:
		to := tx.GetTo()
		return to != nil && *to == protocol.EEZL2Address
	}
	return false
}
