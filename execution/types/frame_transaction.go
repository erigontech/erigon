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

package types

// NOTE: EIP-8141 (Frame Transactions) is still in draft status. The encoding
// format, opcode numbers, and gas rules are not yet finalized. This file
// provides scaffolding — the struct and basic interface compliance — so that
// Phase 5 work can proceed. Methods that depend on the final spec (Hash,
// SigningHash, EncodeRLP, DecodeRLP) are stubs that will be completed when
// the spec is finalized.

import (
	"errors"
	"io"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// FrameTxType is the EIP-8141 transaction type byte.
// Value 6 follows AccountAbstractionTxType (5) in the EIP-2718 type space.
const FrameTxType = AccountAbstractionTxType + 1 // = 6

// FrameKind identifies the role of a frame within a FrameTransaction.
type FrameKind uint8

const (
	FrameKindVerify  FrameKind = 0 // Custom signature validation (uses APPROVE opcode)
	FrameKindSender  FrameKind = 1 // Determines the effective sender address
	FrameKindDefault FrameKind = 2 // Normal EVM execution from the determined sender
)

// TxFrame is a single execution frame within a FrameTransaction.
// EIP-8141 draft — field layout subject to change before spec finalization.
type TxFrame struct {
	Kind     FrameKind      // VERIFY / SENDER / DEFAULT
	To       common.Address // Call target
	Data     []byte         // Call data
	GasLimit uint64         // Gas budget for this frame
	Value    *uint256.Int   // Value transferred (nil = 0)
}

// FrameTransaction is an EIP-8141 transaction containing an ordered sequence
// of execution frames. Each frame runs in a different context: VERIFY frames
// validate the signature/authorization, SENDER frames determine the effective
// sender, and DEFAULT frames execute the actual user operation.
//
// EIP-8141 draft — struct layout and encoding subject to change.
type FrameTransaction struct {
	TransactionMisc
	ChainID        *uint256.Int
	Nonce          uint64
	Tip            *uint256.Int
	FeeCap         *uint256.Int
	GasLimit       uint64
	Frames         []TxFrame
	AccessList     AccessList
	Authorizations []Authorization // EIP-7702 delegation support
}

// --- Transaction interface ---

func (tx *FrameTransaction) Type() byte { return FrameTxType }

func (tx *FrameTransaction) GetChainID() *uint256.Int { return tx.ChainID }

func (tx *FrameTransaction) GetNonce() uint64 { return tx.Nonce }

func (tx *FrameTransaction) GetTipCap() *uint256.Int { return tx.Tip }

func (tx *FrameTransaction) GetFeeCap() *uint256.Int { return tx.FeeCap }

func (tx *FrameTransaction) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetTipCap()
	}
	feeCap := tx.GetFeeCap()
	if feeCap.Lt(baseFee) {
		return uint256.NewInt(0)
	}
	effective := new(uint256.Int).Sub(feeCap, baseFee)
	if tx.GetTipCap().Lt(effective) {
		return tx.GetTipCap()
	}
	return effective
}

func (tx *FrameTransaction) GetBlobHashes() []common.Hash       { return []common.Hash{} }
func (tx *FrameTransaction) GetGasLimit() uint64                { return tx.GasLimit }
func (tx *FrameTransaction) GetBlobGas() uint64                 { return 0 }
func (tx *FrameTransaction) GetValue() *uint256.Int             { return uint256.NewInt(0) }
func (tx *FrameTransaction) GetTo() *common.Address             { return nil }
func (tx *FrameTransaction) GetData() []byte                    { return []byte{} }
func (tx *FrameTransaction) GetAccessList() AccessList          { return tx.AccessList }
func (tx *FrameTransaction) GetAuthorizations() []Authorization { return tx.Authorizations }
func (tx *FrameTransaction) IsContractDeploy() bool             { return false }
func (tx *FrameTransaction) Protected() bool                    { return true }
func (tx *FrameTransaction) Unwrap() Transaction                { return tx }

func (tx *FrameTransaction) AsMessage(s Signer, baseFee *uint256.Int, rules *chain.Rules) (*Message, error) {
	return &Message{
		to:         accounts.NilAddress,
		gasPrice:   *tx.FeeCap,
		blobHashes: []common.Hash{},
	}, nil
}

func (tx *FrameTransaction) WithSignature(_ Signer, _ []byte) (Transaction, error) {
	return tx, nil
}

func (tx *FrameTransaction) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return new(uint256.Int), new(uint256.Int), new(uint256.Int)
}

func (tx *FrameTransaction) Sender(_ Signer) (accounts.Address, error) {
	return accounts.NilAddress, nil
}

func (tx *FrameTransaction) cachedSender() (accounts.Address, bool) {
	return tx.from, !tx.from.IsNil()
}

func (tx *FrameTransaction) GetSender() (accounts.Address, bool) {
	return tx.from, !tx.from.IsNil()
}

func (tx *FrameTransaction) SetSender(addr accounts.Address) { tx.from = addr }

// Hash returns the EIP-8141 transaction hash.
// NOTE: encoding is not yet finalized — this is a placeholder that uses the
// same prefixedRlpHash helper as other types. The fields hashed will change
// when the spec is finalized.
func (tx *FrameTransaction) Hash() common.Hash {
	if h := tx.hash.Load(); h != nil {
		return *h
	}
	hash := prefixedRlpHash(FrameTxType, []any{
		tx.ChainID,
		tx.Nonce,
		tx.Tip, tx.FeeCap,
		tx.GasLimit,
		tx.Frames,
		tx.AccessList,
		tx.Authorizations,
	})
	tx.hash.Store(&hash)
	return hash
}

// SigningHash returns the hash over which the transaction is signed.
// NOTE: placeholder — will be updated when EIP-8141 encoding is finalized.
func (tx *FrameTransaction) SigningHash(chainID *big.Int) common.Hash {
	return prefixedRlpHash(FrameTxType, []any{
		chainID,
		tx.Nonce,
		tx.Tip, tx.FeeCap,
		tx.GasLimit,
		tx.Frames,
		tx.AccessList,
	})
}

// EncodingSize returns the RLP encoding size.
// NOTE: placeholder returning 0 — implement when spec is finalized.
func (tx *FrameTransaction) EncodingSize() int { return 0 }

// EncodeRLP encodes the transaction to RLP.
// NOTE: placeholder — EIP-8141 encoding format not yet finalized.
func (tx *FrameTransaction) EncodeRLP(_ io.Writer) error {
	return errors.New("FrameTransaction.EncodeRLP: EIP-8141 encoding not yet implemented")
}

// DecodeRLP decodes the transaction from RLP.
// NOTE: placeholder — EIP-8141 encoding format not yet finalized.
func (tx *FrameTransaction) DecodeRLP(_ *rlp.Stream) error {
	return errors.New("FrameTransaction.DecodeRLP: EIP-8141 encoding not yet implemented")
}

// MarshalBinary writes the EIP-2718 typed encoding.
// NOTE: placeholder — implement when spec is finalized.
func (tx *FrameTransaction) MarshalBinary(_ io.Writer) error {
	return errors.New("FrameTransaction.MarshalBinary: EIP-8141 encoding not yet implemented")
}
