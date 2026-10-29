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

import (
	"bytes"
	"crypto/elliptic"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"slices"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/crypto/secp256r1"
	libmath "github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const (
	FrameDefault = iota
	FrameVerify
	FrameSender
)

const (
	FrameApprovePayment   = 1
	FrameApproveExecution = 2
	FrameApproveBoth      = 3
	FrameAtomicBatch      = 4
	FrameMaxCount         = 64
	FrameMaxNonceKeys     = 16
)

const (
	FrameSignatureArbitrary = iota
	FrameSignatureSecp256k1
	FrameSignatureP256
)

var FrameExpiryVerifier = common.HexToAddress("0x81413f0cF12e9b6a49B1D0439E081c577D57FfFf")

var FrameNonceManager = common.HexToAddress("0x8250968C12e01A19d6F667b9B2F3b3A4d0e51cB7")

var FrameRecentRootVerifier = common.HexToAddress("0x8272D9679689Ea2f307140CdF9002D27dC00Ffff")

// Frame is a call with its own caller mode, approval scope and gas budgets.
// A nil target denotes the transaction sender; creation happens inside a frame call.
type Frame struct {
	Mode   uint8
	Flags  uint8
	Target *common.Address `rlp:"nil"`
	Limits mdgas.MdGas
	Value  uint256.Int
	Data   []byte
}

// FrameSignature supplies a verified signature claim, or opaque data for contract
// validation. Verifying a signature alone does not authorize execution or payment.
type FrameSignature struct {
	Scheme    uint8
	Signer    []byte
	Message   []byte
	Signature []byte
}

type FrameFees struct {
	TipCap     uint256.Int
	FeeCap     uint256.Int
	BlobFeeCap uint256.Int
}

// FrameTransaction carries an explicit sender and a sequence of calls that
// authorize execution, select a payer and perform the requested work.
// Its format combines EIP-8141 frames with EIP-8250 nonce keys:
// https://eips.ethereum.org/EIPS/eip-8141 and https://eips.ethereum.org/EIPS/eip-8250.
type FrameTransaction struct {
	TransactionMisc     `rlp:"-"`
	ChainID             uint256.Int
	NonceKeys           []uint256.Int
	Nonce               uint64
	SenderAddress       common.Address
	Frames              []Frame
	Signatures          []FrameSignature
	Fees                FrameFees
	BlobVersionedHashes []common.Hash
	verifiedSignatures  atomic.Pointer[common.Hash] `rlp:"-"`
}

type frameTransactionRLP FrameTransaction

func (frame *Frame) ResolvedTarget(sender common.Address) common.Address {
	if frame.Target == nil {
		return sender
	}
	return *frame.Target
}

func (frame *Frame) IsExpiryVerifier() bool {
	return frame.Mode == FrameVerify && frame.Target != nil && *frame.Target == FrameExpiryVerifier
}

func (frame *Frame) IsRecentRootVerifier() bool {
	return frame.Mode == FrameVerify && frame.Target != nil && *frame.Target == FrameRecentRootVerifier
}

func (sig *FrameSignature) VerificationGas() uint64 {
	switch sig.Scheme {
	case FrameSignatureArbitrary:
		return params.FrameSignatureArbitraryGas
	case FrameSignatureSecp256k1:
		return params.FrameSignatureSecp256k1Gas
	case FrameSignatureP256:
		return params.FrameSignatureP256Gas
	default:
		return 0
	}
}

func (sig *FrameSignature) ResolvedSigner(sender common.Address) common.Address {
	if len(sig.Signer) == 0 {
		return sender
	}
	return common.BytesToAddress(sig.Signer)
}

func (sig *FrameSignature) validate(sender common.Address, hash common.Hash) error {
	if len(sig.Signer) != 0 && len(sig.Signer) != 20 {
		return errors.New("invalid signer length")
	}
	if len(sig.Message) != 0 {
		if len(sig.Message) != 32 {
			return errors.New("invalid message length")
		}
		if common.BytesToHash(sig.Message) == (common.Hash{}) {
			return errors.New("zero message digest")
		}
		hash = common.BytesToHash(sig.Message)
	}
	switch sig.Scheme {
	case FrameSignatureArbitrary:
		if len(sig.Signer) != 0 {
			return errors.New("arbitrary signature has signer")
		}
	case FrameSignatureSecp256k1:
		if len(sig.Signature) != 65 {
			return errors.New("invalid signature length")
		}
		var r, s uint256.Int
		r.SetBytes(sig.Signature[1:33])
		s.SetBytes(sig.Signature[33:])
		recovered, err := recoverSignerFromHash(hash, sig.Signature[0], r, s)
		if err != nil {
			return errors.New("signature verification failed")
		}
		if recovered != sig.ResolvedSigner(sender) {
			return errors.New("signer mismatch")
		}
	case FrameSignatureP256:
		if len(sig.Signature) != 128 {
			return errors.New("invalid signature length")
		}
		r := new(big.Int).SetBytes(sig.Signature[:32])
		s := new(big.Int).SetBytes(sig.Signature[32:64])
		n := elliptic.P256().Params().N
		if r.Sign() == 0 || r.Cmp(n) >= 0 || s.Sign() == 0 || s.Cmp(new(big.Int).Rsh(new(big.Int).Set(n), 1)) > 0 {
			return errors.New("signature verification failed")
		}
		if common.BytesToAddress(crypto.Keccak256(sig.Signature[64:])[12:]) != sig.ResolvedSigner(sender) {
			return errors.New("signer mismatch")
		}
		x := new(big.Int).SetBytes(sig.Signature[64:96])
		y := new(big.Int).SetBytes(sig.Signature[96:])
		if !secp256r1.Verify(hash[:], r, s, x, y) {
			return errors.New("signature verification failed")
		}
	default:
		return errors.New("unknown scheme")
	}
	return nil
}

// Validate checks structure, gas limits and cryptographic signatures without state.
// Authorization, nonce consumption and payment are checked during frame execution.
func (tx *FrameTransaction) Validate() error {
	if err := tx.validateNonceKeys(); err != nil {
		return err
	}
	if len(tx.Frames) == 0 || len(tx.Frames) > FrameMaxCount {
		return errors.New("invalid frame count")
	}
	if len(tx.BlobVersionedHashes) == 0 && !tx.Fees.BlobFeeCap.IsZero() {
		return errors.New("blob fee without blobs")
	}
	if len(tx.BlobVersionedHashes) > params.MaxBlobsPerTxn {
		return errors.New("too many frame transaction blobs")
	}
	for _, hash := range tx.BlobVersionedHashes {
		if hash[0] != 1 {
			return errors.New("invalid blob versioned hash")
		}
	}
	var expirySeen bool
	for i := range tx.Frames {
		frame := &tx.Frames[i]
		if frame.Mode > FrameSender || frame.Flags >= 8 {
			return fmt.Errorf("invalid frame %d mode or flags", i)
		}
		if frame.Mode != FrameSender && !frame.Value.IsZero() {
			return fmt.Errorf("frame %d value requires sender mode", i)
		}
		if frame.Flags&FrameApproveExecution != 0 && frame.ResolvedTarget(tx.SenderAddress) != tx.SenderAddress {
			return fmt.Errorf("frame %d execution approval requires sender target", i)
		}
		atomic := frame.Flags&FrameAtomicBatch != 0
		if atomic && (frame.Mode == FrameVerify || i+1 == len(tx.Frames) || tx.Frames[i+1].Mode == FrameVerify) {
			return fmt.Errorf("invalid atomic batch at frame %d", i)
		}
		if (atomic || i > 0 && tx.Frames[i-1].Flags&FrameAtomicBatch != 0) && frame.Flags&FrameApproveBoth != 0 {
			return fmt.Errorf("approval in atomic batch at frame %d", i)
		}
		if frame.IsExpiryVerifier() {
			if expirySeen || frame.Flags != 0 || frame.Limits.State != 0 || len(frame.Data) != 8 {
				return errors.New("invalid expiry verifier frame")
			}
			expirySeen = true
		}
	}
	_, _, limits, err := tx.GasLimits()
	if err != nil {
		return err
	}
	if limits.Execution > params.MaxTxnGasLimit {
		return errors.New("frame transaction exceeds execution gas cap")
	}
	if len(tx.Signatures) == 0 {
		return nil
	}
	// Use fresh contents, including signature bytes: Hash can be cached before
	// a mutation, and SigningHash omits signatures over the transaction itself.
	validationHash := prefixedRlpHash(FrameTxType, (*frameTransactionRLP)(tx))
	if verified := tx.verifiedSignatures.Load(); verified != nil && *verified == validationHash {
		return nil
	}
	hash := tx.SigningHash(nil)
	for i := range tx.Signatures {
		if err := tx.Signatures[i].validate(tx.SenderAddress, hash); err != nil {
			return fmt.Errorf("invalid frame signature %d: %w", i, err)
		}
	}
	tx.verifiedSignatures.Store(&validationHash)
	return nil
}

func (tx *FrameTransaction) Type() byte { return FrameTxType }

func (tx *FrameTransaction) GetChainID() *uint256.Int { return &tx.ChainID }

func (tx *FrameTransaction) GetNonce() uint64 { return tx.Nonce }

func (tx *FrameTransaction) GetTipCap() *uint256.Int { return &tx.Fees.TipCap }

func (tx *FrameTransaction) GetFeeCap() *uint256.Int { return &tx.Fees.FeeCap }

func (tx *FrameTransaction) GetEffectiveGasTip(baseFee *uint256.Int) uint256.Int {
	return CalcEffectiveGasTip(baseFee, tx.GetTipCap, tx.GetFeeCap)
}

func (tx *FrameTransaction) GetBlobHashes() []common.Hash {
	if len(tx.BlobVersionedHashes) == 0 {
		return nil
	}
	return tx.BlobVersionedHashes
}

func (tx *FrameTransaction) GetBlobGas() uint64 {
	return uint64(len(tx.BlobVersionedHashes)) * params.GasPerBlob
}

func (tx *FrameTransaction) GetGasLimit() uint64 {
	_, _, limits, err := tx.GasLimits()
	if err != nil {
		// The interface cannot return an error. Saturate so invalid limits
		// cannot wrap to a small value and pass gas-cap checks.
		return math.MaxUint64
	}
	return limits.Total()
}

// GasLimits includes transaction overhead in the execution limit and sums state
// limits separately. The calldata floor raises only the execution limit; neither
// dimension can borrow capacity from the other.
func (tx *FrameTransaction) GasLimits() (intrinsic, floor uint64, limits mdgas.MdGas, err error) {
	args := mdgas.FrameIntrinsicGasCalcArgs{FramesLen: uint64(len(tx.Frames))}
	if !tx.UsesLegacyNonce() {
		args.KeyedNoncesLen = uint64(len(tx.NonceKeys))
	}
	addData := func(data []byte) {
		args.DataLen += uint64(len(data))
		args.DataNonZeroLen += uint64(mdgas.CountNonZeroBytes(data))
	}
	nonceData, err := rlp.EncodeToBytes(tx.NonceKeys)
	if err != nil {
		return 0, 0, limits, err
	}
	// EIP-8250 prices the full nonce encodings as calldata, including RLP prefixes.
	addData(rlp.AppendUint64(nonceData, tx.Nonce))
	var total uint64
	for i := range tx.Frames {
		frame := &tx.Frames[i]
		var overflow bool
		if total, overflow = libmath.SafeAdd(total, frame.Limits.Execution); overflow {
			return 0, 0, limits, errors.New("frame gas limit overflow")
		}
		if total, overflow = libmath.SafeAdd(total, frame.Limits.State); overflow {
			return 0, 0, limits, errors.New("frame gas limit overflow")
		}
		limits.Execution += frame.Limits.Execution
		limits.State += frame.Limits.State
		if !frame.Value.IsZero() && frame.ResolvedTarget(tx.SenderAddress) != tx.SenderAddress {
			args.ValueTransfersLen++
		}
		addData(frame.Data)
	}
	for i := range tx.Signatures {
		sig := &tx.Signatures[i]
		args.SignatureGas += sig.VerificationGas()
		addData(sig.Signer)
		addData(sig.Message)
		addData(sig.Signature)
	}
	intrinsicGas := mdgas.CalcFrameIntrinsicGas(args)
	intrinsic, floor = intrinsicGas.ExecutionGas, intrinsicGas.FloorGasCost
	var overflow bool
	if limits.Execution, overflow = libmath.SafeAdd(limits.Execution, intrinsic); overflow {
		return 0, 0, limits, errors.New("frame gas limit overflow")
	}
	limits.Execution = max(limits.Execution, floor)
	if _, overflow = libmath.SafeAdd(limits.Execution, limits.State); overflow {
		return 0, 0, limits, errors.New("frame gas limit overflow")
	}
	return intrinsic, floor, limits, nil
}

func (tx *FrameTransaction) GetValue() *uint256.Int { return new(uint256.Int) }

// GetTo has no single destination to return. Use IsContractDeploy to distinguish
// contract creation from transactions whose destinations are in their frames.
func (tx *FrameTransaction) GetTo() *common.Address { return nil }

func (tx *FrameTransaction) GetData() []byte { return nil }

func (tx *FrameTransaction) GetAccessList() AccessList { return nil }

func (tx *FrameTransaction) GetAuthorizations() []Authorization { return nil }

func (tx *FrameTransaction) Protected() bool { return true }

func (tx *FrameTransaction) IsContractDeploy() bool { return false }

func (tx *FrameTransaction) Unwrap() Transaction { return tx }

func (tx *FrameTransaction) Sender(signer Signer) (accounts.Address, error) {
	return signer.Sender(tx)
}

func (tx *FrameTransaction) cachedSender() (accounts.Address, bool) {
	return accounts.InternAddress(tx.SenderAddress), true
}

func (tx *FrameTransaction) GetSender() (accounts.Address, bool) { return tx.cachedSender() }

func (tx *FrameTransaction) SetSender(accounts.Address) {}

func (tx *FrameTransaction) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return new(uint256.Int), new(uint256.Int), new(uint256.Int)
}

func (tx *FrameTransaction) WithSignature(Signer, []byte) (Transaction, error) {
	return nil, ErrInvalidTxType
}

func (tx *FrameTransaction) AsMessage(_ Signer, baseFee *uint256.Int, rules *chain.Rules) (*Message, error) {
	if !rules.IsBogota {
		return nil, ErrTxTypeNotSupported
	}
	if rules.ChainID != nil && !tx.ChainID.Eq(rules.ChainID) {
		return nil, ErrInvalidChainId
	}
	if err := tx.Validate(); err != nil {
		return nil, err
	}
	msg := &Message{
		from: accounts.InternAddress(tx.SenderAddress), nonce: tx.Nonce,
		gasLimit: tx.GetGasLimit(), gasPrice: tx.Fees.FeeCap, feeCap: tx.Fees.FeeCap, tipCap: tx.Fees.TipCap,
		maxFeePerBlobGas: tx.Fees.BlobFeeCap, blobHashes: tx.BlobVersionedHashes,
		checkNonce: true, checkTransaction: true, checkGas: true,
		frameTransaction: tx,
	}
	if baseFee != nil {
		tip := tx.GetEffectiveGasTip(baseFee)
		msg.gasPrice.Add(baseFee, &tip)
		if msg.gasPrice.Gt(&tx.Fees.FeeCap) {
			msg.gasPrice.Set(&tx.Fees.FeeCap)
		}
	}
	return msg, nil
}

func (tx *FrameTransaction) DecodeRLP(s *rlp.Stream) error {
	if err := s.Decode((*frameTransactionRLP)(tx)); err != nil {
		return err
	}
	return tx.validateNonceKeys()
}

func (tx *FrameTransaction) validateNonceKeys() error {
	if len(tx.NonceKeys) == 0 || len(tx.NonceKeys) > FrameMaxNonceKeys {
		return errors.New("invalid nonce key count")
	}
	if len(tx.NonceKeys) > 1 && tx.NonceKeys[0].IsZero() {
		return errors.New("zero nonce key alongside other keys")
	}
	for i := 1; i < len(tx.NonceKeys); i++ {
		if !tx.NonceKeys[i-1].Lt(&tx.NonceKeys[i]) {
			return errors.New("nonce keys not strictly increasing")
		}
	}
	return nil
}

// UsesLegacyNonce identifies the [0] key set, which consumes the sender's account
// nonce. Other key sets share Nonce as their sequence in FrameNonceManager storage.
func (tx *FrameTransaction) UsesLegacyNonce() bool {
	return len(tx.NonceKeys) == 1 && tx.NonceKeys[0].IsZero()
}

func FrameNonceSlot(sender common.Address, key uint256.Int) common.Hash {
	var input [64]byte
	copy(input[12:32], sender[:])
	key.PutUint256(input[32:])
	return crypto.Keccak256Hash(input[:])
}

// NonceKeysHash uses fixed-width words for the count and keys, as required by
// transaction introspection. This commitment is distinct from the RLP encoding.
func (tx *FrameTransaction) NonceKeysHash() common.Hash {
	input := make([]byte, 32*(1+len(tx.NonceKeys)))
	uint256.NewInt(uint64(len(tx.NonceKeys))).PutUint256(input[:32])
	for i := range tx.NonceKeys {
		tx.NonceKeys[i].PutUint256(input[32*(i+1):])
	}
	return crypto.Keccak256Hash(input)
}

func (tx *FrameTransaction) MarshalBinary(w io.Writer) error {
	if _, err := w.Write([]byte{FrameTxType}); err != nil {
		return err
	}
	return rlp.Encode(w, (*frameTransactionRLP)(tx))
}

func (tx *FrameTransaction) EncodeRLP(w io.Writer) error {
	var payload bytes.Buffer
	if err := tx.MarshalBinary(&payload); err != nil {
		return err
	}
	return rlp.Encode(w, payload.Bytes())
}

func (tx *FrameTransaction) EncodingSize() int {
	payload, err := rlp.EncodeToBytes((*frameTransactionRLP)(tx))
	if err != nil {
		panic(err)
	}
	return 1 + len(payload)
}

func (tx *FrameTransaction) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash
	}
	hash := prefixedRlpHash(FrameTxType, (*frameTransactionRLP)(tx))
	tx.hash.Store(&hash)
	return hash
}

func (tx *FrameTransaction) SigningHash(*uint256.Int) common.Hash {
	// Empty-message entries sign this transaction, so omit their signatures to
	// avoid self-reference. Signatures over explicit messages stay in the hash.
	signatures := slices.Clone(tx.Signatures)
	for i := range signatures {
		if len(signatures[i].Message) == 0 {
			signatures[i].Signature = nil
		}
	}
	return prefixedRlpHash(FrameTxType, []any{
		tx.ChainID, tx.NonceKeys, tx.Nonce, tx.SenderAddress, tx.Frames,
		signatures, tx.Fees, tx.BlobVersionedHashes,
	})
}
