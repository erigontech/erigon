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
	FrameAssert
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

var (
	p256N     = elliptic.P256().Params().N
	p256HalfN = new(big.Int).Rsh(p256N, 1)
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
	MaxFee uint256.Int
	TipCap uint256.Int
}

type FrameFeeBudget struct {
	MaxCost  uint256.Int
	BlobCost uint256.Int
	GasPrice uint256.Int
}

var ErrFrameFeeBudget = errors.New("frame fee budget below base fees")

// Budget reserves base fees and as much of the priority fee as MaxFee permits.
// GASPRICE rounds the reserved priority fee down; settlement uses the remaining
// budget directly rather than multiplying that rounded price by gas used.
func (fees *FrameFees) Budget(gas, blobGas uint64, baseFee, blobBaseFee *uint256.Int) (budget FrameFeeBudget, err error) {
	var required, executionCost uint256.Int
	_, overflow := budget.BlobCost.MulOverflow(uint256.NewInt(blobGas), blobBaseFee)
	if overflow {
		return budget, ErrFrameFeeBudget
	}
	_, overflow = executionCost.MulOverflow(uint256.NewInt(gas), baseFee)
	if overflow {
		return budget, ErrFrameFeeBudget
	}
	_, overflow = required.AddOverflow(&executionCost, &budget.BlobCost)
	if overflow || required.Gt(&fees.MaxFee) {
		return budget, ErrFrameFeeBudget
	}
	available := new(uint256.Int).Sub(&fees.MaxFee, &required)
	priority := fees.PriorityCost(gas, available)
	budget.MaxCost.Add(&required, &priority)
	budget.GasPrice.Div(&priority, uint256.NewInt(gas))
	budget.GasPrice.Add(&budget.GasPrice, baseFee)
	return budget, nil
}

func (fees *FrameFees) PriorityCost(gas uint64, available *uint256.Int) uint256.Int {
	var priority uint256.Int
	if _, overflow := priority.MulOverflow(&fees.TipCap, uint256.NewInt(gas)); overflow || priority.Gt(available) {
		return *available
	}
	return priority
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
	verifiedSignatures  atomic.Pointer[frameSignatureValidation] `rlp:"-"`
}

type frameTransactionRLP FrameTransaction

type frameSignatureValidation struct {
	sender      common.Address
	signingHash common.Hash
	signatures  []FrameSignature
}

func (frame *Frame) ResolvedTarget(sender common.Address) common.Address {
	if frame.Target == nil {
		return sender
	}
	return *frame.Target
}

// IsStatic selects STATICCALL execution. VERIFY still allows APPROVE to change
// nonce and payment state; ASSERT forbids APPROVE.
func (frame *Frame) IsStatic() bool {
	return frame.Mode == FrameVerify || frame.Mode == FrameAssert
}

func (frame *Frame) IsExpiryVerifier() bool {
	return frame.Mode == FrameVerify && frame.Target != nil && *frame.Target == FrameExpiryVerifier
}

func (frame *Frame) IsRecentRootVerifier() bool {
	return frame.Mode == FrameVerify && frame.Flags == 0 && frame.Target != nil && *frame.Target == FrameRecentRootVerifier
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
		if r.Sign() == 0 || r.Cmp(p256N) >= 0 || s.Sign() == 0 || s.Cmp(p256HalfN) > 0 {
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
		if frame.Mode > FrameAssert || frame.Flags >= 8 {
			return fmt.Errorf("invalid frame %d mode or flags", i)
		}
		if i > 0 && tx.Frames[i-1].Mode == FrameAssert && frame.Mode != FrameAssert {
			return fmt.Errorf("invalid frame %d mode or flags: ASSERT frames must form a trailing suffix", i)
		}
		if frame.Mode != FrameSender && !frame.Value.IsZero() {
			return fmt.Errorf("frame %d value requires sender mode", i)
		}
		if frame.Flags&FrameApproveExecution != 0 && frame.ResolvedTarget(tx.SenderAddress) != tx.SenderAddress {
			return fmt.Errorf("frame %d execution approval requires sender target", i)
		}
		atomic := frame.Flags&FrameAtomicBatch != 0
		if atomic && (frame.IsStatic() || i+1 == len(tx.Frames) || tx.Frames[i+1].IsStatic()) {
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
	return tx.validateSignatures()
}

func (tx *FrameTransaction) validateSignatures() error {
	if len(tx.Signatures) == 0 {
		return nil
	}
	var signingHash common.Hash
	if slices.ContainsFunc(tx.Signatures, func(sig FrameSignature) bool {
		return (sig.Scheme == FrameSignatureSecp256k1 || sig.Scheme == FrameSignatureP256) && len(sig.Message) == 0
	}) {
		signingHash = tx.SigningHash(nil)
	}
	// Successful cryptographic checks are cached; Validate still checks structure and gas.
	// Copied signer, message and signature bytes, plus a fresh digest for signatures over
	// the transaction, prevent changed inputs from reusing an earlier result.
	// Arbitrary payload bytes are not validated, so they are not compared or copied.
	if cached := tx.verifiedSignatures.Load(); cached != nil && cached.sender == tx.SenderAddress && cached.signingHash == signingHash &&
		slices.EqualFunc(tx.Signatures, cached.signatures, func(a, b FrameSignature) bool {
			return a.Scheme == b.Scheme && bytes.Equal(a.Signer, b.Signer) && bytes.Equal(a.Message, b.Message) &&
				(a.Scheme == FrameSignatureArbitrary || bytes.Equal(a.Signature, b.Signature))
		}) {
		return nil
	}
	hasCrypto := false
	for i := range tx.Signatures {
		sig := &tx.Signatures[i]
		if err := sig.validate(tx.SenderAddress, signingHash); err != nil {
			return fmt.Errorf("invalid frame signature %d: %w", i, err)
		}
		hasCrypto = hasCrypto || sig.Scheme != FrameSignatureArbitrary
	}
	if hasCrypto {
		signatures := slices.Clone(tx.Signatures)
		for i := range signatures {
			sig := &signatures[i]
			sig.Signer = bytes.Clone(sig.Signer)
			sig.Message = bytes.Clone(sig.Message)
			if sig.Scheme == FrameSignatureArbitrary {
				sig.Signature = nil
			} else {
				sig.Signature = bytes.Clone(sig.Signature)
			}
		}
		tx.verifiedSignatures.Store(&frameSignatureValidation{
			sender: tx.SenderAddress, signingHash: signingHash, signatures: signatures,
		})
	}
	return nil
}

func (tx *FrameTransaction) Type() byte { return FrameTxType }

func (tx *FrameTransaction) GetChainID() *uint256.Int { return &tx.ChainID }

func (tx *FrameTransaction) GetNonce() uint64 { return tx.Nonce }

func (tx *FrameTransaction) GetTipCap() *uint256.Int { return &tx.Fees.TipCap }

func (tx *FrameTransaction) GetFeeCap() *uint256.Int {
	return tx.ExecutionFeeCap(new(uint256.Int))
}

// ExecutionFeeCap converts the total fee budget to a per-gas bound after blobs.
func (tx *FrameTransaction) ExecutionFeeCap(blobBaseFee *uint256.Int) *uint256.Int {
	var blobCost uint256.Int
	if _, overflow := blobCost.MulOverflow(uint256.NewInt(tx.GetBlobGas()), blobBaseFee); overflow || blobCost.Gt(&tx.Fees.MaxFee) {
		return new(uint256.Int)
	}
	return new(uint256.Int).Div(new(uint256.Int).Sub(&tx.Fees.MaxFee, &blobCost), uint256.NewInt(tx.GetGasLimit()))
}

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
	// EIP-8250 prices the full nonce encodings as calldata, including RLP prefixes.
	args.DataLen, args.DataNonZeroLen = tx.nonceDataCounts()
	addData := func(data []byte) {
		args.DataLen += uint64(len(data))
		args.DataNonZeroLen += uint64(mdgas.CountNonZeroBytes(data))
	}
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
		if len(sig.Message) == 0 {
			args.DataLen += uint64(len(sig.Signature))
			args.DataNonZeroLen += uint64(len(sig.Signature))
		} else {
			addData(sig.Signature)
		}
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

func (tx *FrameTransaction) nonceDataCounts() (size, nonZero uint64) {
	payloadLen := 0
	for _, key := range tx.NonceKeys {
		keyLen := rlp.Uint256Len(key)
		payloadLen += keyLen
		// Single-byte integers (including zero as 0x80) and integer prefixes
		// each contribute one nonzero byte.
		nonZero++
		if keyLen > 1 {
			keyBytes := key.Bytes32()
			nonZero += uint64(mdgas.CountNonZeroBytes(keyBytes[:]))
		}
	}
	var buf [9]byte
	prefixLen := rlp.EncodeListPrefixToBuf(payloadLen, buf[:])
	nonZero += uint64(mdgas.CountNonZeroBytes(buf[:prefixLen]))
	var nonceLen int
	if tx.UsesLegacyNonce() || tx.Nonce != math.MaxUint64 {
		nonceLen = rlp.EncodeU64ToBuf(tx.Nonce, buf[:])
	}
	nonZero += uint64(mdgas.CountNonZeroBytes(buf[:nonceLen]))
	return uint64(payloadLen + prefixLen + nonceLen), nonZero
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
	feeCap := *tx.GetFeeCap()
	msg := &Message{
		from: accounts.InternAddress(tx.SenderAddress), nonce: tx.Nonce,
		gasLimit: tx.GetGasLimit(), gasPrice: feeCap, feeCap: feeCap, tipCap: tx.Fees.TipCap,
		blobHashes: tx.BlobVersionedHashes,
		checkNonce: true, checkTransaction: true, checkGas: true,
		frameTransaction: tx,
	}
	if baseFee != nil {
		tip := CalcEffectiveGasTip(baseFee, msg.TipCap, msg.FeeCap)
		msg.gasPrice.Add(baseFee, &tip)
		if msg.gasPrice.Gt(&msg.feeCap) {
			msg.gasPrice.Set(&msg.feeCap)
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
	if len(tx.NonceKeys) > FrameMaxNonceKeys {
		return errors.New("invalid nonce key count")
	}
	if tx.UsesLegacyNonce() && tx.Nonce == math.MaxUint64 {
		return errors.New("nonce has max value")
	}
	for i := 1; i < len(tx.NonceKeys); i++ {
		if !tx.NonceKeys[i-1].Lt(&tx.NonceKeys[i]) {
			return errors.New("nonce keys not strictly increasing")
		}
	}
	return nil
}

// UsesLegacyNonce identifies the empty key set, which consumes the sender's account
// nonce. Every explicit key, including zero, selects FrameNonceManager storage.
func (tx *FrameTransaction) UsesLegacyNonce() bool {
	return len(tx.NonceKeys) == 0
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
	payloadSize := rlp.Uint256Len(tx.ChainID)
	nonceKeysSize := 0
	for _, key := range tx.NonceKeys {
		nonceKeysSize += rlp.Uint256Len(key)
	}
	payloadSize += rlp.ListLen(nonceKeysSize)
	payloadSize += rlp.U64Len(tx.Nonce) + rlp.StringLen(tx.SenderAddress[:])

	framesSize := 0
	for i := range tx.Frames {
		frame := &tx.Frames[i]
		targetSize := 1
		if frame.Target != nil {
			targetSize += len(*frame.Target)
		}
		frameSize := rlp.U64Len(uint64(frame.Mode)) + rlp.U64Len(uint64(frame.Flags)) + targetSize
		frameSize += rlp.ListLen(rlp.U64Len(frame.Limits.Execution) + rlp.U64Len(frame.Limits.State))
		frameSize += rlp.Uint256Len(frame.Value) + rlp.StringLen(frame.Data)
		framesSize += rlp.ListLen(frameSize)
	}
	payloadSize += rlp.ListLen(framesSize)

	signaturesSize := 0
	for i := range tx.Signatures {
		sig := &tx.Signatures[i]
		sigSize := rlp.U64Len(uint64(sig.Scheme)) + rlp.StringLen(sig.Signer) + rlp.StringLen(sig.Message) + rlp.StringLen(sig.Signature)
		signaturesSize += rlp.ListLen(sigSize)
	}
	payloadSize += rlp.ListLen(signaturesSize)
	payloadSize += rlp.ListLen(rlp.Uint256Len(tx.Fees.MaxFee) + rlp.Uint256Len(tx.Fees.TipCap))
	payloadSize += rlp.ListLen(blobVersionedHashesSize(tx.BlobVersionedHashes))
	return 1 + rlp.ListLen(payloadSize)
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
	// Empty-message signatures commit to their length instead of their bytes,
	// avoiding self-reference without allowing relayers to increase their gas cost.
	signatures := make([]any, len(tx.Signatures))
	for i, sig := range tx.Signatures {
		var signature any = sig.Signature
		if len(sig.Message) == 0 {
			signature = uint64(len(sig.Signature))
		}
		signatures[i] = []any{sig.Scheme, sig.Signer, sig.Message, signature}
	}
	return prefixedRlpHash(FrameTxType, []any{
		tx.ChainID, tx.NonceKeys, tx.Nonce, tx.SenderAddress, tx.Frames,
		signatures, tx.Fees, tx.BlobVersionedHashes,
	})
}
