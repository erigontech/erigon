package types

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/types"
	"github.com/holiman/uint256"
	"io"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/rlp"
)

// Returns true if nonce checks should be skipped based on inner's isFake()
// This also disables requiring that sender is an EOA and not a contract
func (tx *ArbTx) SkipAccountChecks() bool {
	//return tx.inner.skipAccountChecks()
	return false
}

type fallbackError struct {
}

var fallbackErrorMsg = "missing trie node 0000000000000000000000000000000000000000000000000000000000000000 (path ) <nil>"
var fallbackErrorCode = -32000

func SetFallbackError(msg string, code int) {
	fallbackErrorMsg = msg
	fallbackErrorCode = code
	log.Debug("setting fallback error", "msg", msg, "code", code)
}

func (f fallbackError) ErrorCode() int { return fallbackErrorCode }
func (f fallbackError) Error() string  { return fallbackErrorMsg }

var ErrUseFallback = fallbackError{}

type FallbackClient interface {
	CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error
}

var bigZero = big.NewInt(0)

func (tx *LegacyTx) skipAccountChecks() bool              { return false }
func (tx *AccessListTx) skipAccountChecks() bool          { return false }
func (tx *DynamicFeeTransaction) skipAccountChecks() bool { return false }

func (tx *ArbitrumUnsignedTx) skipAccountChecks() bool        { return false }
func (tx *ArbitrumContractTx) skipAccountChecks() bool        { return true }
func (tx *ArbitrumRetryTx) skipAccountChecks() bool           { return true }
func (tx *ArbitrumSubmitRetryableTx) skipAccountChecks() bool { return true }
func (d *ArbitrumDepositTx) skipAccountChecks() bool          { return true }
func (t *ArbitrumInternalTx) skipAccountChecks() bool         { return true }

type ArbitrumUnsignedTx struct {
	ChainId *big.Int
	From    common.Address

	Nonce     uint64          // nonce of sender account
	GasFeeCap *big.Int        // wei per gas
	Gas       uint64          // gas limit
	To        *common.Address `rlp:"nil"` // nil means contract creation
	Value     *big.Int        // wei amount
	Data      []byte          // contract invocation input data
}

func (tx *ArbitrumUnsignedTx) copy() Transaction {
	cpy := &ArbitrumUnsignedTx{
		ChainId:   new(big.Int),
		Nonce:     tx.Nonce,
		GasFeeCap: new(big.Int),
		Gas:       tx.Gas,
		From:      tx.From,
		To:        nil,
		Value:     new(big.Int),
		Data:      common.Copy(tx.Data),
	}
	if tx.ChainId != nil {
		cpy.ChainId.Set(tx.ChainId)
	}
	if tx.GasFeeCap != nil {
		cpy.GasFeeCap.Set(tx.GasFeeCap)
	}
	if tx.To != nil {
		tmp := *tx.To
		cpy.To = &tmp
	}
	if tx.Value != nil {
		cpy.Value.Set(tx.Value)
	}
	return cpy
}

func (tx *ArbitrumUnsignedTx) Type() byte { return ArbitrumUnsignedTxType }

func (tx *ArbitrumUnsignedTx) GetChainID() *uint256.Int {
	ub, ok := uint256.FromBig(tx.ChainId)
	if !ok {
		panic("invalid chain id")
	}
	return ub
}

func (tx *ArbitrumUnsignedTx) GetNonce() uint64 { return tx.Nonce }

func (tx *ArbitrumUnsignedTx) GetPrice() *uint256.Int {
	return uint256.MustFromBig(tx.GasFeeCap)
}

func (tx *ArbitrumUnsignedTx) GetTip() *uint256.Int { return uintZero }

func (tx *ArbitrumUnsignedTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetPrice()
	}
	res := uint256.NewInt(0)
	return res.Set(baseFee)
}

func (tx *ArbitrumUnsignedTx) GetFeeCap() *uint256.Int {
	return uint256.MustFromBig(tx.GasFeeCap)
}

func (tx *ArbitrumUnsignedTx) GetBlobHashes() []common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) GetGas() uint64 { return tx.Gas }

func (tx *ArbitrumUnsignedTx) GetBlobGas() uint64 {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) GetValue() *uint256.Int {
	return uint256.MustFromBig(tx.Value)
}

func (tx *ArbitrumUnsignedTx) GetTo() *common.Address { return tx.To }

func (tx *ArbitrumUnsignedTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) Hash() common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) SigningHash(chainID *big.Int) common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) GetData() []byte {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) GetAccessList() types.AccessList { return nil }

func (tx *ArbitrumUnsignedTx) Protected() bool {
	//TODO implement me
	panic("implement me")
}

var uintZero = uint256.NewInt(0)

func (tx *ArbitrumUnsignedTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return uintZero, uintZero, uintZero
}

func (tx *ArbitrumUnsignedTx) EncodingSize() int {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) EncodeRLP(w io.Writer) error {
	//rlp.encode
	return tx.encode(w)
	//rlp.Write(w, tx.encode(w))
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) DecodeRLP(s *rlp.Stream) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) MarshalBinary(w io.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) Sender(signer Signer) (common.Address, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) cachedSender() (common.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) GetSender() (common.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) SetSender(address common.Address) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) IsContractDeploy() bool {
	return false
}

func (tx *ArbitrumUnsignedTx) Unwrap() Transaction {
	//TODO implement me
	panic("implement me")
}

//func (tx *ArbitrumUnsignedTx) data() []byte { return tx.Data }

// func (tx *ArbitrumUnsignedTx) gas() uint64         {  }
// func (tx *ArbitrumUnsignedTx) gasPrice() *big.Int  { return tx.GasFeeCap }
// func (tx *ArbitrumUnsignedTx) gasTipCap() *big.Int { return bigZero }
// func (tx *ArbitrumUnsignedTx) gasFeeCap() *big.Int { return tx.GasFeeCap }
// func (tx *ArbitrumUnsignedTx) value() *big.Int     { return tx.Value }
// func (tx *ArbitrumUnsignedTx) nonce() uint64       {  }
// func (tx *ArbitrumUnsignedTx) to() *common.Address { return tx.To }
func (tx *ArbitrumUnsignedTx) encode(b io.Writer) error {
	return rlp.Encode(b, tx)
}
func (tx *ArbitrumUnsignedTx) decode(input []byte) error {
	return rlp.DecodeBytes(input, tx)
}

func (tx *ArbitrumUnsignedTx) setSignatureValues(chainID, v, r, s *big.Int) {}

//func (tx *ArbitrumUnsignedTx) effectiveGasPrice(dst *big.Int, baseFee *big.Int) *big.Int {
//	if baseFee == nil {
//		return dst.Set(tx.GasFeeCap)
//	}
//	return dst.Set(baseFee)
//}

type ArbitrumContractTx struct {
	ChainId   *big.Int
	RequestId common.Hash
	From      common.Address

	GasFeeCap *big.Int        // wei per gas
	Gas       uint64          // gas limit
	To        *common.Address `rlp:"nil"` // nil means contract creation
	Value     *big.Int        // wei amount
	Data      []byte          // contract invocation input data
}

func (tx *ArbitrumContractTx) copy() *ArbitrumContractTx {
	cpy := &ArbitrumContractTx{
		ChainId:   new(big.Int),
		RequestId: tx.RequestId,
		GasFeeCap: new(big.Int),
		Gas:       tx.Gas,
		From:      tx.From,
		To:        nil,
		Value:     new(big.Int),
		Data:      common.CopyBytes(tx.Data),
	}
	if tx.ChainId != nil {
		cpy.ChainId.Set(tx.ChainId)
	}
	if tx.GasFeeCap != nil {
		cpy.GasFeeCap.Set(tx.GasFeeCap)
	}
	if tx.To != nil {
		tmp := *tx.To
		cpy.To = &tmp
	}
	if tx.Value != nil {
		cpy.Value.Set(tx.Value)
	}
	return cpy
}
func (tx *ArbitrumContractTx) Type() byte { return ArbitrumContractTxType }
func (tx *ArbitrumContractTx) GetChainID() *uint256.Int {
	ub, ok := uint256.FromBig(tx.ChainId)
	if !ok {
		panic("invalid chain id")
	}
	return ub
}
func (tx *ArbitrumContractTx) GetNonce() uint64 { return 0 }
func (tx *ArbitrumContractTx) GetPrice() *uint256.Int {
	return uint256.MustFromBig(tx.GasFeeCap)
}
func (tx *ArbitrumContractTx) GetTip() *uint256.Int { return uintZero }
func (tx *ArbitrumContractTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetPrice()
	}
	res := uint256.NewInt(0)
	return res.Set(baseFee)
}
func (tx *ArbitrumContractTx) GetFeeCap() *uint256.Int {
	return uint256.MustFromBig(tx.GasFeeCap)
}
func (tx *ArbitrumContractTx) GetBlobHashes() []common.Hash {
	//TODO implement me
	panic("implement me")
}
func (tx *ArbitrumContractTx) GetGas() uint64 { return tx.Gas }
func (tx *ArbitrumContractTx) GetBlobGas() uint64 {
	//TODO implement me
	panic("implement me")
}
func (tx *ArbitrumContractTx) GetData() []byte { return tx.Data }

func (tx *ArbitrumContractTx) GetValue() *uint256.Int {
	return uint256.MustFromBig(tx.Value)
}
func (tx *ArbitrumContractTx) GetTo() *common.Address          { return tx.To }
func (tx *ArbitrumContractTx) GetAccessList() types.AccessList { return nil }
func (tx *ArbitrumContractTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return uintZero, uintZero, uintZero
}

func (tx *ArbitrumContractTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) Hash() common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) SigningHash(chainID *big.Int) common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) Protected() bool {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) EncodingSize() int {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) EncodeRLP(w io.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) DecodeRLP(s *rlp.Stream) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) MarshalBinary(w io.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) Sender(signer Signer) (common.Address, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) cachedSender() (common.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) GetSender() (common.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) SetSender(address common.Address) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) IsContractDeploy() bool {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) Unwrap() Transaction {
	return tx
}

// func (tx *ArbitrumContractTx) ChainID() *big.Int            { return tx.ChainId }
// func (tx *ArbitrumContractTx) accessList() types.AccessList { return nil }
// func (tx *ArbitrumContractTx) data() []byte { return tx.Data }
// func (tx *ArbitrumContractTx) gas() uint64                  { return tx.Gas }
// func (tx *ArbitrumContractTx) gasPrice() *big.Int           { return tx.GasFeeCap }
// func (tx *ArbitrumContractTx) gasTipCap() *big.Int          { return bigZero }
// func (tx *ArbitrumContractTx) gasFeeCap() *big.Int          { return tx.GasFeeCap }
// func (tx *ArbitrumContractTx) value() *big.Int { return tx.Value }
// func (tx *ArbitrumContractTx) nonce() uint64                { return 0 }
// func (tx *ArbitrumContractTx) to() *common.Address          { return tx.To }
func (tx *ArbitrumContractTx) encode(b *bytes.Buffer) error {
	return rlp.Encode(b, tx)
}
func (tx *ArbitrumContractTx) decode(input []byte) error {
	return rlp.DecodeBytes(input, tx)
}

//	func (tx *ArbitrumContractTx) rawSignatureValues() (v, r, s *big.Int) {
//		return bigZero, bigZero, bigZero
//	}
func (tx *ArbitrumContractTx) setSignatureValues(chainID, v, r, s *big.Int) {}

//func (tx *ArbitrumContractTx) effectiveGasPrice(dst *big.Int, baseFee *big.Int) *big.Int {
//	if baseFee == nil {
//		return dst.Set(tx.GasFeeCap)
//	}
//	return dst.Set(baseFee)
//}

type ArbitrumRetryTx struct {
	ChainId *big.Int
	Nonce   uint64
	From    common.Address

	GasFeeCap           *big.Int        // wei per gas
	Gas                 uint64          // gas limit
	To                  *common.Address `rlp:"nil"` // nil means contract creation
	Value               *big.Int        // wei amount
	Data                []byte          // contract invocation input data
	TicketId            common.Hash
	RefundTo            common.Address
	MaxRefund           *big.Int // the maximum refund sent to RefundTo (the rest goes to From)
	SubmissionFeeRefund *big.Int // the submission fee to refund if successful (capped by MaxRefund)
}

func (tx *ArbitrumRetryTx) Type() byte { return ArbitrumRetryTxType }

func (tx *ArbitrumRetryTx) copy() *ArbitrumRetryTx {
	cpy := &ArbitrumRetryTx{
		ChainId:             new(big.Int),
		Nonce:               tx.Nonce,
		GasFeeCap:           new(big.Int),
		Gas:                 tx.Gas,
		From:                tx.From,
		To:                  nil,
		Value:               new(big.Int),
		Data:                common.CopyBytes(tx.Data),
		TicketId:            tx.TicketId,
		RefundTo:            tx.RefundTo,
		MaxRefund:           new(big.Int),
		SubmissionFeeRefund: new(big.Int),
	}
	if tx.ChainId != nil {
		cpy.ChainId.Set(tx.ChainId)
	}
	if tx.GasFeeCap != nil {
		cpy.GasFeeCap.Set(tx.GasFeeCap)
	}
	if tx.To != nil {
		tmp := *tx.To
		cpy.To = &tmp
	}
	if tx.Value != nil {
		cpy.Value.Set(tx.Value)
	}
	if tx.MaxRefund != nil {
		cpy.MaxRefund.Set(tx.MaxRefund)
	}
	if tx.SubmissionFeeRefund != nil {
		cpy.SubmissionFeeRefund.Set(tx.SubmissionFeeRefund)
	}
	return cpy
}

func (tx *ArbitrumRetryTx) GetChainID() *uint256.Int {
	ub, ok := uint256.FromBig(tx.ChainId)
	if !ok {
		panic("invalid chain id")
	}
	return ub
}
func (tx *ArbitrumRetryTx) GetNonce() uint64 { return tx.Nonce }
func (tx *ArbitrumRetryTx) GetPrice() *uint256.Int {
	return uint256.MustFromBig(tx.GasFeeCap)
}
func (tx *ArbitrumRetryTx) GetTip() *uint256.Int { return uintZero }
func (tx *ArbitrumRetryTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetPrice()
	}
	res := uint256.NewInt(0)
	return res.Set(baseFee)
}
func (tx *ArbitrumRetryTx) GetFeeCap() *uint256.Int {
	return uint256.MustFromBig(tx.GasFeeCap)
}
func (tx *ArbitrumRetryTx) GetBlobHashes() []common.Hash {
	//TODO implement me
	panic("implement me")
}
func (tx *ArbitrumRetryTx) GetGas() uint64 { return tx.Gas }
func (tx *ArbitrumRetryTx) GetBlobGas() uint64 {
	//TODO implement me
	panic("implement me")
}
func (tx *ArbitrumRetryTx) GetData() []byte { return tx.Data }
func (tx *ArbitrumRetryTx) GetValue() *uint256.Int {
	return uint256.MustFromBig(tx.Value)
}
func (tx *ArbitrumRetryTx) GetTo() *common.Address          { return tx.To }
func (tx *ArbitrumRetryTx) GetAccessList() types.AccessList { return nil }
func (tx *ArbitrumRetryTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return uintZero, uintZero, uintZero
}

func (tx *ArbitrumRetryTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) Hash() common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) SigningHash(chainID *big.Int) common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) Protected() bool {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) EncodingSize() int {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) EncodeRLP(w io.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) DecodeRLP(s *rlp.Stream) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) MarshalBinary(w io.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) Sender(signer Signer) (common.Address, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) cachedSender() (common.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) GetSender() (common.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) SetSender(address common.Address) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) IsContractDeploy() bool {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumRetryTx) Unwrap() Transaction {
	return tx
}

// func (tx *ArbitrumRetryTx) chainID() *big.Int            { return tx.ChainId }
// func (tx *ArbitrumRetryTx) accessList() types.AccessList { return nil }
// func (tx *ArbitrumRetryTx) data() []byte                 { return tx.Data }
// func (tx *ArbitrumRetryTx) gas() uint64                  { return tx.Gas }
// func (tx *ArbitrumRetryTx) gasPrice() *big.Int           { return tx.GasFeeCap }
// func (tx *ArbitrumRetryTx) gasTipCap() *big.Int          { return bigZero }
// func (tx *ArbitrumRetryTx) gasFeeCap() *big.Int          { return tx.GasFeeCap }
// func (tx *ArbitrumRetryTx) value() *big.Int              { return tx.Value }
// func (tx *ArbitrumRetryTx) nonce() uint64                { return tx.Nonce }
// func (tx *ArbitrumRetryTx) to() *common.Address          { return tx.To }
func (tx *ArbitrumRetryTx) encode(b *bytes.Buffer) error {
	return rlp.Encode(b, tx)
}
func (tx *ArbitrumRetryTx) decode(input []byte) error {
	return rlp.DecodeBytes(input, tx)
}

//	func (tx *ArbitrumRetryTx) rawSignatureValues() (v, r, s *big.Int) {
//		return bigZero, bigZero, bigZero
//	}
func (tx *ArbitrumRetryTx) setSignatureValues(chainID, v, r, s *big.Int) {}

//func (tx *ArbitrumRetryTx) effectiveGasPrice(dst *big.Int, baseFee *big.Int) *big.Int {
//	if baseFee == nil {
//		return dst.Set(tx.GasFeeCap)
//	}
//	return dst.Set(baseFee)
//}

type ArbitrumSubmitRetryableTx struct {
	ChainId   *big.Int
	RequestId common.Hash
	From      common.Address
	L1BaseFee *big.Int

	DepositValue     *big.Int
	GasFeeCap        *big.Int        // wei per gas
	Gas              uint64          // gas limit
	RetryTo          *common.Address `rlp:"nil"` // nil means contract creation
	RetryValue       *big.Int        // wei amount
	Beneficiary      common.Address
	MaxSubmissionFee *big.Int
	FeeRefundAddr    common.Address
	RetryData        []byte // contract invocation input data
}

func (tx *ArbitrumSubmitRetryableTx) Type() byte { return ArbitrumSubmitRetryableTxType }

func (tx *ArbitrumSubmitRetryableTx) copy() *ArbitrumSubmitRetryableTx {
	cpy := &ArbitrumSubmitRetryableTx{
		ChainId:          new(big.Int),
		RequestId:        tx.RequestId,
		DepositValue:     new(big.Int),
		L1BaseFee:        new(big.Int),
		GasFeeCap:        new(big.Int),
		Gas:              tx.Gas,
		From:             tx.From,
		RetryTo:          tx.RetryTo,
		RetryValue:       new(big.Int),
		Beneficiary:      tx.Beneficiary,
		MaxSubmissionFee: new(big.Int),
		FeeRefundAddr:    tx.FeeRefundAddr,
		RetryData:        common.CopyBytes(tx.RetryData),
	}
	if tx.ChainId != nil {
		cpy.ChainId.Set(tx.ChainId)
	}
	if tx.DepositValue != nil {
		cpy.DepositValue.Set(tx.DepositValue)
	}
	if tx.L1BaseFee != nil {
		cpy.L1BaseFee.Set(tx.L1BaseFee)
	}
	if tx.GasFeeCap != nil {
		cpy.GasFeeCap.Set(tx.GasFeeCap)
	}
	if tx.RetryTo != nil {
		tmp := *tx.RetryTo
		cpy.RetryTo = &tmp
	}
	if tx.RetryValue != nil {
		cpy.RetryValue.Set(tx.RetryValue)
	}
	if tx.MaxSubmissionFee != nil {
		cpy.MaxSubmissionFee.Set(tx.MaxSubmissionFee)
	}
	return cpy
}

func (tx *ArbitrumSubmitRetryableTx) GetChainID() *uint256.Int {
	ub, ok := uint256.FromBig(tx.ChainId)
	if !ok {
		panic("invalid chain id")
	}
	return ub
}
func (tx *ArbitrumSubmitRetryableTx) GetNonce() uint64 { return 0 }
func (tx *ArbitrumSubmitRetryableTx) GetPrice() *uint256.Int {
	return uint256.MustFromBig(tx.GasFeeCap)
}
func (tx *ArbitrumSubmitRetryableTx) GetTip() *uint256.Int { return uintZero }
func (tx *ArbitrumSubmitRetryableTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetPrice()
	}
	res := uint256.NewInt(0)
	return res.Set(baseFee)
}
func (tx *ArbitrumSubmitRetryableTx) GetFeeCap() *uint256.Int {
	return uint256.MustFromBig(tx.GasFeeCap)
}
func (tx *ArbitrumSubmitRetryableTx) GetBlobHashes() []common.Hash {
	//TODO implement me
	panic("implement me")
}
func (tx *ArbitrumSubmitRetryableTx) GetGas() uint64 { return tx.Gas }
func (tx *ArbitrumSubmitRetryableTx) GetBlobGas() uint64 {
	//TODO implement me
	panic("implement me")
}
func (tx *ArbitrumSubmitRetryableTx) GetData() []byte {
	var retryTo common.Address
	if tx.RetryTo != nil {
		retryTo = *tx.RetryTo
	}
	data := make([]byte, 0)
	data = append(data, tx.RequestId.Bytes()...)
	data = append(data, math.U256Bytes(tx.L1BaseFee)...)
	data = append(data, math.U256Bytes(tx.DepositValue)...)
	data = append(data, math.U256Bytes(tx.RetryValue)...)
	data = append(data, math.U256Bytes(tx.GasFeeCap)...)
	data = append(data, math.U256Bytes(new(big.Int).SetUint64(tx.Gas))...)
	data = append(data, math.U256Bytes(tx.MaxSubmissionFee)...)
	data = append(data, make([]byte, 12)...)
	data = append(data, tx.FeeRefundAddr.Bytes()...)
	data = append(data, make([]byte, 12)...)
	data = append(data, tx.Beneficiary.Bytes()...)
	data = append(data, make([]byte, 12)...)
	data = append(data, retryTo.Bytes()...)
	offset := len(data) + 32
	data = append(data, math.U256Bytes(big.NewInt(int64(offset)))...)
	data = append(data, math.U256Bytes(big.NewInt(int64(len(tx.RetryData))))...)
	data = append(data, tx.RetryData...)
	extra := len(tx.RetryData) % 32
	if extra > 0 {
		data = append(data, make([]byte, 32-extra)...)
	}
	data = append(hexutil.MustDecode("0xc9f95d32"), data...)
	return data
}

func (tx *ArbitrumSubmitRetryableTx) GetValue() *uint256.Int {
	return uint256.MustFromBig(common.Big0)
}
func (tx *ArbitrumSubmitRetryableTx) GetTo() *common.Address          { return &ArbRetryableTxAddress }
func (tx *ArbitrumSubmitRetryableTx) GetAccessList() types.AccessList { return nil }
func (tx *ArbitrumSubmitRetryableTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return uintZero, uintZero, uintZero
}

func (tx *ArbitrumSubmitRetryableTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) Hash() common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) SigningHash(chainID *big.Int) common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) Protected() bool {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) EncodingSize() int {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) EncodeRLP(w io.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) DecodeRLP(s *rlp.Stream) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) MarshalBinary(w io.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) Sender(signer Signer) (common.Address, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) cachedSender() (common.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) GetSender() (common.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) SetSender(address common.Address) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) IsContractDeploy() bool {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) Unwrap() Transaction {
	return tx
}

// func (tx *ArbitrumSubmitRetryableTx) chainID() *big.Int            { return tx.ChainId }
// func (tx *ArbitrumSubmitRetryableTx) accessList() types.AccessList { return nil }
// func (tx *ArbitrumSubmitRetryableTx) gas() uint64                  { return tx.Gas }
// func (tx *ArbitrumSubmitRetryableTx) gasPrice() *big.Int           { return tx.GasFeeCap }
// func (tx *ArbitrumSubmitRetryableTx) gasTipCap() *big.Int { return big.NewInt(0) }
// func (tx *ArbitrumSubmitRetryableTx) gasFeeCap() *big.Int { return tx.GasFeeCap }
// func (tx *ArbitrumSubmitRetryableTx) value() *big.Int     { return common.Big0 }
// func (tx *ArbitrumSubmitRetryableTx) nonce() uint64       { return 0 }
// func (tx *ArbitrumSubmitRetryableTx) to() *common.Address { return &ArbRetryableTxAddress }
func (tx *ArbitrumSubmitRetryableTx) encode(b *bytes.Buffer) error {
	return rlp.Encode(b, tx)
}
func (tx *ArbitrumSubmitRetryableTx) decode(input []byte) error {
	return rlp.DecodeBytes(input, tx)
}

//func (tx *ArbitrumSubmitRetryableTx) rawSignatureValues() (v, r, s *big.Int) {
//	return bigZero, bigZero, bigZero
//}
//func (tx *ArbitrumSubmitRetryableTx) setSignatureValues(chainID, v, r, s *big.Int) {}
//
//func (tx *ArbitrumSubmitRetryableTx) effectiveGasPrice(dst *big.Int, baseFee *big.Int) *big.Int {
//	if baseFee == nil {
//		return dst.Set(tx.GasFeeCap)
//	}
//	return dst.Set(baseFee)
//}
//
//func (tx *ArbitrumSubmitRetryableTx) data() []byte {
//}

type ArbitrumDepositTx struct {
	ChainId     *big.Int
	L1RequestId common.Hash
	From        common.Address
	To          common.Address
	Value       *big.Int
}

func (d *ArbitrumDepositTx) Type() byte {
	return ArbitrumDepositTxType
}

func (d *ArbitrumDepositTx) copy() *ArbitrumDepositTx {
	tx := &ArbitrumDepositTx{
		ChainId:     new(big.Int),
		L1RequestId: d.L1RequestId,
		From:        d.From,
		To:          d.To,
		Value:       new(big.Int),
	}
	if d.ChainId != nil {
		tx.ChainId.Set(d.ChainId)
	}
	if d.Value != nil {
		tx.Value.Set(d.Value)
	}
	return tx
}

func (tx *ArbitrumDepositTx) GetChainID() *uint256.Int {
	ub, ok := uint256.FromBig(tx.ChainId)
	if !ok {
		panic("invalid chain id")
	}
	return ub
}
func (tx *ArbitrumDepositTx) GetNonce() uint64       { return 0 }
func (tx *ArbitrumDepositTx) GetPrice() *uint256.Int { return uintZero }
func (tx *ArbitrumDepositTx) GetTip() *uint256.Int   { return uintZero }
func (tx *ArbitrumDepositTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	return uint256.NewInt(0)
}
func (tx *ArbitrumDepositTx) GetFeeCap() *uint256.Int { return uintZero }
func (tx *ArbitrumDepositTx) GetBlobHashes() []common.Hash {
	//TODO implement me
	panic("implement me")
}
func (tx *ArbitrumDepositTx) GetGas() uint64 { return 0 }
func (tx *ArbitrumDepositTx) GetBlobGas() uint64 {
	//TODO implement me
	panic("implement me")
}
func (tx *ArbitrumDepositTx) GetData() []byte { return nil }
func (tx *ArbitrumDepositTx) GetValue() *uint256.Int {
	return uint256.MustFromBig(tx.Value)
}
func (tx *ArbitrumDepositTx) GetTo() *common.Address          { return &tx.To }
func (tx *ArbitrumDepositTx) GetAccessList() types.AccessList { return nil }
func (tx *ArbitrumDepositTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return uintZero, uintZero, uintZero
}

func (d *ArbitrumDepositTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) Hash() common.Hash {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) SigningHash(chainID *big.Int) common.Hash {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) Protected() bool {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) EncodingSize() int {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) EncodeRLP(w io.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) DecodeRLP(s *rlp.Stream) error {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) MarshalBinary(w io.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) Sender(signer Signer) (common.Address, error) {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) cachedSender() (common.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) GetSender() (common.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) SetSender(address common.Address) {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) IsContractDeploy() bool {
	//TODO implement me
	panic("implement me")
}

func (d *ArbitrumDepositTx) Unwrap() Transaction {
	return d
}

// func (d *ArbitrumDepositTx) chainID() *big.Int            { return d.ChainId }
// func (d *ArbitrumDepositTx) accessList() types.AccessList { return nil }
// func (d *ArbitrumDepositTx) data() []byte                 { return nil }
// func (d *ArbitrumDepositTx) gas() uint64                  { return 0 }
// func (d *ArbitrumDepositTx) gasPrice() *big.Int           { return bigZero }
// func (d *ArbitrumDepositTx) gasTipCap() *big.Int          { return bigZero }
// func (d *ArbitrumDepositTx) gasFeeCap() *big.Int          { return bigZero }
// func (d *ArbitrumDepositTx) value() *big.Int     { return d.Value }
// func (d *ArbitrumDepositTx) nonce() uint64       { return 0 }
// func (d *ArbitrumDepositTx) to() *common.Address { return &d.To }
func (d *ArbitrumDepositTx) encode(b *bytes.Buffer) error {
	return rlp.Encode(b, d)
}
func (d *ArbitrumDepositTx) decode(input []byte) error {
	return rlp.DecodeBytes(input, d)
}

//func (d *ArbitrumDepositTx) rawSignatureValues() (v, r, s *big.Int) {
//	return bigZero, bigZero, bigZero
//}
//func (d *ArbitrumDepositTx) setSignatureValues(chainID, v, r, s *big.Int) {}
//func (tx *ArbitrumDepositTx) effectiveGasPrice(dst *big.Int, baseFee *big.Int) *big.Int {
//	return dst.Set(bigZero)
//}

type ArbitrumInternalTx struct {
	ChainId *big.Int
	Data    []byte
}

func (t *ArbitrumInternalTx) Type() byte {
	return ArbitrumInternalTxType
}

func (t *ArbitrumInternalTx) copy() *ArbitrumInternalTx {
	return &ArbitrumInternalTx{
		new(big.Int).Set(t.ChainId),
		common.CopyBytes(t.Data),
	}
}

func (tx *ArbitrumInternalTx) GetChainID() *uint256.Int {
	ub, ok := uint256.FromBig(tx.ChainId)
	if !ok {
		panic("invalid chain id")
	}
	return ub
}
func (tx *ArbitrumInternalTx) GetNonce() uint64       { return 0 }
func (tx *ArbitrumInternalTx) GetPrice() *uint256.Int { return uintZero }
func (tx *ArbitrumInternalTx) GetTip() *uint256.Int   { return uintZero }
func (tx *ArbitrumInternalTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	return uint256.NewInt(0)
}
func (tx *ArbitrumInternalTx) GetFeeCap() *uint256.Int { return uintZero }
func (tx *ArbitrumInternalTx) GetBlobHashes() []common.Hash {
	//TODO implement me
	panic("implement me")
}
func (tx *ArbitrumInternalTx) GetGas() uint64 { return 0 }
func (tx *ArbitrumInternalTx) GetBlobGas() uint64 {
	//TODO implement me
	panic("implement me")
}
func (tx *ArbitrumInternalTx) GetData() []byte { return tx.Data }
func (tx *ArbitrumInternalTx) GetValue() *uint256.Int {
	return uint256.MustFromBig(common.Big0)
}
func (tx *ArbitrumInternalTx) GetTo() *common.Address          { return &ArbosAddress }
func (tx *ArbitrumInternalTx) GetAccessList() types.AccessList { return nil }
func (tx *ArbitrumInternalTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return uintZero, uintZero, uintZero
}

func (tx *ArbitrumInternalTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) Hash() common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) SigningHash(chainID *big.Int) common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) Protected() bool {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) EncodingSize() int {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) EncodeRLP(w io.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) DecodeRLP(s *rlp.Stream) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) MarshalBinary(w io.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) Sender(signer Signer) (common.Address, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) cachedSender() (common.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) GetSender() (common.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) SetSender(address common.Address) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumInternalTx) IsContractDeploy() bool {
	return false
}

func (tx *ArbitrumInternalTx) Unwrap() Transaction {
	return tx
}

// func (t *ArbitrumInternalTx) chainID() *big.Int            { return t.ChainId }
// func (t *ArbitrumInternalTx) accessList() types.AccessList { return nil }
// func (t *ArbitrumInternalTx) data() []byte                 { return t.Data }
// func (t *ArbitrumInternalTx) gas() uint64                  { return 0 }
// func (t *ArbitrumInternalTx) gasPrice() *big.Int           { return bigZero }
// func (t *ArbitrumInternalTx) gasTipCap() *big.Int          { return bigZero }
// func (t *ArbitrumInternalTx) gasFeeCap() *big.Int          { return bigZero }
// func (t *ArbitrumInternalTx) value() *big.Int              { return common.Big0 }
// func (t *ArbitrumInternalTx) nonce() uint64                { return 0 }
// func (t *ArbitrumInternalTx) to() *common.Address          { return &ArbosAddress }
func (t *ArbitrumInternalTx) encode(b *bytes.Buffer) error {
	return rlp.Encode(b, t)
}
func (t *ArbitrumInternalTx) decode(input []byte) error {
	return rlp.DecodeBytes(input, t)
}

//func (d *ArbitrumInternalTx) rawSignatureValues() (v, r, s *big.Int) {
//	return bigZero, bigZero, bigZero
//}
//
//func (d *ArbitrumInternalTx) setSignatureValues(chainID, v, r, s *big.Int) {
//
//}
//
//func (tx *ArbitrumInternalTx) effectiveGasPrice(dst *big.Int, baseFee *big.Int) *big.Int {
//	return dst.Set(bigZero)
//}

type HeaderInfo struct {
	SendRoot           common.Hash
	SendCount          uint64
	L1BlockNumber      uint64
	ArbOSFormatVersion uint64
}

func (info HeaderInfo) extra() []byte {
	return info.SendRoot[:]
}

func (info HeaderInfo) mixDigest() [32]byte {
	mixDigest := common.Hash{}
	binary.BigEndian.PutUint64(mixDigest[:8], info.SendCount)
	binary.BigEndian.PutUint64(mixDigest[8:16], info.L1BlockNumber)
	binary.BigEndian.PutUint64(mixDigest[16:24], info.ArbOSFormatVersion)
	return mixDigest
}

func (info HeaderInfo) UpdateHeaderWithInfo(header *Header) {
	header.MixDigest = info.mixDigest()
	header.Extra = info.extra()
}

func DeserializeHeaderExtraInformation(header *Header) HeaderInfo {
	if header == nil || header.BaseFee == nil || header.BaseFee.Sign() == 0 || len(header.Extra) != 32 || header.Difficulty.Cmp(common.Big1) != 0 {
		// imported blocks have no base fee
		// The genesis block doesn't have an ArbOS encoded extra field
		return HeaderInfo{}
	}
	extra := HeaderInfo{}
	copy(extra.SendRoot[:], header.Extra)
	extra.SendCount = binary.BigEndian.Uint64(header.MixDigest[:8])
	extra.L1BlockNumber = binary.BigEndian.Uint64(header.MixDigest[8:16])
	extra.ArbOSFormatVersion = binary.BigEndian.Uint64(header.MixDigest[16:24])
	return extra
}
