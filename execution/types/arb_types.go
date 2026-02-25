package types

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"

	"github.com/erigontech/erigon/arb"
	"github.com/erigontech/erigon/arb/ethdb/wasmdb"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
)

// Returns true if nonce checks should be skipped based on inner's isFake()
// This also disables requiring that sender is an EOA and not a contract
func (tx *ArbTx) SkipAccountChecks() bool {
	// return tx.inner.skipAccountChecks()
	return skipAccountChecks[tx.Type()]
}

var fallbackErrorMsg = "missing trie node 0000000000000000000000000000000000000000000000000000000000000000 (path ) <nil>"
var fallbackErrorCode = -32000

func SetFallbackError(msg string, code int) {
	fallbackErrorMsg = msg
	fallbackErrorCode = code
	log.Debug("setting fallback error", "msg", msg, "code", code)
}

type fallbackError struct{}

func (f fallbackError) ErrorCode() int { return fallbackErrorCode }
func (f fallbackError) Error() string  { return fallbackErrorMsg }

var ErrUseFallback = fallbackError{}

type FallbackClient interface {
	CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error
}

var bigZero = big.NewInt(0)
var uintZero = uint256.NewInt(0)

var skipAccountChecks = [...]bool{
	ArbitrumDepositTxType:         true,
	ArbitrumRetryTxType:           true,
	ArbitrumSubmitRetryableTxType: true,
	ArbitrumInternalTxType:        true,
	ArbitrumContractTxType:        true,
	ArbitrumUnsignedTxType:        false,
}

// func (tx *LegacyTx) skipAccountChecks() bool                  { return false }
// func (tx *AccessListTx) skipAccountChecks() bool              { return false }
// func (tx *DynamicFeeTransaction) skipAccountChecks() bool     { return false }
// func (tx *ArbitrumUnsignedTx) skipAccountChecks() bool        { return false }
// func (tx *ArbitrumContractTx) skipAccountChecks() bool        { return true }
// func (tx *ArbitrumRetryTx) skipAccountChecks() bool           { return true }
// func (tx *ArbitrumSubmitRetryableTx) skipAccountChecks() bool { return true }
// func (tx *ArbitrumDepositTx) skipAccountChecks() bool         { return true }
// func (tx *ArbitrumInternalTx) skipAccountChecks() bool        { return true }

type ArbitrumUnsignedTx struct {
	arb.NoTimeBoosted
	ChainId *big.Int
	From    accounts.Address

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

func (tx *ArbitrumUnsignedTx) Type() byte                         { return ArbitrumUnsignedTxType }
func (tx *ArbitrumUnsignedTx) GetChainID() *uint256.Int           { return uint256.MustFromBig(tx.ChainId) }
func (tx *ArbitrumUnsignedTx) GetNonce() uint64                   { return tx.Nonce }
func (tx *ArbitrumUnsignedTx) GetPrice() *uint256.Int             { return uint256.MustFromBig(tx.GasFeeCap) }
func (tx *ArbitrumUnsignedTx) GetTipCap() *uint256.Int            { return uintZero }
func (tx *ArbitrumUnsignedTx) GetBlobHashes() []common.Hash       { return []common.Hash{} }
func (tx *ArbitrumUnsignedTx) GetGasLimit() uint64                { return tx.Gas }
func (tx *ArbitrumUnsignedTx) GetBlobGas() uint64                 { return 0 }
func (tx *ArbitrumUnsignedTx) GetValue() *uint256.Int             { return uint256.MustFromBig(tx.Value) }
func (tx *ArbitrumUnsignedTx) GetTo() *common.Address             { return tx.To }
func (tx *ArbitrumUnsignedTx) GetData() []byte                    { return tx.Data }
func (tx *ArbitrumUnsignedTx) GetAccessList() AccessList          { return nil }
func (tx *ArbitrumUnsignedTx) GetFeeCap() *uint256.Int            { return uint256.MustFromBig(tx.GasFeeCap) }
func (tx *ArbitrumUnsignedTx) GetAuthorizations() []Authorization { return nil }

func (tx *ArbitrumUnsignedTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetPrice()
	}
	res := uint256.NewInt(0)
	return res.Set(baseFee)
}

func (tx *ArbitrumUnsignedTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error) {
	var to accounts.Address
	if tx.GetTo() == nil {
		to = accounts.InternAddress(*tx.GetTo())
	} else {
		to = accounts.NilAddress
	}
	msg := &Message{
		gasPrice:   *tx.GetPrice(),
		tipCap:     *tx.GetTipCap(),
		feeCap:     *tx.GetFeeCap(),
		gasLimit:   tx.GetGasLimit(),
		nonce:      tx.GetNonce(),
		accessList: tx.GetAccessList(),
		from:       tx.From,
		to:         to,
		data:       tx.GetData(),
		amount:     *tx.GetValue(),
		checkNonce: !skipAccountChecks[tx.Type()],

		// TxRunMode: MessageRunMode, // must be set separately?
		Tx:           tx,
		TxRunContext: NewMessageCommitContext([]wasmdb.WasmTarget{wasmdb.LocalTarget()}),
	}
	// if baseFee != nil {
	// 	msg.gasPrice.SetFromBig(math.BigMin(msg.gasPrice.ToBig().Add(msg.tip.ToBig(), baseFee), msg.feeCap.ToBig()))
	// }

	return msg, nil
}

func (tx *ArbitrumUnsignedTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) Hash() common.Hash {
	//TODO implement me
	return prefixedRlpHash(ArbitrumUnsignedTxType, []interface{}{
		tx.ChainId,
		tx.From.Value(),
		tx.Nonce,
		tx.GasFeeCap,
		tx.Gas,
		tx.To,
		tx.Value,
		tx.Data,
	})
}

func (tx *ArbitrumUnsignedTx) SigningHash(chainID *big.Int) common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) Protected() bool {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return uintZero, uintZero, uintZero
}

func (tx *ArbitrumUnsignedTx) payloadSize() (payloadSize int, nonceLen, gasLen int) {
	// ChainId
	payloadSize += rlp.BigIntLen(tx.ChainId)

	// Nonce
	nonceLen = rlp.U64Len(tx.Nonce)
	payloadSize += nonceLen

	// size of From (20 bytes)
	payloadSize++
	payloadSize += length.Addr

	// GasFeeCap
	payloadSize += rlp.BigIntLen(tx.GasFeeCap)

	// Gas
	gasLen = rlp.U64Len(tx.Gas)
	payloadSize += gasLen

	// To (20 bytes if non-nil)
	payloadSize++
	if tx.To != nil {
		payloadSize += 20
	}

	// Value
	payloadSize += rlp.BigIntLen(tx.Value)

	// Data (includes its own header)
	payloadSize += rlp.StringLen(tx.Data)

	return payloadSize, nonceLen, gasLen
}

func (tx *ArbitrumUnsignedTx) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen int) error {
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeBigInt(tx.ChainId, w, b); err != nil {
		return err
	}

	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	addrFrom := tx.From.Value()
	if _, err := w.Write(addrFrom[:]); err != nil {
		return err
	}

	if err := rlp.EncodeInt(tx.Nonce, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeBigInt(tx.GasFeeCap, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeInt(tx.Gas, w, b); err != nil {
		return err
	}

	if tx.To == nil {
		b[0] = 128
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		b[0] = 128 + 20
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write(tx.To[:]); err != nil {
			return err
		}
	}

	if err := rlp.EncodeBigInt(tx.Value, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeString(tx.Data, w, b); err != nil {
		return err
	}

	return nil
}

func (tx *ArbitrumUnsignedTx) EncodingSize() int {
	payloadSize, _, _ := tx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (tx *ArbitrumUnsignedTx) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen := tx.payloadSize()

	// size of struct prefix and TxType
	envelopeSize := 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)
	// envelope
	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}

	// encode TxType
	b[0] = ArbitrumUnsignedTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumUnsignedTx) DecodeRLP(s *rlp.Stream) error {
	// Begin decoding the RLP list.
	if _, err := s.List(); err != nil {
		return err
	}

	var b []byte
	var err error

	// Decode ChainId (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read ChainId: %w", err)
	}
	tx.ChainId = new(big.Int).SetBytes(b)

	// Decode From (common.Address, 20 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read From: %w", err)
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for From: %d", len(b))
	}
	addr := common.Address{}
	copy(addr[:], b)
	tx.From = accounts.InternAddress(addr)

	// Decode Nonce (uint64)
	if tx.Nonce, err = s.Uint(); err != nil {
		return fmt.Errorf("read Nonce: %w", err)
	}

	// Decode GasFeeCap (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read GasFeeCap: %w", err)
	}
	tx.GasFeeCap = new(big.Int).SetBytes(b)

	// Decode Gas (uint64)
	if tx.Gas, err = s.Uint(); err != nil {
		return fmt.Errorf("read Gas: %w", err)
	}

	// Decode To (*common.Address, 20 bytes if non-nil)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read To: %w", err)
	}
	if len(b) > 0 {
		if len(b) != 20 {
			return fmt.Errorf("wrong size for To: %d", len(b))
		}
		tx.To = new(common.Address)
		copy(tx.To[:], b)
	} else {
		tx.To = nil
	}

	// Decode Value (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	tx.Value = new(big.Int).SetBytes(b)

	// Decode Data ([]byte)
	if tx.Data, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Data: %w", err)
	}

	// End the RLP list.
	if err := s.ListEnd(); err != nil {
		return fmt.Errorf("close ArbitrumUnsignedTx: %w", err)
	}
	return nil
}

func (tx *ArbitrumUnsignedTx) MarshalBinary(w io.Writer) error {
	payloadSize, nonceLen, gasLen := tx.payloadSize()
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)
	// encode TxType
	b[0] = ArbitrumUnsignedTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumUnsignedTx) Sender(signer Signer) (accounts.Address, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumUnsignedTx) CachedSender() (accounts.Address, bool) {
	return tx.From, true
}

func (tx *ArbitrumUnsignedTx) GetSender() (accounts.Address, bool) {
	return tx.From, true
}

func (tx *ArbitrumUnsignedTx) SetSender(address accounts.Address) {
	tx.From = address
}

func (tx *ArbitrumUnsignedTx) IsContractDeploy() bool {
	return tx.To == nil
}

func (tx *ArbitrumUnsignedTx) Unwrap() Transaction {
	//TODO implement me
	panic("implement me")
}

// func (tx *ArbitrumUnsignedTx) gas() uint64         {  }
// func (tx *ArbitrumUnsignedTx) gasPrice() *big.Int  { return tx.GasFeeCap }
// func (tx *ArbitrumUnsignedTx) gasTipCap() *big.Int { return bigZero }
// func (tx *ArbitrumUnsignedTx) gasFeeCap() *big.Int { return tx.GasFeeCap }
// func (tx *ArbitrumUnsignedTx) value() *big.Int     { return tx.Value }
// func (tx *ArbitrumUnsignedTx) nonce() uint64       {  }
// func (tx *ArbitrumUnsignedTx) to() *common.Address { return tx.To }

func (tx *ArbitrumUnsignedTx) setSignatureValues(chainID, v, r, s *big.Int) {}

//func (tx *ArbitrumUnsignedTx) effectiveGasPrice(dst *big.Int, baseFee *big.Int) *big.Int {
//	if baseFee == nil {
//		return dst.Set(tx.GasFeeCap)
//	}
//	return dst.Set(baseFee)
//}

type ArbitrumContractTx struct {
	arb.NoTimeBoosted
	ChainId   *big.Int
	RequestId common.Hash
	From      accounts.Address

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
func (tx *ArbitrumContractTx) Type() byte                         { return ArbitrumContractTxType }
func (tx *ArbitrumContractTx) GetChainID() *uint256.Int           { return uint256.MustFromBig(tx.ChainId) }
func (tx *ArbitrumContractTx) GetNonce() uint64                   { return 0 }
func (tx *ArbitrumContractTx) GetPrice() *uint256.Int             { return uint256.MustFromBig(tx.GasFeeCap) }
func (tx *ArbitrumContractTx) GetTipCap() *uint256.Int            { return uintZero }
func (tx *ArbitrumContractTx) GetFeeCap() *uint256.Int            { return uint256.MustFromBig(tx.GasFeeCap) }
func (tx *ArbitrumContractTx) GetBlobHashes() []common.Hash       { return []common.Hash{} }
func (tx *ArbitrumContractTx) GetGasLimit() uint64                { return tx.Gas }
func (tx *ArbitrumContractTx) GetBlobGas() uint64                 { return 0 }
func (tx *ArbitrumContractTx) GetData() []byte                    { return tx.Data }
func (tx *ArbitrumContractTx) GetValue() *uint256.Int             { return uint256.MustFromBig(tx.Value) }
func (tx *ArbitrumContractTx) GetTo() *common.Address             { return tx.To }
func (tx *ArbitrumContractTx) GetAccessList() AccessList          { return nil }
func (tx *ArbitrumContractTx) GetAuthorizations() []Authorization { return nil }

func (tx *ArbitrumContractTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetPrice()
	}
	res := uint256.NewInt(0)
	return res.Set(baseFee)
}
func (tx *ArbitrumContractTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return uintZero, uintZero, uintZero
}

func (tx *ArbitrumContractTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error) {
	var to accounts.Address
	if tx.To != nil {
		to = accounts.InternAddress(*tx.To)
	} else {
		to = accounts.NilAddress
	}

	msg := &Message{
		gasPrice:   *tx.GetPrice(),
		tipCap:     *tx.GetTipCap(),
		feeCap:     *tx.GetFeeCap(),
		gasLimit:   tx.GetGasLimit(),
		nonce:      tx.GetNonce(),
		accessList: tx.GetAccessList(),
		from:       tx.From,
		to:         to,
		data:       tx.GetData(),
		amount:     *tx.GetValue(),
		checkNonce: !skipAccountChecks[tx.Type()],

		Tx:           tx,
		TxRunContext: NewMessageCommitContext([]wasmdb.WasmTarget{wasmdb.LocalTarget()}),
	}
	if baseFee != nil {
		msg.gasPrice.SetFromBig(math.BigMin(msg.gasPrice.ToBig().Add(msg.tipCap.ToBig(), baseFee), msg.feeCap.ToBig()))
	}
	return msg, nil
}

func (tx *ArbitrumContractTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) Hash() common.Hash {
	//TODO implement me
	return prefixedRlpHash(ArbitrumContractTxType, []interface{}{
		tx.ChainId,
		tx.RequestId,
		tx.From.Value(),
		tx.GasFeeCap,
		tx.Gas,
		tx.To,
		tx.Value,
		tx.Data,
	})
}

func (tx *ArbitrumContractTx) SigningHash(chainID *big.Int) common.Hash {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) Protected() bool {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumContractTx) payloadSize() (payloadSize int, gasLen int) {
	// 1. ChainId (big.Int): 1 header byte + content length.
	payloadSize += rlp.BigIntLen(tx.ChainId)

	// 2. RequestId (common.Hash, fixed 32 bytes): header + 32 bytes.
	payloadSize++ // header for RequestId
	payloadSize += 32

	// 3. From (common.Address, fixed 20 bytes): header + 20 bytes.
	payloadSize++ // header for From
	payloadSize += 20

	// 4. GasFeeCap (big.Int): header + content length.
	payloadSize += rlp.BigIntLen(tx.GasFeeCap)

	// 5. Gas (uint64): header + computed length.
	gasLen = rlp.U64Len(tx.Gas)
	payloadSize += gasLen

	// 6. To (*common.Address): header always; if non-nil then add 20 bytes.
	payloadSize++ // header for To
	if tx.To != nil {
		payloadSize += 20
	}

	// 7. Value (big.Int): header + content length.
	payloadSize += rlp.BigIntLen(tx.Value)

	// 8. Data ([]byte): rlp.StringLen returns full encoded length (header + data).
	payloadSize += rlp.StringLen(tx.Data)

	return payloadSize, gasLen
}

func (tx *ArbitrumContractTx) encodePayload(w io.Writer, b []byte, payloadSize, gasLen int) error {
	// Write the RLP list prefix for the payload.
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	// 1. ChainId (big.Int)
	if err := rlp.EncodeBigInt(tx.ChainId, w, b); err != nil {
		return err
	}

	// 2. RequestId (common.Hash, 32 bytes)
	// Write header for fixed length 32: 0x80 + 32.
	b[0] = 128 + 32
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(tx.RequestId[:]); err != nil {
		return err
	}

	// 3. From (common.Address, 20 bytes)
	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	fromAddr := tx.From.Value()
	if _, err := w.Write(fromAddr[:]); err != nil {
		return err
	}

	// 4. GasFeeCap (big.Int)
	if err := rlp.EncodeBigInt(tx.GasFeeCap, w, b); err != nil {
		return err
	}

	// 5. Gas (uint64)
	if err := rlp.EncodeInt(tx.Gas, w, b); err != nil {
		return err
	}

	// 6. To (*common.Address)
	if tx.To == nil {
		// nil is encoded as an empty byte string.
		b[0] = 128
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		// Write header for 20-byte string and then the address bytes.
		b[0] = 128 + 20
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write((*tx.To)[:]); err != nil {
			return err
		}
	}

	// 7. Value (big.Int)
	if err := rlp.EncodeBigInt(tx.Value, w, b); err != nil {
		return err
	}

	// 8. Data ([]byte)
	if err := rlp.EncodeString(tx.Data, w, b); err != nil {
		return err
	}

	return nil
}

func (tx *ArbitrumContractTx) EncodingSize() int {
	payloadSize, _ := tx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (tx *ArbitrumContractTx) EncodeRLP(w io.Writer) error {
	payloadSize, gasLen := tx.payloadSize()

	// size of struct prefix and TxType
	envelopeSize := 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)
	// envelope
	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}

	// encode TxType
	b[0] = ArbitrumContractTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, gasLen); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumContractTx) DecodeRLP(s *rlp.Stream) error {
	// Begin decoding the RLP list.
	if _, err := s.List(); err != nil {
		return err
	}

	var b []byte
	var err error

	// Decode ChainId (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read ChainId: %w", err)
	}
	tx.ChainId = new(big.Int).SetBytes(b)

	// Decode RequestId (common.Hash, 32 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read RequestId: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for RequestId: %d", len(b))
	}
	copy(tx.RequestId[:], b)

	// Decode From (common.Address, 20 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read From: %w", err)
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for From: %d", len(b))
	}
	fromAddr := common.Address{}
	copy(fromAddr[:], b)
	tx.From = accounts.InternAddress(fromAddr)

	// Decode GasFeeCap (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read GasFeeCap: %w", err)
	}
	tx.GasFeeCap = new(big.Int).SetBytes(b)

	// Decode Gas (uint64)
	if tx.Gas, err = s.Uint(); err != nil {
		return fmt.Errorf("read Gas: %w", err)
	}

	// Decode To (*common.Address, 20 bytes if non-nil)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read To: %w", err)
	}
	if len(b) > 0 {
		if len(b) != 20 {
			return fmt.Errorf("wrong size for To: %d", len(b))
		}
		addrValue := common.Address{}
		copy(addrValue[:], b)
		tx.To = &addrValue
	} else {
		tx.To = nil
	}

	// Decode Value (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	tx.Value = new(big.Int).SetBytes(b)

	// Decode Data ([]byte)
	if tx.Data, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Data: %w", err)
	}

	// End the RLP list.
	if err := s.ListEnd(); err != nil {
		return fmt.Errorf("close ArbitrumContractTx: %w", err)
	}
	return nil
}

func (tx *ArbitrumContractTx) MarshalBinary(w io.Writer) error {
	payloadSize, gasLen := tx.payloadSize()
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)
	// encode TxType
	b[0] = ArbitrumContractTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, gasLen); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumContractTx) Sender(signer Signer) (accounts.Address, error) {
	panic("implement me")
}

func (tx *ArbitrumContractTx) CachedSender() (accounts.Address, bool) {
	return tx.From, true
}

func (tx *ArbitrumContractTx) GetSender() (accounts.Address, bool) {
	return tx.From, true
}

func (tx *ArbitrumContractTx) SetSender(address accounts.Address) {
	tx.From = address
}

func (tx *ArbitrumContractTx) IsContractDeploy() bool {
	return tx.To == nil
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
	ChainId             *big.Int
	Nonce               uint64
	From                accounts.Address
	GasFeeCap           *big.Int        // wei per gas
	Gas                 uint64          // gas limit
	To                  *common.Address `rlp:"nil"` // nil means contract creation
	Value               *big.Int        // wei amount
	Data                []byte          // contract invocation input data
	TicketId            common.Hash
	RefundTo            accounts.Address
	MaxRefund           *big.Int // the maximum refund sent to RefundTo (the rest goes to From)
	SubmissionFeeRefund *big.Int // the submission fee to refund if successful (capped by MaxRefund)
	Timeboosted         *bool
}

func (t *ArbitrumRetryTx) copy() *ArbitrumRetryTx {
	cpy := &ArbitrumRetryTx{
		ChainId:             new(big.Int),
		Nonce:               t.Nonce,
		GasFeeCap:           new(big.Int),
		Gas:                 t.Gas,
		From:                t.From,
		To:                  nil,
		Value:               new(big.Int),
		Data:                common.Copy(t.Data),
		TicketId:            t.TicketId,
		RefundTo:            t.RefundTo,
		MaxRefund:           new(big.Int),
		SubmissionFeeRefund: new(big.Int),
		Timeboosted:         t.Timeboosted,
	}
	if t.ChainId != nil {
		cpy.ChainId.Set(t.ChainId)
	}
	if t.GasFeeCap != nil {
		cpy.GasFeeCap.Set(t.GasFeeCap)
	}
	if t.To != nil {
		tmp := *t.To
		cpy.To = &tmp
	}
	if t.Value != nil {
		cpy.Value.Set(t.Value)
	}
	if t.MaxRefund != nil {
		cpy.MaxRefund.Set(t.MaxRefund)
	}
	if t.SubmissionFeeRefund != nil {
		cpy.SubmissionFeeRefund.Set(t.SubmissionFeeRefund)
	}
	return cpy
}

func (t *ArbitrumRetryTx) Type() byte                          { return ArbitrumRetryTxType }
func (t *ArbitrumRetryTx) GetChainID() *uint256.Int            { return uint256.MustFromBig(t.ChainId) }
func (t *ArbitrumRetryTx) GetNonce() uint64                    { return t.Nonce }
func (t *ArbitrumRetryTx) GetPrice() *uint256.Int              { return uint256.MustFromBig(t.GasFeeCap) }
func (t *ArbitrumRetryTx) GetTipCap() *uint256.Int             { return uintZero }
func (t *ArbitrumRetryTx) GetFeeCap() *uint256.Int             { return uint256.MustFromBig(t.GasFeeCap) }
func (t *ArbitrumRetryTx) GetBlobHashes() []common.Hash        { return []common.Hash{} }
func (t *ArbitrumRetryTx) GetGasLimit() uint64                 { return t.Gas }
func (t *ArbitrumRetryTx) GetBlobGas() uint64                  { return 0 }
func (t *ArbitrumRetryTx) GetData() []byte                     { return t.Data }
func (t *ArbitrumRetryTx) GetValue() *uint256.Int              { return uint256.MustFromBig(t.Value) }
func (t *ArbitrumRetryTx) GetTo() *common.Address              { return t.To }
func (t *ArbitrumRetryTx) GetAccessList() AccessList           { return nil }
func (tx *ArbitrumRetryTx) GetAuthorizations() []Authorization { return nil }

func (t *ArbitrumRetryTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return t.GetPrice()
	}
	res := uint256.NewInt(0)
	return res.Set(baseFee)
}
func (t *ArbitrumRetryTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return uintZero, uintZero, uintZero
}

func (t *ArbitrumRetryTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error) {
	var to accounts.Address
	if t.To != nil {
		to = accounts.InternAddress(*t.To)
	} else {
		to = accounts.NilAddress
	}
	msg := &Message{
		gasPrice:   *t.GetPrice(),
		tipCap:     *t.GetTipCap(),
		feeCap:     *t.GetFeeCap(),
		gasLimit:   t.GetGasLimit(),
		nonce:      t.GetNonce(),
		accessList: t.GetAccessList(),
		from:       t.From,
		to:         to,
		data:       t.GetData(),
		amount:     *t.GetValue(),
		checkNonce: !skipAccountChecks[t.Type()],

		Tx:           t,
		TxRunContext: NewMessageCommitContext([]wasmdb.WasmTarget{wasmdb.LocalTarget()}),
	}
	if baseFee != nil {
		msg.gasPrice.SetFromBig(math.BigMin(msg.gasPrice.ToBig().Add(msg.tipCap.ToBig(), baseFee), msg.feeCap.ToBig()))
	}
	return msg, nil
}

func (t *ArbitrumRetryTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	//TODO implement me
	panic("implement me")
}

func (t *ArbitrumRetryTx) Hash() common.Hash {
	//TODO implement me
	return prefixedRlpHash(ArbitrumRetryTxType, []interface{}{
		t.ChainId,
		t.Nonce,
		t.From.Value(),
		t.GasFeeCap,
		t.Gas,
		t.To,
		t.Value,
		t.Data,
		t.TicketId,
		t.RefundTo.Value(),
		t.MaxRefund,
		t.SubmissionFeeRefund,
	})
}

func (t *ArbitrumRetryTx) SigningHash(chainID *big.Int) common.Hash {
	//TODO implement me
	panic("implement me")
}

func (t *ArbitrumRetryTx) Protected() bool {
	//TODO implement me
	panic("implement me")
}

func (t *ArbitrumRetryTx) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen int, hashingOnly bool) error {
	// Write the RLP list prefix.
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	// ChainId (big.Int)
	if err := rlp.EncodeBigInt(t.ChainId, w, b); err != nil {
		return err
	}

	// Nonce (uint64)
	if err := rlp.EncodeInt(t.Nonce, w, b); err != nil {
		return err
	}

	// From (common.Address, 20 bytes)
	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	fromAddr := t.From.Value()
	if _, err := w.Write(fromAddr[:]); err != nil {
		return err
	}

	// GasFeeCap (big.Int)
	if err := rlp.EncodeBigInt(t.GasFeeCap, w, b); err != nil {
		return err
	}

	// Gas (uint64)
	if err := rlp.EncodeInt(t.Gas, w, b); err != nil {
		return err
	}

	// To (optional common.Address, 20 bytes if non-nil)
	if t.To == nil {
		b[0] = 128
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		b[0] = 128 + 20
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write((*t.To)[:]); err != nil {
			return err
		}
	}

	// Value (big.Int)
	if err := rlp.EncodeBigInt(t.Value, w, b); err != nil {
		return err
	}

	// Data ([]byte)
	if err := rlp.EncodeString(t.Data, w, b); err != nil {
		return err
	}

	// TicketId (common.Hash, 32 bytes)
	b[0] = 128 + 32
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(t.TicketId[:]); err != nil {
		return err
	}

	// RefundTo (common.Address, 20 bytes)
	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(t.RefundTo.Value().Bytes()); err != nil {
		return err
	}

	// MaxRefund (big.Int)
	if err := rlp.EncodeBigInt(t.MaxRefund, w, b); err != nil {
		return err
	}

	// SubmissionFeeRefund (big.Int)
	if err := rlp.EncodeBigInt(t.SubmissionFeeRefund, w, b); err != nil {
		return err
	}

	if t.Timeboosted != nil && !hashingOnly {
		if err := rlp.EncodeBool(*t.Timeboosted, w, b); err != nil {
			return err
		}
	}

	return nil
}

func (t *ArbitrumRetryTx) payloadSize(hashingOnly bool) (payloadSize int, nonceLen, gasLen int) {
	// ChainId (big.Int)
	payloadSize += rlp.BigIntLen(t.ChainId)

	// Nonce (uint64)
	nonceLen = rlp.U64Len(t.Nonce)
	payloadSize += nonceLen

	// From (common.Address, 20 bytes)
	payloadSize++ // header
	payloadSize += 20

	// GasFeeCap (big.Int)
	payloadSize += rlp.BigIntLen(t.GasFeeCap)

	// Gas (uint64)
	gasLen = rlp.U64Len(t.Gas)
	payloadSize += gasLen

	// To (optional common.Address, 20 bytes if non-nil)
	payloadSize++ // header
	if t.To != nil {
		payloadSize += 20
	}

	// Value (big.Int)
	payloadSize += rlp.BigIntLen(t.Value)

	// Data ([]byte) — rlp.StringLen returns the full encoded length (header + data)
	payloadSize += rlp.StringLen(t.Data)

	// TicketId (common.Hash, 32 bytes)
	payloadSize++ // header
	payloadSize += 32

	// RefundTo (common.Address, 20 bytes)
	payloadSize++ // header
	payloadSize += 20

	// MaxRefund (big.Int)
	payloadSize += rlp.BigIntLen(t.MaxRefund)

	// SubmissionFeeRefund (big.Int)
	payloadSize += rlp.BigIntLen(t.SubmissionFeeRefund)

	if t.Timeboosted != nil && !hashingOnly {
		// Timeboosted (bool)
		payloadSize++
		payloadSize += rlp.BoolLen()
	}

	return payloadSize, nonceLen, gasLen
}

func (t *ArbitrumRetryTx) EncodingSize() int {
	payloadSize, _, _ := t.payloadSize(false)
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (t *ArbitrumRetryTx) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen := t.payloadSize(false)

	// size of struct prefix and TxType
	envelopeSize := 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)
	// envelope
	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}

	// encode TxType
	b[0] = ArbitrumRetryTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := t.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, false); err != nil {
		return err
	}
	return nil
}

func (t *ArbitrumRetryTx) DecodeRLP(s *rlp.Stream) error {
	// Begin list decoding.
	if _, err := s.List(); err != nil {
		return err
	}

	var b []byte
	var err error

	// Decode ChainId (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read ChainId: %w", err)
	}
	t.ChainId = new(big.Int).SetBytes(b)

	// Decode Nonce (uint64)
	if t.Nonce, err = s.Uint(); err != nil {
		return fmt.Errorf("read Nonce: %w", err)
	}

	// Decode From (common.Address, 20 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read From: %w", err)
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for From: %d", len(b))
	}
	fromAddr := common.Address{}
	copy(fromAddr[:], b)

	t.From = accounts.InternAddress(fromAddr)

	// Decode GasFeeCap (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read GasFeeCap: %w", err)
	}
	t.GasFeeCap = new(big.Int).SetBytes(b)

	// Decode Gas (uint64)
	if t.Gas, err = s.Uint(); err != nil {
		return fmt.Errorf("read Gas: %w", err)
	}

	// Decode To (*common.Address, 20 bytes if non-nil)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read To: %w", err)
	}
	if len(b) > 0 {
		if len(b) != 20 {
			return fmt.Errorf("wrong size for To: %d", len(b))
		}
		t.To = new(common.Address)
		copy(t.To[:], b)
	}

	// Decode Value (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	t.Value = new(big.Int).SetBytes(b)

	// Decode Data ([]byte)
	if t.Data, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Data: %w", err)
	}

	// Decode TicketId (common.Hash, 32 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read TicketId: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for TicketId: %d", len(b))
	}
	copy(t.TicketId[:], b)

	// Decode RefundTo (common.Address, 20 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read RefundTo: %w", err)
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for RefundTo: %d", len(b))
	}
	addr := common.Address{}
	copy(addr[:], b)
	t.RefundTo = accounts.InternAddress(addr)

	// Decode MaxRefund (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read MaxRefund: %w", err)
	}
	t.MaxRefund = new(big.Int).SetBytes(b)

	// Decode SubmissionFeeRefund (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read SubmissionFeeRefund: %w", err)
	}
	t.SubmissionFeeRefund = new(big.Int).SetBytes(b)

	if s.MoreDataInList() {
		boolVal, err := s.Bool()
		if err != nil {
			return err
		}
		t.Timeboosted = &boolVal
	}
	return s.ListEnd()
}

func (t *ArbitrumRetryTx) MarshalBinary(w io.Writer) error {
	payloadSize, nonceLen, gasLen := t.payloadSize(false)
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)
	// encode TxType
	b[0] = ArbitrumRetryTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := t.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, false); err != nil {
		return err
	}
	return nil
}

func (t *ArbitrumRetryTx) MarshalBinaryForHashing(w io.Writer) error {
	payloadSize, nonceLen, gasLen := t.payloadSize(true)
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)
	// encode TxType
	b[0] = ArbitrumRetryTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := t.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, true); err != nil {
		return err
	}
	return nil
}

func (t *ArbitrumRetryTx) Sender(signer Signer) (accounts.Address, error) {
	//TODO implement me
	panic("implement me")
}

func (t *ArbitrumRetryTx) CachedSender() (accounts.Address, bool) {
	//TODO implement me
	panic("implement me")
}

func (t *ArbitrumRetryTx) GetSender() (accounts.Address, bool) {
	return t.From, true
}

func (t *ArbitrumRetryTx) SetSender(address accounts.Address) {
	t.From = address
}

func (t *ArbitrumRetryTx) IsContractDeploy() bool {
	return t.To == nil
}

func (t *ArbitrumRetryTx) Unwrap() Transaction {
	return t
}

func (t *ArbitrumRetryTx) IsTimeBoosted() *bool {
	return t.Timeboosted
}

func (t *ArbitrumRetryTx) SetTimeboosted(val *bool) {
	t.Timeboosted = val
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
func (t *ArbitrumRetryTx) encode(b *bytes.Buffer) error {
	return rlp.Encode(b, t)
}
func (t *ArbitrumRetryTx) decode(input []byte) error {
	return rlp.DecodeBytes(input, t)
}

func (t *ArbitrumRetryTx) setSignatureValues(chainID, v, r, s *big.Int) {}

//func (tx *ArbitrumRetryTx) effectiveGasPrice(dst *big.Int, baseFee *big.Int) *big.Int {
//	if baseFee == nil {
//		return dst.Set(tx.GasFeeCap)
//	}
//	return dst.Set(baseFee)
//}

type ArbitrumSubmitRetryableTx struct {
	arb.NoTimeBoosted
	ChainId   *big.Int
	RequestId common.Hash
	From      accounts.Address
	L1BaseFee *big.Int

	DepositValue     *big.Int
	GasFeeCap        *big.Int        // wei per gas
	Gas              uint64          // gas limit for the retryable tx, actual gas spending is EffectiveGasUsed
	RetryTo          *common.Address `rlp:"nil"` // nil means contract creation
	RetryValue       *big.Int        // wei amount
	Beneficiary      accounts.Address
	MaxSubmissionFee *big.Int
	FeeRefundAddr    accounts.Address
	RetryData        []byte // contract invocation input data
	EffectiveGasUsed uint64
}

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
		RetryData:        common.Copy(tx.RetryData),
		EffectiveGasUsed: tx.EffectiveGasUsed,
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

func (tx *ArbitrumSubmitRetryableTx) Type() byte                   { return ArbitrumSubmitRetryableTxType }
func (tx *ArbitrumSubmitRetryableTx) GetBlobHashes() []common.Hash { return []common.Hash{} }
func (tx *ArbitrumSubmitRetryableTx) GetGasLimit() uint64          { return tx.Gas }
func (tx *ArbitrumSubmitRetryableTx) GetBlobGas() uint64           { return 0 }
func (tx *ArbitrumSubmitRetryableTx) GetNonce() uint64             { return 0 }
func (tx *ArbitrumSubmitRetryableTx) GetTipCap() *uint256.Int      { return uintZero }
func (tx *ArbitrumSubmitRetryableTx) GetValue() *uint256.Int       { return uintZero }
func (tx *ArbitrumSubmitRetryableTx) GetTo() *common.Address {
	a := ArbRetryableTxAddress.Value()
	return &a
}
func (tx *ArbitrumSubmitRetryableTx) GetAccessList() AccessList          { return nil }
func (tx *ArbitrumSubmitRetryableTx) GetAuthorizations() []Authorization { return nil }
func (tx *ArbitrumSubmitRetryableTx) GetChainID() *uint256.Int {
	return uint256.MustFromBig(tx.ChainId)
}
func (tx *ArbitrumSubmitRetryableTx) GetPrice() *uint256.Int {
	return uint256.MustFromBig(tx.GasFeeCap)
}
func (tx *ArbitrumSubmitRetryableTx) GetFeeCap() *uint256.Int {
	return uint256.MustFromBig(tx.GasFeeCap)
}

func (tx *ArbitrumSubmitRetryableTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetPrice()
	}
	res := uint256.NewInt(0)
	return res.Set(baseFee)
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
	data = append(data, tx.FeeRefundAddr.Value().Bytes()...)
	data = append(data, make([]byte, 12)...)
	data = append(data, tx.Beneficiary.Value().Bytes()...)
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

func (tx *ArbitrumSubmitRetryableTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return uintZero, uintZero, uintZero
}

func (tx *ArbitrumSubmitRetryableTx) payloadSize(hashingOnly bool) (payloadSize int, gasLen int) {
	size := 0
	size += rlp.BigIntLen(tx.ChainId)

	size++
	size += length.Hash

	size++
	size += length.Addr

	size += rlp.BigIntLen(tx.L1BaseFee)

	size += rlp.BigIntLen(tx.DepositValue)

	size += rlp.BigIntLen(tx.GasFeeCap)

	gasLen = rlp.U64Len(tx.Gas)
	size += gasLen

	size++
	if tx.RetryTo != nil {
		size += length.Addr
	}

	size += rlp.BigIntLen(tx.RetryValue)

	size++
	size += length.Addr

	size += rlp.BigIntLen(tx.MaxSubmissionFee)

	size++
	size += length.Addr

	size += rlp.StringLen(tx.RetryData)

	if hashingOnly {
		return size, gasLen
	}
	// effective gas used is only included in non-hashing RLP encodings
	size += rlp.U64Len(tx.EffectiveGasUsed)

	return size, gasLen
}

func (tx *ArbitrumSubmitRetryableTx) encodePayload(w io.Writer, b []byte, payloadSize int, hashingOnly bool) error {
	// Write the RLP list prefix.
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	// ChainId (big.Int)
	if err := rlp.EncodeBigInt(tx.ChainId, w, b); err != nil {
		return err
	}

	// RequestId (common.Hash, 32 bytes)
	b[0] = 128 + length.Hash
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(tx.RequestId[:]); err != nil {
		return err
	}

	// From (common.Address, 20 bytes)
	b[0] = 128 + length.Addr
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	fromAddr := tx.From.Value()
	if _, err := w.Write(fromAddr[:]); err != nil {
		return err
	}

	if err := rlp.EncodeBigInt(tx.L1BaseFee, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeBigInt(tx.DepositValue, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeBigInt(tx.GasFeeCap, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeInt(tx.Gas, w, b); err != nil {
		return err
	}

	// RetryTo (pointer to common.Address, 20 bytes if non-nil; otherwise RLP nil)
	if tx.RetryTo == nil {
		b[0] = 128
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		b[0] = 128 + length.Addr
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write((*tx.RetryTo)[:]); err != nil {
			return err
		}
	}
	if err := rlp.EncodeBigInt(tx.RetryValue, w, b); err != nil {
		return err
	}
	// Beneficiary (common.Address, 20 bytes)
	b[0] = 128 + length.Addr
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(tx.Beneficiary.Value().Bytes()); err != nil {
		return err
	}
	if err := rlp.EncodeBigInt(tx.MaxSubmissionFee, w, b); err != nil {
		return err
	}

	// FeeRefundAddr (common.Address, 20 bytes)
	b[0] = 128 + length.Addr
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(tx.FeeRefundAddr.Value().Bytes()); err != nil {
		return err
	}
	if err := rlp.EncodeString(tx.RetryData, w, b); err != nil {
		return err
	}

	if bytes.Equal(tx.Hash().Bytes()[:], TxHashhh[:]) {
		fmt.Printf("marshal %x hashingOnly=%t tx: %+v\n", TxHashhh, hashingOnly, tx)
	}

	if hashingOnly {
		return nil
	}
	if err := rlp.EncodeInt(tx.EffectiveGasUsed, w, b); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumSubmitRetryableTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error) {
	var to accounts.Address
	if tx.GetTo() != nil {
		to = accounts.InternAddress(*tx.GetTo())
	} else {
		to = accounts.NilAddress
	}
	msg := &Message{
		gasPrice:   *tx.GetPrice(),
		tipCap:     *tx.GetTipCap(),
		feeCap:     *tx.GetFeeCap(),
		gasLimit:   tx.GetGasLimit(),
		nonce:      tx.GetNonce(),
		accessList: tx.GetAccessList(),
		from:       tx.From,
		to:         to,
		data:       tx.GetData(),
		amount:     *tx.GetValue(),
		checkNonce: !skipAccountChecks[tx.Type()],

		EffectiveGas: tx.EffectiveGasUsed,
		Tx:           tx,
		TxRunContext: NewMessageCommitContext([]wasmdb.WasmTarget{wasmdb.LocalTarget()}),
	}
	if baseFee != nil {
		msg.gasPrice.SetFromBig(math.BigMin(msg.gasPrice.ToBig().Add(msg.tipCap.ToBig(), baseFee), msg.feeCap.ToBig()))
	}
	// if !rules.IsCancun {
	// 	return msg, errors.New("BlobTx transactions require Cancun")
	// }
	// if baseFee != nil {
	// 	overflow := msg.gasPrice.SetFromBig(baseFee)
	// 	if overflow {
	// 		return msg, errors.New("gasPrice higher than 2^256-1")
	// 	}
	// }
	// msg.gasPrice.Add(&msg.gasPrice, stx.Tip)
	// if msg.gasPrice.Gt(stx.FeeCap) {
	// 	msg.gasPrice.Set(stx.FeeCap)
	// }
	// var err error
	// msg.from, err = d.Sender(s)
	// msg.maxFeePerBlobGas = *stx.MaxFeePerBlobGas
	// msg.blobHashes = stx.BlobVersionedHashes
	return msg, nil
}

func (tx *ArbitrumSubmitRetryableTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumSubmitRetryableTx) Hash() common.Hash {
	return prefixedRlpHash(ArbitrumSubmitRetryableTxType, []interface{}{
		tx.ChainId,
		tx.RequestId,
		tx.From.Value(),
		tx.L1BaseFee,
		tx.DepositValue,
		tx.GasFeeCap,
		tx.Gas,
		tx.RetryTo,
		tx.RetryValue,
		tx.Beneficiary.Value(),
		tx.MaxSubmissionFee,
		tx.FeeRefundAddr.Value(),
		tx.RetryData,
	})
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
	payloadSize, _ := tx.payloadSize(false)
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (tx *ArbitrumSubmitRetryableTx) EncodeRLP(w io.Writer) error {
	hashingOnly := false

	payloadSize, _ := tx.payloadSize(hashingOnly)

	// size of struct prefix and TxType
	envelopeSize := 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)

	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}

	// encode TxType
	b[0] = ArbitrumSubmitRetryableTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, hashingOnly); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumSubmitRetryableTx) DecodeRLP(s *rlp.Stream) error {
	// Begin decoding the RLP list.
	if _, err := s.List(); err != nil {
		return err
	}

	var b []byte
	var err error

	// Decode ChainId (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read ChainId: %w", err)
	}
	tx.ChainId = new(big.Int).SetBytes(b)

	// Decode RequestId (common.Hash, 32 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read RequestId: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for RequestId: %d", len(b))
	}
	copy(tx.RequestId[:], b)

	// Decode From (common.Address, 20 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read From: %w", err)
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for From: %d", len(b))
	}
	fromAddr := common.Address{}
	copy(fromAddr[:], b)

	tx.From = accounts.InternAddress(fromAddr)

	// Decode L1BaseFee (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read L1BaseFee: %w", err)
	}
	tx.L1BaseFee = new(big.Int).SetBytes(b)

	// Decode DepositValue (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read DepositValue: %w", err)
	}
	tx.DepositValue = new(big.Int).SetBytes(b)

	// Decode GasFeeCap (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read GasFeeCap: %w", err)
	}
	tx.GasFeeCap = new(big.Int).SetBytes(b)

	// Decode Gas (uint64)
	if tx.Gas, err = s.Uint(); err != nil {
		return fmt.Errorf("read Gas: %w", err)
	}

	// Decode RetryTo (*common.Address, 20 bytes if non-nil)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read RetryTo: %w", err)
	}
	if len(b) > 0 {
		if len(b) != 20 {
			return fmt.Errorf("wrong size for RetryTo: %d", len(b))
		}
		tx.RetryTo = new(common.Address)
		copy(tx.RetryTo[:], b)
	} else {
		tx.RetryTo = nil
	}

	// Decode RetryValue (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read RetryValue: %w", err)
	}
	tx.RetryValue = new(big.Int).SetBytes(b)

	// Decode Beneficiary (common.Address, 20 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Beneficiary: %w", err)
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for Beneficiary: %d", len(b))
	}

	addr := common.Address{}
	copy(addr[:], b)
	tx.Beneficiary = accounts.InternAddress(addr)

	// Decode MaxSubmissionFee (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read MaxSubmissionFee: %w", err)
	}
	tx.MaxSubmissionFee = new(big.Int).SetBytes(b)

	// Decode FeeRefundAddr (common.Address, 20 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read FeeRefundAddr: %w", err)
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for FeeRefundAddr: %d", len(b))
	}
	copy(addr[:], b)
	tx.FeeRefundAddr = accounts.InternAddress(addr)

	// Decode RetryData ([]byte)
	if tx.RetryData, err = s.Bytes(); err != nil {
		return fmt.Errorf("read RetryData: %w", err)
	}

	if s.MoreDataInList() {
		if tx.EffectiveGasUsed, err = s.Uint(); err != nil {
			return fmt.Errorf("read EffectiveGasUSed: %w", err)
		}
	}

	// End the RLP list.
	if err := s.ListEnd(); err != nil {
		return fmt.Errorf("close ArbitrumSubmitRetryableTx: %w", err)
	}
	return nil
}

var TxHashhh = common.HexToHash("0xae75e367d4b38d413a9cc3c0ff825453913e95db0f4089fbfdccae2e77e9cf1c")

func (tx *ArbitrumSubmitRetryableTx) MarshalBinary(w io.Writer) error {
	hashingOnly := false
	payloadSize, _ := tx.payloadSize(hashingOnly)
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)
	// encode TxType
	b[0] = ArbitrumSubmitRetryableTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, hashingOnly); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumSubmitRetryableTx) MarshalBinaryForHashing(w io.Writer) error {
	hashingOnly := true

	payloadSize, _ := tx.payloadSize(hashingOnly)
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)
	// encode TxType
	b[0] = ArbitrumSubmitRetryableTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, hashingOnly); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumSubmitRetryableTx) Sender(signer Signer) (accounts.Address, error) {
	panic("cannot sign ArbitrumSubmitRetryableTx")
}

func (tx *ArbitrumSubmitRetryableTx) CachedSender() (accounts.Address, bool) {
	return tx.From, true
}

func (tx *ArbitrumSubmitRetryableTx) GetSender() (accounts.Address, bool) {
	return tx.From, true
}

func (tx *ArbitrumSubmitRetryableTx) SetSender(address accounts.Address) {
	tx.From = address
}

func (tx *ArbitrumSubmitRetryableTx) IsContractDeploy() bool {
	return tx.RetryTo == nil
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

//func (tx *ArbitrumSubmitRetryableTx) setSignatureValues(chainID, v, r, s *big.Int) {}
//
//func (tx *ArbitrumSubmitRetryableTx) effectiveGasPrice(dst *big.Int, baseFee *big.Int) *big.Int {
//	if baseFee == nil {
//		return dst.Set(tx.GasFeeCap)
//	}
//	return dst.Set(baseFee)
//}

type ArbitrumDepositTx struct {
	arb.NoTimeBoosted
	ChainId     *big.Int
	L1RequestId common.Hash
	From        accounts.Address
	To          common.Address
	Value       *big.Int
}

func (tx *ArbitrumDepositTx) copy() *ArbitrumDepositTx {
	dtx := &ArbitrumDepositTx{
		ChainId:     new(big.Int),
		L1RequestId: tx.L1RequestId,
		From:        tx.From,
		To:          tx.To,
		Value:       new(big.Int),
	}
	if dtx.ChainId != nil {
		dtx.ChainId.Set(tx.ChainId)
	}
	if dtx.Value != nil {
		dtx.Value.Set(tx.Value)
	}
	return dtx
}

func (tx *ArbitrumDepositTx) Type() byte                         { return ArbitrumDepositTxType }
func (tx *ArbitrumDepositTx) GetChainID() *uint256.Int           { return uint256.MustFromBig(tx.ChainId) }
func (tx *ArbitrumDepositTx) GetNonce() uint64                   { return 0 }
func (tx *ArbitrumDepositTx) GetPrice() *uint256.Int             { return uintZero }
func (tx *ArbitrumDepositTx) GetTipCap() *uint256.Int            { return uintZero }
func (tx *ArbitrumDepositTx) GetFeeCap() *uint256.Int            { return uintZero }
func (tx *ArbitrumDepositTx) GetBlobHashes() []common.Hash       { return []common.Hash{} }
func (tx *ArbitrumDepositTx) GetGasLimit() uint64                { return 0 }
func (tx *ArbitrumDepositTx) GetBlobGas() uint64                 { return 0 }
func (tx *ArbitrumDepositTx) GetData() []byte                    { return nil }
func (tx *ArbitrumDepositTx) GetValue() *uint256.Int             { return uint256.MustFromBig(tx.Value) }
func (tx *ArbitrumDepositTx) GetTo() *common.Address             { return &tx.To }
func (tx *ArbitrumDepositTx) GetAccessList() AccessList          { return nil }
func (tx *ArbitrumDepositTx) GetAuthorizations() []Authorization { return nil }

func (tx *ArbitrumDepositTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int { return uintZero }
func (tx *ArbitrumDepositTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return uintZero, uintZero, uintZero
}

func (tx *ArbitrumDepositTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error) {
	var to accounts.Address
	if tx.GetTo() != nil {
		to = accounts.InternAddress(*tx.GetTo())
	} else {
		to = accounts.NilAddress
	}
	msg := &Message{
		gasPrice:   *tx.GetPrice(),
		tipCap:     *tx.GetTipCap(),
		feeCap:     *tx.GetFeeCap(),
		gasLimit:   tx.GetGasLimit(),
		nonce:      tx.GetNonce(),
		accessList: tx.GetAccessList(),
		from:       tx.From,
		to:         to,
		data:       tx.GetData(),
		amount:     *tx.GetValue(),
		checkNonce: !skipAccountChecks[tx.Type()],

		Tx:           tx,
		TxRunContext: NewMessageCommitContext([]wasmdb.WasmTarget{wasmdb.LocalTarget()}),
	}
	if baseFee != nil {
		msg.gasPrice.SetFromBig(math.BigMin(msg.gasPrice.ToBig().Add(msg.tipCap.ToBig(), baseFee), msg.feeCap.ToBig()))
	}
	// if msg.feeCap.IsZero() {
	// 	msg.feeCap.Set(uint256.NewInt(0x5f5e100))
	// }
	// if !rules.IsCancun {
	// 	return msg, errors.New("BlobTx transactions require Cancun")
	// }
	// if baseFee != nil {
	// 	overflow := msg.gasPrice.SetFromBig(baseFee)
	// 	if overflow {
	// 		return msg, errors.New("gasPrice higher than 2^256-1")
	// 	}
	// }
	// msg.gasPrice.Add(&msg.gasPrice, tx.GetTipCap())
	// if msg.gasPrice.Gt(tx.GetFeeCap()) {
	// 	msg.gasPrice.Set(tx.GetFeeCap())
	// }
	// var err error
	// msg.from, err = d.Sender(s)
	// msg.maxFeePerBlobGas = *stx.MaxFeePerBlobGas
	// msg.blobHashes = stx.BlobVersionedHashes
	return msg, nil
}

func (tx *ArbitrumDepositTx) SigningHash(chainID *big.Int) common.Hash {
	//TODO implement me
	panic("implement me")
}
func (tx *ArbitrumDepositTx) Protected() bool {
	//TODO implement me
	panic("implement me")
}
func (tx *ArbitrumDepositTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *ArbitrumDepositTx) Hash() common.Hash {
	//TODO implement me
	return prefixedRlpHash(ArbitrumDepositTxType, []interface{}{
		tx.ChainId,
		tx.L1RequestId,
		tx.From.Value(),
		tx.To,
		tx.Value,
	})
}

func (tx *ArbitrumDepositTx) EncodingSize() int {
	payloadSize := tx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (tx *ArbitrumDepositTx) EncodeRLP(w io.Writer) error {
	payloadSize := tx.payloadSize()

	// size of struct prefix and TxType
	envelopeSize := 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)

	// envelope
	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}

	// encode TxType
	b[0] = ArbitrumDepositTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumDepositTx) payloadSize() int {
	size := 0

	// ChainId: header + length of big.Int (excluding header)
	size += rlp.BigIntLen(tx.ChainId)

	// L1RequestId: header + 32 bytes
	size++ // header for L1RequestId
	size += 32

	// From: header + 20 bytes
	size++ // header for From
	size += 20

	// To: header + 20 bytes
	size++ // header for To
	size += 20

	// Value: header + length of big.Int (excluding header)
	size += rlp.BigIntLen(tx.Value)

	return size
}

func (tx *ArbitrumDepositTx) encodePayload(w io.Writer, b []byte, payloadSize int) error {
	// Write the RLP list prefix.
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	// Encode ChainId.
	if err := rlp.EncodeBigInt(tx.ChainId, w, b); err != nil {
		return err
	}

	// Encode L1RequestId (common.Hash, 32 bytes).
	b[0] = 128 + 32
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(tx.L1RequestId[:]); err != nil {
		return err
	}

	// Encode From (common.Address, 20 bytes).
	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	fromAddr := tx.From.Value()
	if _, err := w.Write(fromAddr[:]); err != nil {
		return err
	}

	// Encode To (common.Address, 20 bytes).
	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(tx.To[:]); err != nil {
		return err
	}

	// Encode Value.
	if err := rlp.EncodeBigInt(tx.Value, w, b); err != nil {
		return err
	}

	return nil
}

func (tx *ArbitrumDepositTx) DecodeRLP(s *rlp.Stream) error {
	// Begin decoding the RLP list.
	if _, err := s.List(); err != nil {
		return err
	}

	var b []byte
	var err error

	// Decode ChainId (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read ChainId: %w", err)
	}
	tx.ChainId = new(big.Int).SetBytes(b)

	// Decode L1RequestId (common.Hash, 32 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read L1RequestId: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for L1RequestId: %d", len(b))
	}
	copy(tx.L1RequestId[:], b)

	// Decode From (common.Address, 20 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read From: %w", err)
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for From: %d", len(b))
	}
	from := common.Address{}
	copy(from[:], b)

	tx.From = accounts.InternAddress(from)

	// Decode To (common.Address, 20 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read To: %w", err)
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for To: %d", len(b))
	}
	copy(tx.To[:], b)

	// Decode Value (*big.Int)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	tx.Value = new(big.Int).SetBytes(b)

	// End the RLP list.
	if err := s.ListEnd(); err != nil {
		return fmt.Errorf("close ArbitrumDepositTx: %w", err)
	}
	return nil
}

func (tx *ArbitrumDepositTx) MarshalBinary(w io.Writer) error {
	payloadSize := tx.payloadSize()
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)
	// encode TxType
	b[0] = ArbitrumDepositTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumDepositTx) Sender(signer Signer) (accounts.Address, error) { panic("implement me") }
func (tx *ArbitrumDepositTx) CachedSender() (accounts.Address, bool)         { return tx.From, true }
func (tx *ArbitrumDepositTx) GetSender() (accounts.Address, bool)            { return tx.From, true }
func (tx *ArbitrumDepositTx) SetSender(address accounts.Address)             { tx.From = address }
func (tx *ArbitrumDepositTx) IsContractDeploy() bool                         { return false }
func (tx *ArbitrumDepositTx) Unwrap() Transaction                            { return tx }
func (tx *ArbitrumDepositTx) encode(b *bytes.Buffer) error                   { return rlp.Encode(b, tx) }
func (tx *ArbitrumDepositTx) decode(input []byte) error                      { return rlp.DecodeBytes(input, tx) }

//func (tx *ArbitrumDepositTx) effectiveGasPrice(dst *big.Int, baseFee *big.Int) *big.Int {
//	return dst.Set(bigZero)
//}

type ArbitrumInternalTx struct {
	ChainId *uint256.Int
	Data    []byte

	//arb.NoTimeBoosted
}

func (t *ArbitrumInternalTx) IsTimeBoosted() *bool {
	return nil
}
func (t *ArbitrumInternalTx) SetTimeboosted(b *bool) {}

func (t *ArbitrumInternalTx) copy() *ArbitrumInternalTx {
	cpy := &ArbitrumInternalTx{
		ChainId: t.ChainId.Clone(),
		Data:    common.Copy(t.Data),
	}
	return cpy
}

func (tx *ArbitrumInternalTx) Type() byte                   { return ArbitrumInternalTxType }
func (tx *ArbitrumInternalTx) GetChainID() *uint256.Int     { return tx.ChainId }
func (tx *ArbitrumInternalTx) GetNonce() uint64             { return 0 }
func (tx *ArbitrumInternalTx) GetPrice() *uint256.Int       { return uintZero }
func (tx *ArbitrumInternalTx) GetTipCap() *uint256.Int      { return uintZero }
func (tx *ArbitrumInternalTx) GetFeeCap() *uint256.Int      { return uintZero }
func (tx *ArbitrumInternalTx) GetBlobHashes() []common.Hash { return []common.Hash{} }
func (tx *ArbitrumInternalTx) GetGasLimit() uint64          { return 0 }
func (tx *ArbitrumInternalTx) GetBlobGas() uint64           { return 0 } // todo
func (tx *ArbitrumInternalTx) GetData() []byte              { return tx.Data }
func (tx *ArbitrumInternalTx) GetValue() *uint256.Int       { return uintZero }
func (tx *ArbitrumInternalTx) GetTo() *common.Address {
	addrVal := ArbosAddress.Value()
	return &addrVal
}
func (tx *ArbitrumInternalTx) GetAccessList() AccessList                      { return nil }
func (tx *ArbitrumInternalTx) GetAuthorizations() []Authorization             { return nil }
func (tx *ArbitrumInternalTx) CachedSender() (accounts.Address, bool)         { return ArbosAddress, true }
func (tx *ArbitrumInternalTx) GetSender() (accounts.Address, bool)            { return ArbosAddress, true }
func (tx *ArbitrumInternalTx) IsContractDeploy() bool                         { return false }
func (tx *ArbitrumInternalTx) Unwrap() Transaction                            { return tx }
func (tx *ArbitrumInternalTx) SigningHash(chainID *big.Int) common.Hash       { panic("implement me") }
func (tx *ArbitrumInternalTx) Protected() bool                                { panic("implement me") }
func (tx *ArbitrumInternalTx) SetSender(address accounts.Address)             {} // not supported in ArbitrumInternalTx
func (tx *ArbitrumInternalTx) Sender(signer Signer) (accounts.Address, error) { panic("not supported") }

func (tx *ArbitrumInternalTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int { return uintZero }
func (tx *ArbitrumInternalTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return uintZero, uintZero, uintZero
}
func (tx *ArbitrumInternalTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	panic("implement me")
}
func (tx *ArbitrumInternalTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error) {
	var to accounts.Address
	if tx.GetTo() != nil {
		to = accounts.InternAddress(*tx.GetTo())
	} else {
		to = accounts.NilAddress
	}

	msg := &Message{
		gasPrice:     *tx.GetPrice(),
		tipCap:       *tx.GetTipCap(),
		feeCap:       *tx.GetFeeCap(),
		gasLimit:     tx.GetGasLimit(),
		nonce:        tx.GetNonce(),
		accessList:   tx.GetAccessList(),
		from:         ArbosAddress,
		to:           to,
		data:         tx.GetData(),
		amount:       *tx.GetValue(),
		checkNonce:   !skipAccountChecks[tx.Type()],
		Tx:           tx,
		TxRunContext: NewMessageCommitContext([]wasmdb.WasmTarget{wasmdb.LocalTarget()}),
	}

	if baseFee != nil {
		msg.gasPrice.SetFromBig(math.BigMin(msg.gasPrice.ToBig().Add(msg.tipCap.ToBig(), baseFee), msg.feeCap.ToBig()))
	}
	// if msg.feeCap.IsZero() {
	// 	msg.gasLimit = baseFee.Uint64()
	// 	msg.feeCap.Set(uint256.NewInt(0x5f5e100))
	// }
	// if baseFee != nil {
	// 	overflow := msg.gasPrice.SetFromBig(baseFee)
	// 	if overflow {
	// 		return msg, errors.New("gasPrice higher than 2^256-1")
	// 	}
	// }
	// if msg.feeCap.IsZero() {
	// 	msg.gasLimit = baseFee.Uint64()
	// }
	// msg.gasPrice.Add(&msg.gasPrice, tx.GetTipCap())
	// if msg.gasPrice.Gt(tx.GetFeeCap()) {
	// 	msg.gasPrice.Set(tx.GetFeeCap())
	// }
	return msg, nil
}

func (tx *ArbitrumInternalTx) Hash() common.Hash {
	return prefixedRlpHash(ArbitrumInternalTxType, []interface{}{
		tx.ChainId,
		tx.Data,
	})
}

func (tx *ArbitrumInternalTx) EncodingSize() int {
	payloadSize := tx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (tx *ArbitrumInternalTx) payloadSize() (size int) {
	// ChainId: add 1 byte for header and the length of ChainId (excluding header)
	size += rlp.Uint256Len(*tx.ChainId)

	// Data: rlp.StringLen returns the full encoded length (header + payload)
	size += rlp.StringLen(tx.Data)
	return size
}

func (tx *ArbitrumInternalTx) encodePayload(w io.Writer, b []byte, payloadSize int) error {
	// Write the RLP list prefix
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	// Encode ChainId
	if err := rlp.EncodeUint256(*tx.ChainId, w, b); err != nil {
		return err
	}

	// Encode Data
	if err := rlp.EncodeString(tx.Data, w, b); err != nil {
		return err
	}

	return nil
}

func (tx *ArbitrumInternalTx) EncodeRLP(w io.Writer) error {
	payloadSize := tx.payloadSize()
	// size of struct prefix and TxType
	envelopeSize := 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)
	// envelope
	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}
	// encode TxType
	b[0] = ArbitrumInternalTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumInternalTx) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	var b []byte
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read ChainId: %w", err)
	}
	tx.ChainId = new(uint256.Int).SetBytes(b)
	if tx.Data, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Data: %w", err)
	}

	if err := s.ListEnd(); err != nil {
		return fmt.Errorf("close ArbitrumInternalTx: %w", err)
	}
	return nil
}

func (tx *ArbitrumInternalTx) MarshalBinary(w io.Writer) error {
	payloadSize := tx.payloadSize()
	b := NewEncodingBuf()
	defer PooledBuf.Put(b)
	// encode TxType
	b[0] = ArbitrumInternalTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize); err != nil {
		return err
	}
	return nil
}

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

func GetArbOSVersion(header *Header, chain *chain.Config) uint64 {
	if !chain.IsArbitrum() {
		return 0
	}
	extraInfo := DeserializeHeaderExtraInformation(header)
	return extraInfo.ArbOSFormatVersion
}
