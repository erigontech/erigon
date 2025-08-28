// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync/atomic"

	"github.com/holiman/uint256"
	"github.com/protolambda/ztyp/codec"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	libcrypto "github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/rlp"
)

var (
	ErrInvalidSig           = errors.New("invalid transaction v, r, s values")
	ErrUnexpectedProtection = errors.New("transaction type does not supported EIP-155 protected signatures")
	ErrInvalidTxType        = errors.New("transaction type not valid in this context")
	ErrTxTypeNotSupported   = errors.New("transaction type not supported")
)

// Transaction types.
const (
	LegacyTxType = iota
	AccessListTxType
	DynamicFeeTxType
	BlobTxType
	SetCodeTxType
	AccountAbstractionTxType
)

// Transaction is an Ethereum transaction.
type Transaction interface {
	Type() byte
	GetChainID() *uint256.Int
	GetNonce() uint64
	GetTipCap() *uint256.Int                              // max_priority_fee_per_gas in EIP-1559
	GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int // effective_gas_price in EIP-1559
	GetFeeCap() *uint256.Int                              // max_fee_per_gas in EIP-1559
	GetBlobHashes() []common.Hash
	GetGasLimit() uint64
	GetBlobGas() uint64
	GetValue() *uint256.Int
	GetTo() *common.Address
	AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error)
	WithSignature(signer Signer, sig []byte) (Transaction, error)
	Hash() common.Hash
	SigningHash(chainID *big.Int) common.Hash
	GetData() []byte
	GetAccessList() AccessList
	GetAuthorizations() []Authorization // If this is a network wrapper, returns the unwrapped txn. Otherwise returns itself.
	Protected() bool
	RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int)
	EncodingSize() int
	EncodeRLP(w io.Writer) error
	DecodeRLP(s *rlp.Stream) error
	MarshalBinary(w io.Writer) error
	// Sender returns the address derived from the signature (V, R, S) using secp256k1
	// elliptic curve and an error if it failed deriving or upon an incorrect
	// signature.
	//
	// Sender may cache the address, allowing it to be used regardless of
	// signing method. The cache is invalidated if the cached signer does
	// not match the signer used in the current call.
	Sender(Signer) (common.Address, error)
	cachedSender() (common.Address, bool)
	GetSender() (common.Address, bool)
	SetSender(common.Address)
	IsContractDeploy() bool
	Unwrap() Transaction // If this is a network wrapper, returns the unwrapped txn. Otherwise returns itself.
}

// TransactionMisc is collection of miscellaneous fields for transaction that is supposed to be embedded into concrete
// implementations of different transaction types
type TransactionMisc struct {
	// caches
	hash atomic.Pointer[common.Hash]
	from atomic.Pointer[common.Address]
}

// RLP-marshalled legacy transactions and binary-marshalled (not wrapped into an RLP string) typed (EIP-2718) transactions
type BinaryTransactions [][]byte

func (t BinaryTransactions) Len() int {
	return len(t)
}

func (t BinaryTransactions) EncodeIndex(i int, w *bytes.Buffer) {
	w.Write(t[i])
}

func DecodeRLPTransaction(s *rlp.Stream, blobTxnsAreWrappedWithBlobs bool) (Transaction, error) {
	kind, _, err := s.Kind()
	if err != nil {
		return nil, err
	}
	if rlp.List == kind {
		txn := &LegacyTx{}
		if err = txn.DecodeRLP(s); err != nil {
			return nil, err
		}
		return txn, nil
	}
	if rlp.String != kind {
		return nil, fmt.Errorf("not an RLP encoded transaction. If this is a canonical encoded transaction, use UnmarshalTransactionFromBinary instead. Got %v for kind, expected String", kind)
	}
	// Decode the EIP-2718 typed txn envelope.
	var b []byte
	if b, err = s.Bytes(); err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, rlp.EOL
	}
	return UnmarshalTransactionFromBinary(b, blobTxnsAreWrappedWithBlobs)
}

// DecodeWrappedTransaction as similar to DecodeTransaction,
// but type-3 (blob) transactions are expected to be wrapped with blobs/commitments/proofs.
// See https://eips.ethereum.org/EIPS/eip-4844#networking
func DecodeWrappedTransaction(data []byte) (Transaction, error) {
	blobTxnsAreWrappedWithBlobs := true
	if len(data) == 0 {
		return nil, io.EOF
	}
	if data[0] < 0x80 { // the encoding is canonical, not RLP
		return UnmarshalTransactionFromBinary(data, blobTxnsAreWrappedWithBlobs)
	}
	s, done := rlp.NewStreamFromPool(bytes.NewReader(data), uint64(len(data)))
	defer done()
	return DecodeRLPTransaction(s, blobTxnsAreWrappedWithBlobs)
}

// DecodeTransaction decodes a transaction either in RLP or canonical format
func DecodeTransaction(data []byte) (Transaction, error) {
	blobTxnsAreWrappedWithBlobs := false
	if len(data) == 0 {
		return nil, io.EOF
	}
	if data[0] < 0x80 { // the encoding is canonical, not RLP
		return UnmarshalTransactionFromBinary(data, blobTxnsAreWrappedWithBlobs)
	}
	s, done := rlp.NewStreamFromPool(bytes.NewReader(data), uint64(len(data)))
	defer done()
	tx, err := DecodeRLPTransaction(s, blobTxnsAreWrappedWithBlobs)
	if err != nil {
		return nil, err
	}
	if s.Remaining() != 0 {
		return nil, errors.New("trailing bytes after rlp encoded transaction")
	}
	return tx, nil
}

// Parse transaction without envelope.
func UnmarshalTransactionFromBinary(data []byte, blobTxnsAreWrappedWithBlobs bool) (Transaction, error) {
	if len(data) <= 1 {
		return nil, fmt.Errorf("short input: %v", len(data))
	}
	s, done := rlp.NewStreamFromPool(bytes.NewReader(data[1:]), uint64(len(data)-1))
	defer done()
	var t Transaction
	switch data[0] {
	case AccessListTxType:
		t = &AccessListTx{}
	case DynamicFeeTxType:
		t = &DynamicFeeTransaction{}
	case BlobTxType:
		if blobTxnsAreWrappedWithBlobs {
			t = &BlobTxWrapper{}
		} else {
			t = &BlobTx{}
		}
	case SetCodeTxType:
		t = &SetCodeTransaction{}
	case AccountAbstractionTxType:
		t = &AccountAbstractionTransaction{}
	default:
		if data[0] >= 0x80 {
			// txn is type legacy which is RLP encoded
			return DecodeTransaction(data)
		}
		return nil, ErrTxTypeNotSupported
	}
	if err := t.DecodeRLP(s); err != nil {
		return nil, err
	}
	if s.Remaining() != 0 {
		return nil, errors.New("trailing bytes after rlp encoded transaction")
	}
	return t, nil
}

// Removes everything but the payload body from blob tx and prepends 0x3 at the beginning - no copy
// Doesn't change non-blob tx
func UnwrapTxPlayloadRlp(blobTxRlp []byte) ([]byte, error) {
	if blobTxRlp[0] != BlobTxType {
		return blobTxRlp, nil
	}
	dataposPrev, _, isList, err := rlp.Prefix(blobTxRlp[1:], 0)
	if err != nil || dataposPrev < 1 {
		return nil, err
	}
	if !isList { // This is clearly not wrapped txn then
		return blobTxRlp, nil
	}

	blobTxRlp = blobTxRlp[1:]
	// Get to the wrapper list
	datapos, datalen, err := rlp.ParseList(blobTxRlp, dataposPrev)
	if err != nil {
		return nil, err
	}
	blobTxRlp = blobTxRlp[dataposPrev-1 : datapos+datalen] // seekInFiles left an extra-bit
	blobTxRlp[0] = 0x3
	// Include the prefix part of the rlp
	return blobTxRlp, nil
}

func MarshalTransactionsBinary(txs Transactions) ([][]byte, error) {
	var err error
	var buf bytes.Buffer
	result := make([][]byte, len(txs))
	for i := range txs {
		if txs[i] == nil {
			result[i] = nil
			continue
		}
		buf.Reset()
		err = txs[i].MarshalBinary(&buf)
		if err != nil {
			return nil, err
		}
		result[i] = common.CopyBytes(buf.Bytes())
	}
	return result, nil
}

func DecodeTransactions(txs [][]byte) ([]Transaction, error) {
	result := make([]Transaction, len(txs))
	var err error
	for i := range txs {
		result[i], err = UnmarshalTransactionFromBinary(txs[i], false /* blobTxnsAreWrappedWithBlobs*/)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func TypedTransactionMarshalledAsRlpString(data []byte) bool {
	// Unless it's a single byte, serialized RLP strings have their first byte in the [0x80, 0xc0) range
	return len(data) > 0 && 0x80 <= data[0] && data[0] < 0xc0
}

func sanityCheckSignature(v *uint256.Int, r *uint256.Int, s *uint256.Int, maybeProtected bool) error {
	if isProtectedV(v) && !maybeProtected {
		return ErrUnexpectedProtection
	}

	var plainV byte
	if isProtectedV(v) {
		chainID := DeriveChainId(v).Uint64()
		plainV = byte(v.Uint64() - 35 - 2*chainID)
	} else if maybeProtected {
		// Only EIP-155 signatures can be optionally protected. Since
		// we determined this v value is not protected, it must be a
		// raw 27 or 28.
		plainV = byte(v.Uint64() - 27)
	} else {
		// If the signature is not optionally protected, we assume it
		// must already be equal to the recovery id.
		plainV = byte(v.Uint64())
	}
	if !libcrypto.TransactionSignatureIsValid(plainV, r, s, true /* allowPreEip2s */) {
		return ErrInvalidSig
	}

	return nil
}

func isProtectedV(V *uint256.Int) bool {
	if V.BitLen() <= 8 {
		v := V.Uint64()
		return v != 27 && v != 28 && v != 1 && v != 0
	}
	// anything not 27 or 28 is considered protected
	return true
}

// Transactions implements DerivableList for transactions.
type Transactions []Transaction

// Len returns the length of s.
func (s Transactions) Len() int { return len(s) }

// EncodeIndex encodes the i'th transaction to w. Note that this does not check for errors
// because we assume that *Transaction will only ever contain valid txs that were either
// constructed by decoding or via public API in this package.
func (s Transactions) EncodeIndex(i int, w *bytes.Buffer) {
	if err := s[i].MarshalBinary(w); err != nil {
		panic(err)
	}
}

// TransactionsGroupedBySender - lists of transactions grouped by sender
type TransactionsGroupedBySender []Transactions

// TxDifference returns a new set which is the difference between a and b.
func TxDifference(a, b Transactions) Transactions {
	keep := make(Transactions, 0, len(a))

	remove := make(map[common.Hash]struct{})
	for _, txn := range b {
		remove[txn.Hash()] = struct{}{}
	}

	for _, txn := range a {
		if _, ok := remove[txn.Hash()]; !ok {
			keep = append(keep, txn)
		}
	}

	return keep
}

// TxByNonce implements the sort interface to allow sorting a list of transactions
// by their nonces. This is usually only useful for sorting transactions from a
// single account, otherwise a nonce comparison doesn't make much sense.
type TxByNonce Transactions

func (s TxByNonce) Len() int           { return len(s) }
func (s TxByNonce) Less(i, j int) bool { return s[i].GetNonce() < s[j].GetNonce() }
func (s TxByNonce) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Message is a fully derived transaction and implements core.Message
type Message struct {
	to               *common.Address
	from             common.Address
	nonce            uint64
	amount           uint256.Int
	gasLimit         uint64
	gasPrice         uint256.Int
	feeCap           uint256.Int
	tipCap           uint256.Int
	maxFeePerBlobGas uint256.Int
	data             []byte
	accessList       AccessList
	checkNonce       bool
	isFree           bool
	blobHashes       []common.Hash
	authorizations   []Authorization
}

func NewMessage(from common.Address, to *common.Address, nonce uint64, amount *uint256.Int, gasLimit uint64,
	gasPrice *uint256.Int, feeCap, tipCap *uint256.Int, data []byte, accessList AccessList, checkNonce bool,
	isFree bool, maxFeePerBlobGas *uint256.Int,
) *Message {
	m := Message{
		from:       from,
		to:         to,
		nonce:      nonce,
		amount:     *amount,
		gasLimit:   gasLimit,
		data:       data,
		accessList: accessList,
		checkNonce: checkNonce,
		isFree:     isFree,
	}
	if gasPrice != nil {
		m.gasPrice.Set(gasPrice)
	}
	if tipCap != nil {
		m.tipCap.Set(tipCap)
	}
	if feeCap != nil {
		m.feeCap.Set(feeCap)
	}
	if maxFeePerBlobGas != nil {
		m.maxFeePerBlobGas.Set(maxFeePerBlobGas)
	}
	return &m
}

func (m *Message) From() common.Address            { return m.from }
func (m *Message) To() *common.Address             { return m.to }
func (m *Message) GasPrice() *uint256.Int          { return &m.gasPrice }
func (m *Message) FeeCap() *uint256.Int            { return &m.feeCap }
func (m *Message) TipCap() *uint256.Int            { return &m.tipCap }
func (m *Message) Value() *uint256.Int             { return &m.amount }
func (m *Message) Gas() uint64                     { return m.gasLimit }
func (m *Message) Nonce() uint64                   { return m.nonce }
func (m *Message) Data() []byte                    { return m.data }
func (m *Message) AccessList() AccessList          { return m.accessList }
func (m *Message) Authorizations() []Authorization { return m.authorizations }
func (m *Message) SetBlobVersionedHashes(blobHashes []common.Hash) {
	m.blobHashes = blobHashes
}
func (m *Message) SetAuthorizations(authorizations []Authorization) {
	m.authorizations = authorizations
}
func (m *Message) CheckNonce() bool { return m.checkNonce }
func (m *Message) SetCheckNonce(checkNonce bool) {
	m.checkNonce = checkNonce
}
func (m *Message) IsFree() bool { return m.isFree }
func (m *Message) SetIsFree(isFree bool) {
	m.isFree = isFree
}

func (m *Message) ChangeGas(globalGasCap, desiredGas uint64) {
	gas := globalGasCap
	if gas == 0 {
		gas = uint64(math.MaxUint64 / 2)
	}
	if desiredGas > 0 {
		gas = desiredGas
	}
	if globalGasCap != 0 && globalGasCap < gas {
		log.Warn("Caller gas above allowance, capping", "requested", gas, "cap", globalGasCap)
		gas = globalGasCap
	}

	m.gasLimit = gas
}

func (m *Message) BlobGas() uint64 { return params.GasPerBlob * uint64(len(m.blobHashes)) }

func (m *Message) MaxFeePerBlobGas() *uint256.Int {
	return &m.maxFeePerBlobGas
}

func (m *Message) BlobHashes() []common.Hash { return m.blobHashes }

func DecodeSSZ(data []byte, dest codec.Deserializable) error {
	err := dest.Deserialize(codec.NewDecodingReader(bytes.NewReader(data), uint64(len(data))))
	return err
}

func EncodeSSZ(w io.Writer, obj codec.Serializable) error {
	return obj.Serialize(codec.NewEncodingWriter(w))
}
