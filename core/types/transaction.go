// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/log/v3"
	"github.com/protolambda/ztyp/codec"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
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
)

// Transaction is an Ethereum transaction.
type Transaction interface {
	Type() byte
	GetChainID() *uint256.Int
	GetNonce() uint64
	GetPrice() *uint256.Int
	GetTip() *uint256.Int
	GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int
	GetFeeCap() *uint256.Int
	Cost() *uint256.Int
	GetDataHashes() []libcommon.Hash
	GetGas() uint64
	GetDataGas() uint64
	GetValue() *uint256.Int
	Time() time.Time
	GetTo() *libcommon.Address
	AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error)
	WithSignature(signer Signer, sig []byte) (Transaction, error)
	FakeSign(address libcommon.Address) (Transaction, error)
	Hash() libcommon.Hash
	SigningHash(chainID *big.Int) libcommon.Hash
	GetData() []byte
	GetAccessList() types2.AccessList
	Protected() bool
	RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int)
	EncodingSize() int
	EncodeRLP(w io.Writer) error
	MarshalBinary(w io.Writer) error
	// Sender returns the address derived from the signature (V, R, S) using secp256k1
	// elliptic curve and an error if it failed deriving or upon an incorrect
	// signature.
	//
	// Sender may cache the address, allowing it to be used regardless of
	// signing method. The cache is invalidated if the cached signer does
	// not match the signer used in the current call.
	Sender(Signer) (libcommon.Address, error)
	GetSender() (libcommon.Address, bool)
	SetSender(libcommon.Address)
	IsContractDeploy() bool
	Unwrap() Transaction // If this is a network wrapper, returns the unwrapped tx. Otherwiwes returns itself.
}

// TransactionMisc is collection of miscelaneous fields for transaction that is supposed to be embedded into concrete
// implementations of different transaction types
type TransactionMisc struct {
	time time.Time // Time first seen locally (spam avoidance)

	// caches
	hash atomic.Value //nolint:structcheck
	from atomic.Value
}

// RLP-marshalled legacy transactions and binary-marshalled (not wrapped into an RLP string) typed (EIP-2718) transactions
type BinaryTransactions [][]byte

func (t BinaryTransactions) Len() int {
	return len(t)
}

func (t BinaryTransactions) EncodeIndex(i int, w *bytes.Buffer) {
	w.Write(t[i])
}

func (tm TransactionMisc) Time() time.Time {
	return tm.time
}

func (tm TransactionMisc) From() *atomic.Value {
	return &tm.from
}

func DecodeRLPTransaction(s *rlp.Stream) (Transaction, error) {
	kind, size, err := s.Kind()
	if err != nil {
		return nil, err
	}
	if rlp.List == kind {
		tx := &LegacyTx{}
		if err = tx.DecodeRLP(s, size); err != nil {
			return nil, err
		}
		return tx, nil
	}
	if rlp.String != kind {
		return nil, fmt.Errorf("Not an RLP encoded transaction. If this is a canonical encoded transaction, use UnmarshalTransactionFromBinary instead. Got %v for kind, expected String", kind)
	}
	// Decode the EIP-2718 typed TX envelope.
	var b []byte
	if b, err = s.Bytes(); err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, rlp.EOL
	}
	return UnmarshalTransactionFromBinary(b)
}

// DecodeWrappedTransaction decodes network encoded transaction with or without
// envelope. When transaction is not network encoded use DecodeTransaction.
func DecodeWrappedTransaction(data []byte) (Transaction, error) {
	if len(data) == 0 {
		return nil, io.EOF
	}
	if data[0] < 0x80 { // the encoding is canonical, not RLP

		return UnmarshalWrappedTransactionFromBinary(data)
	}
	s := rlp.NewStream(bytes.NewReader(data), uint64(len(data)))
	return DecodeRLPTransaction(s)
}

// DecodeTransaction decodes a transaction either in RLP or canonical format
func DecodeTransaction(data []byte) (Transaction, error) {
	if len(data) == 0 {
		return nil, io.EOF
	}
	if data[0] < 0x80 {
		// the encoding is canonical, not RLP
		return UnmarshalTransactionFromBinary(data)
	}
	s := rlp.NewStream(bytes.NewReader(data), uint64(len(data)))
	return DecodeRLPTransaction(s)
}

// Parse transaction without envelope.
func UnmarshalTransactionFromBinary(data []byte) (Transaction, error) {
	if len(data) <= 1 {
		return nil, fmt.Errorf("short input: %v", len(data))
	}
	switch data[0] {
	case AccessListTxType:
		s := rlp.NewStream(bytes.NewReader(data[1:]), uint64(len(data)-1))
		t := &AccessListTx{}
		if err := t.DecodeRLP(s); err != nil {
			return nil, err
		}
		return t, nil
	case DynamicFeeTxType:
		s := rlp.NewStream(bytes.NewReader(data[1:]), uint64(len(data)-1))
		t := &DynamicFeeTransaction{}
		if err := t.DecodeRLP(s); err != nil {
			return nil, err
		}
		return t, nil
	case BlobTxType:
		s := rlp.NewStream(bytes.NewReader(data[1:]), uint64(len(data)-1))
		t := &BlobTx{}
		if err := t.DecodeRLP(s); err != nil {
			return nil, err
		}
		return t, nil
	default:
		if data[0] >= 0x80 {
			// Tx is type legacy which is RLP encoded
			return DecodeTransaction(data)
		}
		return nil, ErrTxTypeNotSupported
	}
}

// Parse network encoded transaction without envelope.
func UnmarshalWrappedTransactionFromBinary(data []byte) (Transaction, error) {
	if len(data) <= 1 {
		return nil, fmt.Errorf("short input: %v", len(data))
	}
	if data[0] != BlobTxType {
		return UnmarshalTransactionFromBinary(data)
	}
	s := rlp.NewStream(bytes.NewReader(data[1:]), uint64(len(data)-1))
	t := &BlobTxWrapper{}
	if err := t.DecodeRLP(s); err != nil {
		return nil, err
	}
	return t, nil
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
		result[i], err = UnmarshalTransactionFromBinary(txs[i])
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
	if !crypto.ValidateSignatureValues(plainV, r, s, false) {
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

	remove := make(map[libcommon.Hash]struct{})
	for _, tx := range b {
		remove[tx.Hash()] = struct{}{}
	}

	for _, tx := range a {
		if _, ok := remove[tx.Hash()]; !ok {
			keep = append(keep, tx)
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

// TxByPriceAndTime implements both the sort and the heap interface, making it useful
// for all at once sorting as well as individually adding and removing elements.
type TxByPriceAndTime Transactions

func (s TxByPriceAndTime) Len() int { return len(s) }
func (s TxByPriceAndTime) Less(i, j int) bool {
	// If the prices are equal, use the time the transaction was first seen for
	// deterministic sorting
	cmp := s[i].GetPrice().Cmp(s[j].GetPrice())
	if cmp == 0 {
		return s[i].Time().Before(s[j].Time())
	}
	return cmp > 0
}
func (s TxByPriceAndTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s *TxByPriceAndTime) Push(x interface{}) {
	*s = append(*s, x.(Transaction))
}

func (s *TxByPriceAndTime) Pop() interface{} {
	old := *s
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*s = old[0 : n-1]
	return x
}

type TransactionsStream interface {
	Empty() bool
	Peek() Transaction
	Shift()
	Pop()
}

// TransactionsByPriceAndNonce represents a set of transactions that can return
// transactions in a profit-maximizing sorted order, while supporting removing
// entire batches of transactions for non-executable accounts.
type TransactionsByPriceAndNonce struct {
	idx    map[libcommon.Address]int   // Per account nonce-sorted list of transactions
	txs    TransactionsGroupedBySender // Per account nonce-sorted list of transactions
	heads  TxByPriceAndTime            // Next transaction for each unique account (price heap)
	signer Signer                      // Signer for the set of transactions
}

// NewTransactionsByPriceAndNonce creates a transaction set that can retrieve
// price sorted transactions in a nonce-honouring way.
//
// Note, the input map is reowned so the caller should not interact any more with
// if after providing it to the constructor.
func NewTransactionsByPriceAndNonce(signer Signer, txs TransactionsGroupedBySender) *TransactionsByPriceAndNonce {
	// Initialize a price and received time based heap with the head transactions
	heads := make(TxByPriceAndTime, 0, len(txs))
	idx := make(map[libcommon.Address]int, len(txs))
	for i, accTxs := range txs {
		from, _ := accTxs[0].Sender(signer)

		// Ensure the sender address is from the signer
		//if  acc != from {
		//	delete(txs, from)
		//txs[i] = txs[len(txs)-1]
		//txs = txs[:len(txs)-1]
		//continue
		//}
		heads = append(heads, accTxs[0])
		idx[from] = i
		txs[i] = accTxs[1:]
	}
	heap.Init(&heads)

	// Assemble and return the transaction set
	return &TransactionsByPriceAndNonce{
		idx:    idx,
		txs:    txs,
		heads:  heads,
		signer: signer,
	}
}

func (t *TransactionsByPriceAndNonce) Empty() bool {
	if t == nil {
		return true
	}
	return len(t.idx) == 0
}

// Peek returns the next transaction by price.
func (t *TransactionsByPriceAndNonce) Peek() Transaction {
	if len(t.heads) == 0 {
		return nil
	}
	return t.heads[0]
}

// Shift replaces the current best head with the next one from the same account.
func (t *TransactionsByPriceAndNonce) Shift() {
	acc, _ := t.heads[0].Sender(t.signer)
	idx, ok := t.idx[acc]
	if !ok {
		heap.Pop(&t.heads)
		return
	}
	txs := t.txs[idx]
	if len(txs) == 0 {
		heap.Pop(&t.heads)
		return
	}
	t.heads[0], t.txs[idx] = txs[0], txs[1:]
	heap.Fix(&t.heads, 0)
}

// Pop removes the best transaction, *not* replacing it with the next one from
// the same account. This should be used when a transaction cannot be executed
// and hence all subsequent ones should be discarded from the same account.
func (t *TransactionsByPriceAndNonce) Pop() {
	heap.Pop(&t.heads)
}

// TransactionsFixedOrder represents a set of transactions that can return
// transactions in a profit-maximizing sorted order, while supporting removing
// entire batches of transactions for non-executable accounts.
type TransactionsFixedOrder struct {
	Transactions
}

// NewTransactionsFixedOrder creates a transaction set that can retrieve
// price sorted transactions in a nonce-honouring way.
//
// Note, the input map is reowned so the caller should not interact any more with
// if after providing it to the constructor.
func NewTransactionsFixedOrder(txs Transactions) *TransactionsFixedOrder {
	return &TransactionsFixedOrder{txs}
}

func (t *TransactionsFixedOrder) Empty() bool {
	if t == nil {
		return true
	}
	return len(t.Transactions) == 0
}

// Peek returns the next transaction by price.
func (t *TransactionsFixedOrder) Peek() Transaction {
	if len(t.Transactions) == 0 {
		return nil
	}
	return t.Transactions[0]
}

// Shift replaces the current best head with the next one from the same account.
func (t *TransactionsFixedOrder) Shift() {
	t.Transactions = t.Transactions[1:]
}

// Pop removes the best transaction, *not* replacing it with the next one from
// the same account. This should be used when a transaction cannot be executed
// and hence all subsequent ones should be discarded from the same account.
func (t *TransactionsFixedOrder) Pop() {
	t.Transactions = t.Transactions[1:]
}

// Message is a fully derived transaction and implements core.Message
type Message struct {
	to               *libcommon.Address
	from             libcommon.Address
	nonce            uint64
	amount           uint256.Int
	gasLimit         uint64
	gasPrice         uint256.Int
	feeCap           uint256.Int
	tip              uint256.Int
	maxFeePerDataGas uint256.Int
	data             []byte
	accessList       types2.AccessList
	checkNonce       bool
	isFree           bool
	dataHashes       []libcommon.Hash
}

func NewMessage(from libcommon.Address, to *libcommon.Address, nonce uint64, amount *uint256.Int, gasLimit uint64, gasPrice *uint256.Int, feeCap, tip *uint256.Int, data []byte, accessList types2.AccessList, checkNonce bool, isFree bool, maxFeePerDataGas *uint256.Int) Message {
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
	if tip != nil {
		m.tip.Set(tip)
	}
	if feeCap != nil {
		m.feeCap.Set(feeCap)
	}
	if maxFeePerDataGas != nil {
		m.maxFeePerDataGas.Set(maxFeePerDataGas)
	}
	return m
}

func (m Message) From() libcommon.Address       { return m.from }
func (m Message) To() *libcommon.Address        { return m.to }
func (m Message) GasPrice() *uint256.Int        { return &m.gasPrice }
func (m Message) FeeCap() *uint256.Int          { return &m.feeCap }
func (m Message) Tip() *uint256.Int             { return &m.tip }
func (m Message) Value() *uint256.Int           { return &m.amount }
func (m Message) Gas() uint64                   { return m.gasLimit }
func (m Message) Nonce() uint64                 { return m.nonce }
func (m Message) Data() []byte                  { return m.data }
func (m Message) AccessList() types2.AccessList { return m.accessList }
func (m Message) CheckNonce() bool              { return m.checkNonce }
func (m *Message) SetCheckNonce(checkNonce bool) {
	m.checkNonce = checkNonce
}
func (m Message) IsFree() bool { return m.isFree }
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

func (m Message) DataGas() uint64 { return params.DataGasPerBlob * uint64(len(m.dataHashes)) }
func (m Message) MaxFeePerDataGas() *uint256.Int {
	return &m.maxFeePerDataGas
}

func (m Message) DataHashes() []libcommon.Hash { return m.dataHashes }

func DecodeSSZ(data []byte, dest codec.Deserializable) error {
	err := dest.Deserialize(codec.NewDecodingReader(bytes.NewReader(data), uint64(len(data))))
	return err
}

func EncodeSSZ(w io.Writer, obj codec.Serializable) error {
	return obj.Serialize(codec.NewEncodingWriter(w))
}

// copyAddressPtr copies an address.
func copyAddressPtr(a *libcommon.Address) *libcommon.Address {
	if a == nil {
		return nil
	}
	cpy := *a
	return &cpy
}
