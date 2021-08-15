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

// Package types contains data types related to Ethereum consensus.
package types

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/bits"
	"reflect"
	"sync/atomic"
	"time"

	rlp2 "github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/rlp"
)

var (
	EmptyRootHash  = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	EmptyUncleHash = rlpHash([]*Header(nil))
)

// A BlockNonce is a 64-bit hash which proves (combined with the
// mix-hash) that a sufficient amount of computation has been carried
// out on a block.
type BlockNonce [8]byte

// EncodeNonce converts the given integer to a block nonce.
func EncodeNonce(i uint64) BlockNonce {
	var n BlockNonce
	binary.BigEndian.PutUint64(n[:], i)
	return n
}

// Uint64 returns the integer value of a block nonce.
func (n BlockNonce) Uint64() uint64 {
	return binary.BigEndian.Uint64(n[:])
}

// MarshalText encodes n as a hex string with 0x prefix.
func (n BlockNonce) MarshalText() ([]byte, error) {
	return hexutil.Bytes(n[:]).MarshalText()
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (n *BlockNonce) UnmarshalText(input []byte) error {
	return hexutil.UnmarshalFixedText("BlockNonce", input, n[:])
}

// go:generate gencodec -type Header -field-override headerMarshaling -out gen_header_json.go

// Header represents a block header in the Ethereum blockchain.
// DESCRIBED: docs/programmers_guide/guide.md#organising-ethereum-state-into-a-merkle-tree
type Header struct {
	ParentHash  common.Hash    `json:"parentHash"       gencodec:"required"`
	UncleHash   common.Hash    `json:"sha3Uncles"       gencodec:"required"`
	Coinbase    common.Address `json:"miner"            gencodec:"required"`
	Root        common.Hash    `json:"stateRoot"        gencodec:"required"`
	TxHash      common.Hash    `json:"transactionsRoot" gencodec:"required"`
	ReceiptHash common.Hash    `json:"receiptsRoot"     gencodec:"required"`
	Bloom       Bloom          `json:"logsBloom"        gencodec:"required"`
	Difficulty  *big.Int       `json:"difficulty"       gencodec:"required"`
	Number      *big.Int       `json:"number"           gencodec:"required"`
	GasLimit    uint64         `json:"gasLimit"         gencodec:"required"`
	GasUsed     uint64         `json:"gasUsed"          gencodec:"required"`
	Time        uint64         `json:"timestamp"        gencodec:"required"`
	Extra       []byte         `json:"extraData"        gencodec:"required"`
	MixDigest   common.Hash    `json:"mixHash"`
	Nonce       BlockNonce     `json:"nonce"`
	BaseFee     *big.Int       `json:"baseFeePerGas"`
	Eip1559     bool           // to avoid relying on BaseFee != nil for that
	Seal        []rlp.RawValue // AuRa POA network field
	WithSeal    bool           // to avoid relying on Seal != nil for that
}

func (h Header) EncodingSize() int {
	encodingSize := 33 /* ParentHash */ + 33 /* UncleHash */ + 21 /* Coinbase */ + 33 /* Root */ + 33 /* TxHash */ +
		33 /* ReceiptHash */ + 259 /* Bloom */

	var sealListLen int
	if h.WithSeal {
		for i := range h.Seal {
			sealListLen += len(h.Seal[i])
		}
		encodingSize += sealListLen
	} else {
		encodingSize += 33 /* MixDigest */ + 9 /* BlockNonce */
	}
	encodingSize++
	var diffLen int
	if h.Difficulty != nil && h.Difficulty.BitLen() >= 8 {
		diffLen = (h.Difficulty.BitLen() + 7) / 8
	}
	encodingSize += diffLen
	encodingSize++
	var numberLen int
	if h.Number != nil && h.Number.BitLen() >= 8 {
		numberLen = (h.Number.BitLen() + 7) / 8
	}
	encodingSize += numberLen
	encodingSize++
	var gasLimitLen int
	if h.GasLimit >= 128 {
		gasLimitLen = (bits.Len64(h.GasLimit) + 7) / 8
	}
	encodingSize += gasLimitLen
	encodingSize++
	var gasUsedLen int
	if h.GasUsed >= 128 {
		gasUsedLen = (bits.Len64(h.GasUsed) + 7) / 8
	}
	encodingSize += gasUsedLen
	encodingSize++
	var timeLen int
	if h.Time >= 128 {
		timeLen = (bits.Len64(h.Time) + 7) / 8
	}
	encodingSize += timeLen
	// size of Extra
	encodingSize++
	switch len(h.Extra) {
	case 0:
	case 1:
		if h.Extra[0] >= 128 {
			encodingSize++
		}
	default:
		if len(h.Extra) >= 56 {
			encodingSize += (bits.Len(uint(len(h.Extra))) + 7) / 8
		}
		encodingSize += len(h.Extra)
	}
	// size of BaseFee
	var baseFeeLen int
	if h.Eip1559 {
		encodingSize++
		if h.BaseFee.BitLen() >= 8 {
			baseFeeLen = (h.BaseFee.BitLen() + 7) / 8
		}
		encodingSize += baseFeeLen
	}

	return encodingSize
}

func (h Header) EncodeRLP(w io.Writer) error {
	// Precompute the size of the encoding
	encodingSize := 33 /* ParentHash */ + 33 /* UncleHash */ + 21 /* Coinbase */ + 33 /* Root */ + 33 /* TxHash */ +
		33 /* ReceiptHash */ + 259 /* Bloom */

	var sealListLen int
	if h.WithSeal {
		for i := range h.Seal {
			sealListLen += len(h.Seal[i])
		}
		encodingSize += sealListLen
	} else {
		encodingSize += 33 /* MixDigest */ + 9 /* BlockNonce */
	}

	encodingSize++
	var diffLen int
	if h.Difficulty != nil && h.Difficulty.BitLen() >= 8 {
		diffLen = (h.Difficulty.BitLen() + 7) / 8
	}
	encodingSize += diffLen

	encodingSize++
	var numberLen int
	if h.Number != nil && h.Number.BitLen() >= 8 {
		numberLen = (h.Number.BitLen() + 7) / 8
	}
	encodingSize += numberLen

	encodingSize++
	var gasLimitLen int
	if h.GasLimit >= 128 {
		gasLimitLen = (bits.Len64(h.GasLimit) + 7) / 8
	}
	encodingSize += gasLimitLen

	encodingSize++
	var gasUsedLen int
	if h.GasUsed >= 128 {
		gasUsedLen = (bits.Len64(h.GasUsed) + 7) / 8
	}
	encodingSize += gasUsedLen

	encodingSize++
	var timeLen int
	if h.Time >= 128 {
		timeLen = (bits.Len64(h.Time) + 7) / 8
	}
	encodingSize += timeLen
	// size of Extra
	encodingSize++
	switch len(h.Extra) {
	case 0:
	case 1:
		if h.Extra[0] >= 128 {
			encodingSize++
		}
	default:
		if len(h.Extra) >= 56 {
			encodingSize += (bits.Len(uint(len(h.Extra))) + 7) / 8
		}
		encodingSize += len(h.Extra)
	}
	var baseFeeLen int
	if h.Eip1559 {
		encodingSize++
		if h.BaseFee.BitLen() >= 8 {
			baseFeeLen = (h.BaseFee.BitLen() + 7) / 8
		}
		encodingSize += baseFeeLen
	}

	var b [33]byte
	// Prefix
	if err := EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	b[0] = 128 + 32
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(h.ParentHash.Bytes()); err != nil {
		return err
	}
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(h.UncleHash.Bytes()); err != nil {
		return err
	}
	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(h.Coinbase.Bytes()); err != nil {
		return err
	}
	b[0] = 128 + 32
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(h.Root.Bytes()); err != nil {
		return err
	}
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(h.TxHash.Bytes()); err != nil {
		return err
	}
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(h.ReceiptHash.Bytes()); err != nil {
		return err
	}
	b[0] = 183 + 2
	b[1] = 1
	b[2] = 0
	if _, err := w.Write(b[:3]); err != nil {
		return err
	}
	if _, err := w.Write(h.Bloom.Bytes()); err != nil {
		return err
	}
	if h.Difficulty != nil && h.Difficulty.BitLen() > 0 && h.Difficulty.BitLen() < 8 {
		b[0] = byte(h.Difficulty.Uint64())
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		b[0] = 128 + byte(diffLen)
		if h.Difficulty != nil {
			h.Difficulty.FillBytes(b[1 : 1+diffLen])
		}
		if _, err := w.Write(b[:1+diffLen]); err != nil {
			return err
		}
	}
	if h.Number != nil && h.Number.BitLen() > 0 && h.Number.BitLen() < 8 {
		b[0] = byte(h.Number.Uint64())
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		b[0] = 128 + byte(numberLen)
		if h.Number != nil {
			h.Number.FillBytes(b[1 : 1+numberLen])
		}
		if _, err := w.Write(b[:1+numberLen]); err != nil {
			return err
		}
	}
	if h.GasLimit > 0 && h.GasLimit < 128 {
		b[0] = byte(h.GasLimit)
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		binary.BigEndian.PutUint64(b[1:], h.GasLimit)
		b[8-gasLimitLen] = 128 + byte(gasLimitLen)
		if _, err := w.Write(b[8-gasLimitLen : 9]); err != nil {
			return err
		}
	}
	if h.GasUsed > 0 && h.GasUsed < 128 {
		b[0] = byte(h.GasUsed)
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		binary.BigEndian.PutUint64(b[1:], h.GasUsed)
		b[8-gasUsedLen] = 128 + byte(gasUsedLen)
		if _, err := w.Write(b[8-gasUsedLen : 9]); err != nil {
			return err
		}
	}
	if h.Time > 0 && h.Time < 128 {
		b[0] = byte(h.Time)
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		binary.BigEndian.PutUint64(b[1:], h.Time)
		b[8-timeLen] = 128 + byte(timeLen)
		if _, err := w.Write(b[8-timeLen : 9]); err != nil {
			return err
		}
	}
	if err := EncodeString(h.Extra, w, b[:]); err != nil {
		return err
	}

	if h.WithSeal {
		for i := range h.Seal {
			if _, err := w.Write(h.Seal[i]); err != nil {
				return err
			}
		}
	} else {
		b[0] = 128 + 32
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write(h.MixDigest.Bytes()); err != nil {
			return err
		}
		b[0] = 128 + 8
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write(h.Nonce[:]); err != nil {
			return err
		}
	}

	if h.Eip1559 {
		if h.BaseFee.BitLen() > 0 && h.BaseFee.BitLen() < 8 {
			b[0] = byte(h.BaseFee.Uint64())
			if _, err := w.Write(b[:1]); err != nil {
				return err
			}
		} else {
			b[0] = 128 + byte(baseFeeLen)
			h.BaseFee.FillBytes(b[1 : 1+baseFeeLen])
			if _, err := w.Write(b[:1+baseFeeLen]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Header) DecodeRLP(s *rlp.Stream) error {
	if !h.WithSeal { // then tests can enable without env flag
		h.WithSeal = debug.HeadersSeal()
	}
	_, err := s.List()
	if err != nil {
		return err
		// return fmt.Errorf("open header struct: %w", err)
	}
	var b []byte
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read ParentHash: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for ParentHash: %d", len(b))
	}
	copy(h.ParentHash[:], b)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read UncleHash: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for UncleHash: %d", len(b))
	}
	copy(h.UncleHash[:], b)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Coinbase: %w", err)
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for Coinbase: %d", len(b))
	}
	copy(h.Coinbase[:], b)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Root: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for Root: %d", len(b))
	}
	copy(h.Root[:], b)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read TxHash: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for TxHash: %d", len(b))
	}
	copy(h.TxHash[:], b)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read ReceiptHash: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for ReceiptHash: %d", len(b))
	}
	copy(h.ReceiptHash[:], b)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Bloom: %w", err)
	}
	if len(b) != 256 {
		return fmt.Errorf("wrong size for Bloom: %d", len(b))
	}
	copy(h.Bloom[:], b)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Difficulty: %w", err)
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for Difficulty: %d", len(b))
	}
	h.Difficulty = new(big.Int).SetBytes(b)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Number: %w", err)
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for Number: %d", len(b))
	}
	h.Number = new(big.Int).SetBytes(b)
	if h.GasLimit, err = s.Uint(); err != nil {
		return fmt.Errorf("read GasLimit: %w", err)
	}
	if h.GasUsed, err = s.Uint(); err != nil {
		return fmt.Errorf("read GasUsed: %w", err)
	}
	if h.Time, err = s.Uint(); err != nil {
		return fmt.Errorf("read Time: %w", err)
	}
	if h.Extra, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Extra: %w", err)
	}

	if h.WithSeal {
		h.WithSeal = true
		for b, err = s.Raw(); err == nil; b, err = s.Raw() {
			h.Seal = append(h.Seal, b)
		}
		if !errors.Is(err, rlp.EOL) {
			return fmt.Errorf("open accessTuple: %d %w", len(h.Seal), err)
		}
	} else {
		if b, err = s.Bytes(); err != nil {
			return fmt.Errorf("read MixDigest: %w", err)
		}
		if len(b) != 32 {
			return fmt.Errorf("wrong size for MixDigest: %d", len(b))
		}
		copy(h.MixDigest[:], b)
		if b, err = s.Bytes(); err != nil {
			return fmt.Errorf("read Nonce: %w", err)
		}
		if len(b) != 8 {
			return fmt.Errorf("wrong size for Nonce: %d", len(b))
		}
		copy(h.Nonce[:], b)
		if b, err = s.Bytes(); err != nil {
			if errors.Is(err, rlp.EOL) {
				h.BaseFee = nil
				h.Eip1559 = false
				if err := s.ListEnd(); err != nil {
					return fmt.Errorf("close header struct (no basefee): %w", err)
				}
				return nil
			}
			return fmt.Errorf("read BaseFee: %w", err)
		}
		if len(b) > 32 {
			return fmt.Errorf("wrong size for BaseFee: %d", len(b))
		}
		h.Eip1559 = true
		h.BaseFee = new(big.Int).SetBytes(b)
	}
	if err := s.ListEnd(); err != nil {
		return fmt.Errorf("close header struct: %w", err)
	}
	return nil
}

// field type overrides for gencodec
type headerMarshaling struct {
	Difficulty *hexutil.Big
	Number     *hexutil.Big
	GasLimit   hexutil.Uint64
	GasUsed    hexutil.Uint64
	Time       hexutil.Uint64
	Extra      hexutil.Bytes
	BaseFee    *hexutil.Big
	Hash       common.Hash `json:"hash"` // adds call to Hash() in MarshalJSON
}

// Hash returns the block hash of the header, which is simply the keccak256 hash of its
// RLP encoding.
func (h *Header) Hash() common.Hash {
	return rlpHash(h)
}

var headerSize = common.StorageSize(reflect.TypeOf(Header{}).Size())

// Size returns the approximate memory used by all internal contents. It is used
// to approximate and limit the memory consumption of various caches.
func (h *Header) Size() common.StorageSize {
	return headerSize + common.StorageSize(len(h.Extra)+(h.Difficulty.BitLen()+h.Number.BitLen())/8)
}

// SanityCheck checks a few basic things -- these checks are way beyond what
// any 'sane' production values should hold, and can mainly be used to prevent
// that the unbounded fields are stuffed with junk data to add processing
// overhead
func (h *Header) SanityCheck() error {
	if h.Number != nil && !h.Number.IsUint64() {
		return fmt.Errorf("too large block number: bitlen %d", h.Number.BitLen())
	}
	if h.Difficulty != nil {
		if diffLen := h.Difficulty.BitLen(); diffLen > 80 {
			return fmt.Errorf("too large block difficulty: bitlen %d", diffLen)
		}
	}
	if eLen := len(h.Extra); eLen > 100*1024 {
		return fmt.Errorf("too large block extradata: size %d", eLen)
	}
	if h.BaseFee != nil {
		if bfLen := h.BaseFee.BitLen(); bfLen > 256 {
			return fmt.Errorf("too large base fee: bitlen %d", bfLen)
		}
	}
	return nil
}

// EmptyBody returns true if there is no additional 'body' to complete the header
// that is: no transactions and no uncles.
func (h *Header) EmptyBody() bool {
	return h.TxHash == EmptyRootHash && h.UncleHash == EmptyUncleHash
}

// EmptyReceipts returns true if there are no receipts for this header/block.
func (h *Header) EmptyReceipts() bool {
	return h.ReceiptHash == EmptyRootHash
}

// Body is a simple (mutable, non-safe) data container for storing and moving
// a block's data contents (transactions and uncles) together.
type Body struct {
	Transactions []Transaction
	Uncles       []*Header
}

// RawBody is semi-parsed variant of Body, where transactions are still unparsed RLP strings
// It is useful in the situations when actual transaction context is not important, for example
// when downloading Block bodies from other peers or serving them to other peers
type RawBody struct {
	Transactions [][]byte
	Uncles       []*Header
}

type BodyForStorage struct {
	BaseTxId uint64
	TxAmount uint32
	Uncles   []*Header
}

// Block represents an entire block in the Ethereum blockchain.
type Block struct {
	header       *Header
	uncles       []*Header
	transactions Transactions

	// caches
	hash atomic.Value
	size atomic.Value

	// Td is used by package core to store the total difficulty
	// of the chain up to and including the block.
	td *big.Int

	// These fields are used by package eth to track
	// inter-peer block relay.
	ReceivedAt   time.Time
	ReceivedFrom interface{}
}

// DeprecatedTd is an old relic for extracting the TD of a block. It is in the
// code solely to facilitate upgrading the database from the old format to the
// new, after which it should be deleted. Do not use!
func (b *Block) DeprecatedTd() *big.Int {
	return b.td
}

// [deprecated by eth/63]
// StorageBlock defines the RLP encoding of a Block stored in the
// state database. The StorageBlock encoding contains fields that
// would otherwise need to be recomputed.
type StorageBlock Block

// [deprecated by eth/63]
// "storage" block encoding. used for database.
type storageblock struct {
	Header *Header
	Txs    []Transaction
	Uncles []*Header
	TD     *big.Int
}

// Copy transaction senders from body into the transactions
func (b *Body) SendersToTxs(senders []common.Address) {
	if senders == nil {
		return
	}
	for i, tx := range b.Transactions {
		tx.SetSender(senders[i])
	}
}

// Copy transaction senders from transactions to the body
func (b *Body) SendersFromTxs() []common.Address {
	senders := make([]common.Address, len(b.Transactions))
	for i, tx := range b.Transactions {
		if sender, ok := tx.GetSender(); ok {
			senders[i] = sender
		}
	}
	return senders
}

func (rb RawBody) EncodingSize() int {
	payloadSize, _, _ := rb.payloadSize()
	return payloadSize
}

func (rb RawBody) payloadSize() (payloadSize int, txsLen, unclesLen int) {
	// size of Transactions
	payloadSize++
	for _, tx := range rb.Transactions {
		txsLen++
		var txLen = len(tx)
		if txLen >= 56 {
			txsLen += (bits.Len(uint(txLen)) + 7) / 8
		}
		txsLen += txLen
	}
	if txsLen >= 56 {
		payloadSize += (bits.Len(uint(txsLen)) + 7) / 8
	}
	payloadSize += txsLen
	// size of Uncles
	payloadSize++
	for _, uncle := range rb.Uncles {
		unclesLen++
		uncleLen := uncle.EncodingSize()
		if uncleLen >= 56 {
			unclesLen += (bits.Len(uint(uncleLen)) + 7) / 8
		}
		unclesLen += uncleLen
	}
	if unclesLen >= 56 {
		payloadSize += (bits.Len(uint(unclesLen)) + 7) / 8
	}
	payloadSize += unclesLen
	return payloadSize, txsLen, unclesLen
}

func (rb RawBody) EncodeRLP(w io.Writer) error {
	payloadSize, txsLen, unclesLen := rb.payloadSize()
	var b [33]byte
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b[:]); err != nil {
		return err
	}
	// encode Transactions
	if err := EncodeStructSizePrefix(txsLen, w, b[:]); err != nil {
		return err
	}
	for _, tx := range rb.Transactions {
		if _, err := w.Write(tx); err != nil {
			return nil
		}
	}
	// encode Uncles
	if err := EncodeStructSizePrefix(unclesLen, w, b[:]); err != nil {
		return err
	}
	for _, uncle := range rb.Uncles {
		if err := uncle.EncodeRLP(w); err != nil {
			return err
		}
	}
	return nil
}

func (rb *RawBody) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	// decode Transactions
	if _, err = s.List(); err != nil {
		return err
	}
	var tx []byte
	for tx, err = s.Raw(); err == nil; tx, err = s.Raw() {
		rb.Transactions = append(rb.Transactions, tx)
	}
	if !errors.Is(err, rlp.EOL) {
		return err
	}
	// end of Transactions
	if err = s.ListEnd(); err != nil {
		return err
	}
	// decode Uncles
	if _, err = s.List(); err != nil {
		return err
	}
	for err == nil {
		var uncle Header
		if err = uncle.DecodeRLP(s); err != nil {
			break
		}
		rb.Uncles = append(rb.Uncles, &uncle)
	}
	if !errors.Is(err, rlp.EOL) {
		return err
	}
	// end of Uncles
	if err = s.ListEnd(); err != nil {
		return err
	}
	return s.ListEnd()
}

func (bb Body) EncodingSize() int {
	payloadSize, _, _ := bb.payloadSize()
	return payloadSize
}

func (bb Body) payloadSize() (payloadSize int, txsLen, unclesLen int) {
	// size of Transactions
	payloadSize++
	for _, tx := range bb.Transactions {
		txsLen++
		var txLen int
		switch t := tx.(type) {
		case *LegacyTx:
			txLen = t.EncodingSize()
		case *AccessListTx:
			txLen = t.EncodingSize()
		case *DynamicFeeTransaction:
			txLen = t.EncodingSize()
		}
		if txLen >= 56 {
			txsLen += (bits.Len(uint(txLen)) + 7) / 8
		}
		txsLen += txLen
	}
	if txsLen >= 56 {
		payloadSize += (bits.Len(uint(txsLen)) + 7) / 8
	}
	payloadSize += txsLen
	// size of Uncles
	payloadSize++
	for _, uncle := range bb.Uncles {
		unclesLen++
		uncleLen := uncle.EncodingSize()
		if uncleLen >= 56 {
			unclesLen += (bits.Len(uint(uncleLen)) + 7) / 8
		}
		unclesLen += uncleLen
	}
	if unclesLen >= 56 {
		payloadSize += (bits.Len(uint(unclesLen)) + 7) / 8
	}
	payloadSize += unclesLen
	return payloadSize, txsLen, unclesLen
}

func (bb Body) EncodeRLP(w io.Writer) error {
	payloadSize, txsLen, unclesLen := bb.payloadSize()
	var b [33]byte
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b[:]); err != nil {
		return err
	}
	// encode Transactions
	if err := EncodeStructSizePrefix(txsLen, w, b[:]); err != nil {
		return err
	}
	for _, tx := range bb.Transactions {
		switch t := tx.(type) {
		case *LegacyTx:
			if err := t.EncodeRLP(w); err != nil {
				return err
			}
		case *AccessListTx:
			if err := t.EncodeRLP(w); err != nil {
				return err
			}
		case *DynamicFeeTransaction:
			if err := t.EncodeRLP(w); err != nil {
				return err
			}
		}
	}
	// encode Uncles
	if err := EncodeStructSizePrefix(unclesLen, w, b[:]); err != nil {
		return err
	}
	for _, uncle := range bb.Uncles {
		if err := uncle.EncodeRLP(w); err != nil {
			return err
		}
	}
	return nil
}

func (bb *Body) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	// decode Transactions
	if _, err = s.List(); err != nil {
		return err
	}
	var tx Transaction
	for tx, err = DecodeTransaction(s); err == nil; tx, err = DecodeTransaction(s) {
		bb.Transactions = append(bb.Transactions, tx)
	}
	if !errors.Is(err, rlp.EOL) {
		return err
	}
	// end of Transactions
	if err = s.ListEnd(); err != nil {
		return err
	}
	// decode Uncles
	if _, err = s.List(); err != nil {
		return err
	}
	for err == nil {
		var uncle Header
		if err = uncle.DecodeRLP(s); err != nil {
			break
		}
		bb.Uncles = append(bb.Uncles, &uncle)
	}
	if !errors.Is(err, rlp.EOL) {
		return err
	}
	// end of Uncles
	if err = s.ListEnd(); err != nil {
		return err
	}
	return s.ListEnd()
}

// NewBlock creates a new block. The input data is copied,
// changes to header and to the field values will not affect the
// block.
//
// The values of TxHash, UncleHash, ReceiptHash and Bloom in header
// are ignored and set to values derived from the given txs, uncles
// and receipts.
func NewBlock(header *Header, txs []Transaction, uncles []*Header, receipts []*Receipt) *Block {
	b := &Block{header: CopyHeader(header), td: new(big.Int)}

	// TODO: panic if len(txs) != len(receipts)
	if len(txs) == 0 {
		b.header.TxHash = EmptyRootHash
	} else {
		b.header.TxHash = DeriveSha(Transactions(txs))
		b.transactions = make(Transactions, len(txs))
		copy(b.transactions, txs)
	}

	if len(receipts) == 0 {
		b.header.ReceiptHash = EmptyRootHash
	} else {
		b.header.ReceiptHash = DeriveSha(Receipts(receipts))
		b.header.Bloom = CreateBloom(receipts)
	}

	if len(uncles) == 0 {
		b.header.UncleHash = EmptyUncleHash
	} else {
		b.header.UncleHash = CalcUncleHash(uncles)
		b.uncles = make([]*Header, len(uncles))
		for i := range uncles {
			b.uncles[i] = CopyHeader(uncles[i])
		}
	}

	return b
}

// NewBlockFromStorage like NewBlock but used to create Block object when read it from DB
// in this case no reason to copy parts, or re-calculate headers fields - they are all stored in DB
func NewBlockFromStorage(hash common.Hash, header *Header, txs []Transaction, uncles []*Header) *Block {
	b := &Block{header: header, td: new(big.Int), transactions: txs, uncles: uncles}
	b.hash.Store(hash)
	return b
}

// NewBlockWithHeader creates a blxock with the given header data. The
// header data is copied, changes to header and to the field values
// will not affect the block.
func NewBlockWithHeader(header *Header) *Block {
	return &Block{header: CopyHeader(header)}
}

// CopyHeader creates a deep copy of a block header to prevent side effects from
// modifying a header variable.
func CopyHeader(h *Header) *Header {
	cpy := *h
	if cpy.Difficulty = new(big.Int); h.Difficulty != nil {
		cpy.Difficulty.Set(h.Difficulty)
	}
	if cpy.Number = new(big.Int); h.Number != nil {
		cpy.Number.Set(h.Number)
	}
	if h.BaseFee != nil {
		cpy.BaseFee = new(big.Int)
		cpy.BaseFee.Set(h.BaseFee)
	}
	if len(h.Extra) > 0 {
		cpy.Extra = make([]byte, len(h.Extra))
		copy(cpy.Extra, h.Extra)
	}
	if len(h.Seal) > 0 {
		cpy.Seal = make([]rlp.RawValue, len(h.Seal))
		for i := range h.Seal {
			cpy.Seal[i] = common.CopyBytes(h.Seal[i])
		}
	}
	return &cpy
}

// DecodeRLP decodes the Ethereum
func (bb *Block) DecodeRLP(s *rlp.Stream) error {
	size, err := s.List()
	if err != nil {
		return err
	}
	// decode header
	var h Header
	if err = h.DecodeRLP(s); err != nil {
		return err
	}
	bb.header = &h
	// decode Transactions
	if _, err = s.List(); err != nil {
		return err
	}
	var tx Transaction
	for tx, err = DecodeTransaction(s); err == nil; tx, err = DecodeTransaction(s) {
		bb.transactions = append(bb.transactions, tx)
	}
	if !errors.Is(err, rlp.EOL) {
		return err
	}
	// end of Transactions
	if err = s.ListEnd(); err != nil {
		return err
	}
	// decode Uncles
	if _, err = s.List(); err != nil {
		return err
	}
	for err == nil {
		var uncle Header
		if err = uncle.DecodeRLP(s); err != nil {
			break
		}
		bb.uncles = append(bb.uncles, &uncle)
	}
	if !errors.Is(err, rlp.EOL) {
		return err
	}
	// end of Uncles
	if err = s.ListEnd(); err != nil {
		return err
	}
	if err = s.ListEnd(); err != nil {
		return err
	}
	bb.size.Store(common.StorageSize(rlp.ListSize(size)))
	return nil
}

func (bb Block) payloadSize() (payloadSize int, txsLen, unclesLen int) {
	// size of Header
	payloadSize++
	headerLen := bb.header.EncodingSize()
	if headerLen >= 56 {
		payloadSize += (bits.Len(uint(headerLen)) + 7) / 8
	}
	payloadSize += headerLen
	// size of Transactions
	payloadSize++
	for _, tx := range bb.transactions {
		txsLen++
		var txLen int
		switch t := tx.(type) {
		case *LegacyTx:
			txLen = t.EncodingSize()
		case *AccessListTx:
			txLen = t.EncodingSize()
		case *DynamicFeeTransaction:
			txLen = t.EncodingSize()
		}
		if txLen >= 56 {
			txsLen += (bits.Len(uint(txLen)) + 7) / 8
		}
		txsLen += txLen
	}
	if txsLen >= 56 {
		payloadSize += (bits.Len(uint(txsLen)) + 7) / 8
	}
	payloadSize += txsLen
	// size of Uncles
	payloadSize++
	for _, uncle := range bb.uncles {
		unclesLen++
		uncleLen := uncle.EncodingSize()
		if uncleLen >= 56 {
			unclesLen += (bits.Len(uint(uncleLen)) + 7) / 8
		}
		unclesLen += uncleLen
	}
	if unclesLen >= 56 {
		payloadSize += (bits.Len(uint(unclesLen)) + 7) / 8
	}
	payloadSize += unclesLen
	return payloadSize, txsLen, unclesLen
}

func (bb Block) EncodingSize() int {
	payloadSize, _, _ := bb.payloadSize()
	return payloadSize
}

// EncodeRLP serializes b into the Ethereum RLP block format.
func (bb Block) EncodeRLP(w io.Writer) error {
	payloadSize, txsLen, unclesLen := bb.payloadSize()
	var b [33]byte
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b[:]); err != nil {
		return err
	}
	// encode Header
	if err := bb.header.EncodeRLP(w); err != nil {
		return err
	}
	// encode Transactions
	if err := EncodeStructSizePrefix(txsLen, w, b[:]); err != nil {
		return err
	}
	for _, tx := range bb.transactions {
		switch t := tx.(type) {
		case *LegacyTx:
			if err := t.EncodeRLP(w); err != nil {
				return err
			}
		case *AccessListTx:
			if err := t.EncodeRLP(w); err != nil {
				return err
			}
		case *DynamicFeeTransaction:
			if err := t.EncodeRLP(w); err != nil {
				return err
			}
		}
	}
	// encode Uncles
	if err := EncodeStructSizePrefix(unclesLen, w, b[:]); err != nil {
		return err
	}
	for _, uncle := range bb.uncles {
		if err := uncle.EncodeRLP(w); err != nil {
			return err
		}
	}
	return nil
}

// [deprecated by eth/63]
func (b *StorageBlock) DecodeRLP(s *rlp.Stream) error {
	var sb storageblock
	if err := s.Decode(&sb); err != nil {
		return err
	}
	b.header, b.uncles, b.transactions, b.td = sb.Header, sb.Uncles, sb.Txs, sb.TD
	return nil
}

func (b *Block) Uncles() []*Header          { return b.uncles }
func (b *Block) Transactions() Transactions { return b.transactions }

func (b *Block) Transaction(hash common.Hash) Transaction {
	for _, transaction := range b.transactions {
		if transaction.Hash() == hash {
			return transaction
		}
	}
	return nil
}

func (b *Block) Number() *big.Int     { return b.header.Number }
func (b *Block) GasLimit() uint64     { return b.header.GasLimit }
func (b *Block) GasUsed() uint64      { return b.header.GasUsed }
func (b *Block) Difficulty() *big.Int { return new(big.Int).Set(b.header.Difficulty) }
func (b *Block) Time() uint64         { return b.header.Time }

func (b *Block) NumberU64() uint64        { return b.header.Number.Uint64() }
func (b *Block) MixDigest() common.Hash   { return b.header.MixDigest }
func (b *Block) Nonce() uint64            { return binary.BigEndian.Uint64(b.header.Nonce[:]) }
func (b *Block) Bloom() Bloom             { return b.header.Bloom }
func (b *Block) Coinbase() common.Address { return b.header.Coinbase }
func (b *Block) Root() common.Hash        { return b.header.Root }
func (b *Block) ParentHash() common.Hash  { return b.header.ParentHash }
func (b *Block) TxHash() common.Hash      { return b.header.TxHash }
func (b *Block) ReceiptHash() common.Hash { return b.header.ReceiptHash }
func (b *Block) UncleHash() common.Hash   { return b.header.UncleHash }
func (b *Block) Extra() []byte            { return common.CopyBytes(b.header.Extra) }
func (b *Block) BaseFee() *big.Int {
	if b.header.BaseFee == nil {
		return nil
	}
	return new(big.Int).Set(b.header.BaseFee)
}

func (b *Block) Header() *Header { return CopyHeader(b.header) }

// Body returns the non-header content of the block.
func (b *Block) Body() *Body {
	bd := &Body{Transactions: b.transactions, Uncles: b.uncles}
	bd.SendersFromTxs()
	return bd
}
func (b *Block) SendersToTxs(senders []common.Address) {
	if senders == nil {
		return
	}
	for i, tx := range b.transactions {
		tx.SetSender(senders[i])
	}
}

// RawBody creates a RawBody based on the block. It is not very efficient, so
// will probably be removed in favour of RawBlock. Also it panics
func (b *Block) RawBody() *RawBody {
	br := &RawBody{Transactions: make([][]byte, len(b.transactions)), Uncles: b.uncles}
	for i, tx := range b.transactions {
		var err error
		br.Transactions[i], err = rlp.EncodeToBytes(tx)
		if err != nil {
			panic(err)
		}
	}
	return br
}

// Size returns the true RLP encoded storage size of the block, either by encoding
// and returning it, or returning a previsouly cached value.
func (b *Block) Size() common.StorageSize {
	if size := b.size.Load(); size != nil {
		return size.(common.StorageSize)
	}
	c := writeCounter(0)
	rlp.Encode(&c, b)
	b.size.Store(common.StorageSize(c))
	return common.StorageSize(c)
}

// SanityCheck can be used to prevent that unbounded fields are
// stuffed with junk data to add processing overhead
func (b *Block) SanityCheck() error {
	return b.header.SanityCheck()
}

type writeCounter common.StorageSize

func (c *writeCounter) Write(b []byte) (int, error) {
	*c += writeCounter(len(b))
	return len(b), nil
}

func CalcUncleHash(uncles []*Header) common.Hash {
	if len(uncles) == 0 {
		return EmptyUncleHash
	}
	return rlpHash(uncles)
}

// WithSeal returns a new block with the data from b but the header replaced with
// the sealed one.
func (b *Block) WithSeal(header *Header) *Block {
	cpy := *header

	return &Block{
		header:       &cpy,
		transactions: b.transactions,
		uncles:       b.uncles,
	}
}

// WithBody returns a new block with the given transaction and uncle contents.
func (b *Block) WithBody(transactions []Transaction, uncles []*Header) *Block {
	block := &Block{
		header:       CopyHeader(b.header),
		transactions: make([]Transaction, len(transactions)),
		uncles:       make([]*Header, len(uncles)),
	}
	copy(block.transactions, transactions)
	for i := range uncles {
		block.uncles[i] = CopyHeader(uncles[i])
	}
	return block
}

// Hash returns the keccak256 hash of b's header.
// The hash is computed on the first call and cached thereafter.
func (b *Block) Hash() common.Hash {
	if hash := b.hash.Load(); hash != nil {
		return hash.(common.Hash)
	}
	v := b.header.Hash()
	b.hash.Store(v)
	return v
}

type Blocks []*Block

func DecodeOnlyTxMetadataFromBody(payload []byte) (baseTxId uint64, txAmount uint32, err error) {
	pos, _, err := rlp2.List(payload, 0)
	if err != nil {
		return baseTxId, txAmount, err
	}
	pos, baseTxId, err = rlp2.U64(payload, pos)
	if err != nil {
		return baseTxId, txAmount, err
	}
	_, txAmount, err = rlp2.U32(payload, pos)
	if err != nil {
		return baseTxId, txAmount, err
	}
	return
}
