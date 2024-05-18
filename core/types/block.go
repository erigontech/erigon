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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"sync/atomic"

	"github.com/gballet/go-verkle"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	rlp2 "github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/rlp"
)

var (
	EmptyRootHash  = libcommon.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	EmptyUncleHash = rlpHash([]*Header(nil))

	ExtraVanityLength = 32 // Fixed number of extra-data prefix bytes reserved for signer vanity
	ExtraSealLength   = 65 // Fixed number of extra-data suffix bytes reserved for signer seal
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
	return hexutility.Bytes(n[:]).MarshalText()
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (n *BlockNonce) UnmarshalText(input []byte) error {
	return hexutility.UnmarshalFixedText("BlockNonce", input, n[:])
}

// go:generate gencodec -type Header -field-override headerMarshaling -out gen_header_json.go

// Header represents a block header in the Ethereum blockchain.
// DESCRIBED: docs/programmers_guide/guide.md#organising-ethereum-state-into-a-merkle-tree
type Header struct {
	ParentHash  libcommon.Hash    `json:"parentHash"       gencodec:"required"`
	UncleHash   libcommon.Hash    `json:"sha3Uncles"       gencodec:"required"`
	Coinbase    libcommon.Address `json:"miner"`
	Root        libcommon.Hash    `json:"stateRoot"        gencodec:"required"`
	TxHash      libcommon.Hash    `json:"transactionsRoot" gencodec:"required"`
	ReceiptHash libcommon.Hash    `json:"receiptsRoot"     gencodec:"required"`
	Bloom       Bloom             `json:"logsBloom"        gencodec:"required"`
	Difficulty  *big.Int          `json:"difficulty"       gencodec:"required"`
	Number      *big.Int          `json:"number"           gencodec:"required"`
	GasLimit    uint64            `json:"gasLimit"         gencodec:"required"`
	GasUsed     uint64            `json:"gasUsed"          gencodec:"required"`
	Time        uint64            `json:"timestamp"        gencodec:"required"`
	Extra       []byte            `json:"extraData"        gencodec:"required"`
	MixDigest   libcommon.Hash    `json:"mixHash"` // prevRandao after EIP-4399
	Nonce       BlockNonce        `json:"nonce"`
	// AuRa extensions (alternative to MixDigest & Nonce)
	AuRaStep uint64
	AuRaSeal []byte

	BaseFee         *big.Int        `json:"baseFeePerGas"`   // EIP-1559
	WithdrawalsHash *libcommon.Hash `json:"withdrawalsRoot"` // EIP-4895

	// BlobGasUsed & ExcessBlobGas were added by EIP-4844 and are ignored in legacy headers.
	BlobGasUsed   *uint64 `json:"blobGasUsed"`
	ExcessBlobGas *uint64 `json:"excessBlobGas"`

	ParentBeaconBlockRoot *libcommon.Hash `json:"parentBeaconBlockRoot"` // EIP-4788

	RequestsRoot *libcommon.Hash `json:"requestsRoot"` // EIP-7685

	// The verkle proof is ignored in legacy headers
	Verkle        bool
	VerkleProof   []byte
	VerkleKeyVals []verkle.KeyValuePair
}

func (h *Header) EncodingSize() int {
	encodingSize := 33 /* ParentHash */ + 33 /* UncleHash */ + 21 /* Coinbase */ + 33 /* Root */ + 33 /* TxHash */ +
		33 /* ReceiptHash */ + 259 /* Bloom */

	encodingSize++
	if h.Difficulty != nil {
		encodingSize += rlp.BigIntLenExcludingHead(h.Difficulty)
	}
	encodingSize++
	if h.Number != nil {
		encodingSize += rlp.BigIntLenExcludingHead(h.Number)
	}
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(h.GasLimit)
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(h.GasUsed)
	encodingSize++
	encodingSize += rlp.IntLenExcludingHead(h.Time)
	// size of Extra
	encodingSize += rlp2.StringLen(h.Extra)

	if len(h.AuRaSeal) != 0 {
		encodingSize += 1 + rlp.IntLenExcludingHead(h.AuRaStep)
		encodingSize += rlp2.ListPrefixLen(len(h.AuRaSeal)) + len(h.AuRaSeal)
	} else {
		encodingSize += 33 /* MixDigest */ + 9 /* BlockNonce */
	}

	if h.BaseFee != nil {
		encodingSize++
		encodingSize += rlp.BigIntLenExcludingHead(h.BaseFee)
	}

	if h.WithdrawalsHash != nil {
		encodingSize += 33
	}

	if h.BlobGasUsed != nil {
		encodingSize++
		encodingSize += rlp.IntLenExcludingHead(*h.BlobGasUsed)
	}
	if h.ExcessBlobGas != nil {
		encodingSize++
		encodingSize += rlp.IntLenExcludingHead(*h.ExcessBlobGas)
	}

	if h.ParentBeaconBlockRoot != nil {
		encodingSize += 33
	}

	if h.RequestsRoot != nil {
		encodingSize += 33
	}

	if h.Verkle {
		// Encoding of Verkle Proof
		encodingSize += rlp2.StringLen(h.VerkleProof)
		var tmpBuffer bytes.Buffer
		if err := rlp.Encode(&tmpBuffer, h.VerkleKeyVals); err != nil {
			panic(err)
		}
		encodingSize += rlp2.ListPrefixLen(tmpBuffer.Len()) + tmpBuffer.Len()
	}

	return encodingSize
}

func (h *Header) EncodeRLP(w io.Writer) error {
	encodingSize := h.EncodingSize()

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
	if err := rlp.EncodeBigInt(h.Difficulty, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeBigInt(h.Number, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeInt(h.GasLimit, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeInt(h.GasUsed, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeInt(h.Time, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeString(h.Extra, w, b[:]); err != nil {
		return err
	}

	if len(h.AuRaSeal) > 0 {
		if err := rlp.EncodeInt(h.AuRaStep, w, b[:]); err != nil {
			return err
		}
		if err := rlp.EncodeString(h.AuRaSeal, w, b[:]); err != nil {
			return err
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

	if h.BaseFee != nil {
		if err := rlp.EncodeBigInt(h.BaseFee, w, b[:]); err != nil {
			return err
		}
	}

	if h.WithdrawalsHash != nil {
		b[0] = 128 + 32
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write(h.WithdrawalsHash.Bytes()); err != nil {
			return err
		}
	}

	if h.BlobGasUsed != nil {
		if err := rlp.EncodeInt(*h.BlobGasUsed, w, b[:]); err != nil {
			return err
		}
	}
	if h.ExcessBlobGas != nil {
		if err := rlp.EncodeInt(*h.ExcessBlobGas, w, b[:]); err != nil {
			return err
		}
	}

	if h.ParentBeaconBlockRoot != nil {
		b[0] = 128 + 32
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write(h.ParentBeaconBlockRoot.Bytes()); err != nil {
			return err
		}
	}

	if h.RequestsRoot != nil {
		b[0] = 128 + 32
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write(h.RequestsRoot.Bytes()); err != nil {
			return err
		}
	}

	if h.Verkle {
		if err := rlp.EncodeString(h.VerkleProof, w, b[:]); err != nil {
			return err
		}

		if err := rlp.Encode(w, h.VerkleKeyVals); err != nil {
			return nil
		}
	}

	return nil
}

func (h *Header) DecodeRLP(s *rlp.Stream) error {
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
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read Difficulty: %w", err)
	}
	h.Difficulty = new(big.Int).SetBytes(b)
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read Number: %w", err)
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

	_, size, err := s.Kind()
	if err != nil {
		return fmt.Errorf("read MixDigest: %w", err)
	}
	if size != 32 { // AuRa
		if h.AuRaStep, err = s.Uint(); err != nil {
			return fmt.Errorf("read AuRaStep: %w", err)
		}
		if h.AuRaSeal, err = s.Bytes(); err != nil {
			return fmt.Errorf("read AuRaSeal: %w", err)
		}
	} else {
		if b, err = s.Bytes(); err != nil {
			return fmt.Errorf("read MixDigest: %w", err)
		}
		copy(h.MixDigest[:], b)
		if b, err = s.Bytes(); err != nil {
			return fmt.Errorf("read Nonce: %w", err)
		}
		if len(b) != 8 {
			return fmt.Errorf("wrong size for Nonce: %d", len(b))
		}
		copy(h.Nonce[:], b)
	}

	// BaseFee
	if b, err = s.Uint256Bytes(); err != nil {
		if errors.Is(err, rlp.EOL) {
			h.BaseFee = nil
			if err := s.ListEnd(); err != nil {
				return fmt.Errorf("close header struct (no BaseFee): %w", err)
			}
			return nil
		}
		return fmt.Errorf("read BaseFee: %w", err)
	}
	h.BaseFee = new(big.Int).SetBytes(b)

	// WithdrawalsHash
	if b, err = s.Bytes(); err != nil {
		if errors.Is(err, rlp.EOL) {
			h.WithdrawalsHash = nil
			if err := s.ListEnd(); err != nil {
				return fmt.Errorf("close header struct (no WithdrawalsHash): %w", err)
			}
			return nil
		}
		return fmt.Errorf("read WithdrawalsHash: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for WithdrawalsHash: %d", len(b))
	}
	h.WithdrawalsHash = new(libcommon.Hash)
	h.WithdrawalsHash.SetBytes(b)

	var blobGasUsed uint64
	if blobGasUsed, err = s.Uint(); err != nil {
		if errors.Is(err, rlp.EOL) {
			h.BlobGasUsed = nil
			if err := s.ListEnd(); err != nil {
				return fmt.Errorf("close header struct (no BlobGasUsed): %w", err)
			}
			return nil
		}
		return fmt.Errorf("read BlobGasUsed: %w", err)
	}
	h.BlobGasUsed = &blobGasUsed

	var excessBlobGas uint64
	if excessBlobGas, err = s.Uint(); err != nil {
		if errors.Is(err, rlp.EOL) {
			h.ExcessBlobGas = nil
			if err := s.ListEnd(); err != nil {
				return fmt.Errorf("close header struct (no ExcessBlobGas): %w", err)
			}
			return nil
		}
		return fmt.Errorf("read ExcessBlobGas: %w", err)
	}
	h.ExcessBlobGas = &excessBlobGas

	// ParentBeaconBlockRoot
	if b, err = s.Bytes(); err != nil {
		if errors.Is(err, rlp.EOL) {
			h.ParentBeaconBlockRoot = nil
			if err := s.ListEnd(); err != nil {
				return fmt.Errorf("close header struct (no ParentBeaconBlockRoot): %w", err)
			}
			return nil
		}
		return fmt.Errorf("read ParentBeaconBlockRoot: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for ParentBeaconBlockRoot: %d", len(b))
	}
	h.ParentBeaconBlockRoot = new(libcommon.Hash)
	h.ParentBeaconBlockRoot.SetBytes(b)

	// RequestsRoot
	if b, err = s.Bytes(); err != nil {
		if errors.Is(err, rlp.EOL) {
			h.RequestsRoot = nil
			if err := s.ListEnd(); err != nil {
				return fmt.Errorf("close header struct (no RequestsRoot): %w", err)
			}
			return nil
		}
		return fmt.Errorf("read RequestsRoot: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for RequestsRoot: %d", len(b))
	}
	h.RequestsRoot = new(libcommon.Hash)
	h.RequestsRoot.SetBytes(b)

	if h.Verkle {
		if h.VerkleProof, err = s.Bytes(); err != nil {
			return fmt.Errorf("read VerkleProof: %w", err)
		}
		rawKv, err := s.Raw()
		if err != nil {
			return err
		}
		rlp.DecodeBytes(rawKv, h.VerkleKeyVals)
	}

	if err := s.ListEnd(); err != nil {
		return fmt.Errorf("close header struct: %w", err)
	}
	return nil
}

// field type overrides for gencodec
type headerMarshaling struct {
	Difficulty    *hexutil.Big
	Number        *hexutil.Big
	GasLimit      hexutil.Uint64
	GasUsed       hexutil.Uint64
	Time          hexutil.Uint64
	Extra         hexutility.Bytes
	BaseFee       *hexutil.Big
	BlobGasUsed   *hexutil.Uint64
	ExcessBlobGas *hexutil.Uint64
	Hash          libcommon.Hash `json:"hash"` // adds call to Hash() in MarshalJSON
}

// Hash returns the block hash of the header, which is simply the keccak256 hash of its
// RLP encoding.
func (h *Header) Hash() libcommon.Hash {
	return rlpHash(h)
}

var headerSize = common.StorageSize(reflect.TypeOf(Header{}).Size())

// Size returns the approximate memory used by all internal contents. It is used
// to approximate and limit the memory consumption of various caches.
func (h *Header) Size() common.StorageSize {
	s := headerSize
	s += common.StorageSize(len(h.Extra) + libcommon.BitLenToByteLen(h.Difficulty.BitLen()) + libcommon.BitLenToByteLen(h.Number.BitLen()))
	if h.BaseFee != nil {
		s += common.StorageSize(libcommon.BitLenToByteLen(h.BaseFee.BitLen()))
	}
	if h.WithdrawalsHash != nil {
		s += common.StorageSize(32)
	}
	if h.BlobGasUsed != nil {
		s += common.StorageSize(8)
	}
	if h.ExcessBlobGas != nil {
		s += common.StorageSize(8)
	}
	if h.ParentBeaconBlockRoot != nil {
		s += common.StorageSize(32)
	}
	if h.RequestsRoot != nil {
		s += common.StorageSize(32)
	}
	return s
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
		if diffLen := h.Difficulty.BitLen(); diffLen > 192 {
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

// Body is a simple (mutable, non-safe) data container for storing and moving
// a block's data contents (transactions and uncles) together.
type Body struct {
	Transactions []Transaction
	Uncles       []*Header
	Withdrawals  []*Withdrawal
	Requests     []*Request
}

// RawBody is semi-parsed variant of Body, where transactions are still unparsed RLP strings
// It is useful in the situations when actual transaction context is not important, for example
// when downloading Block bodies from other peers or serving them to other peers
type RawBody struct {
	Transactions [][]byte
	Uncles       []*Header
	Withdrawals  []*Withdrawal
	Requests     []*Request
}

type BodyForStorage struct {
	BaseTxId    uint64
	TxAmount    uint32
	Uncles      []*Header
	Withdrawals []*Withdrawal
	Requests    []*Request
}

// Alternative representation of the Block.
type RawBlock struct {
	Header *Header
	Body   *RawBody
}

func (r RawBlock) AsBlock() (*Block, error) {
	b := &Block{header: r.Header}
	b.uncles = r.Body.Uncles
	b.withdrawals = r.Body.Withdrawals

	txs := make([]Transaction, len(r.Body.Transactions))
	for i, tx := range r.Body.Transactions {
		var err error
		if txs[i], err = DecodeTransaction(tx); err != nil {
			return nil, err
		}
	}
	b.transactions = txs

	return b, nil
}

// Block represents an entire block in the Ethereum blockchain.
type Block struct {
	header       *Header
	uncles       []*Header
	transactions Transactions
	withdrawals  []*Withdrawal
	requests     []*Request

	// caches
	hash atomic.Value
	size atomic.Value
}

// Copy transaction senders from body into the transactions
func (b *Body) SendersToTxs(senders []libcommon.Address) {
	if senders == nil {
		return
	}
	for i, tx := range b.Transactions {
		tx.SetSender(senders[i])
	}
}

// Copy transaction senders from transactions to the body
func (b *Body) SendersFromTxs() []libcommon.Address {
	senders := make([]libcommon.Address, len(b.Transactions))
	for i, tx := range b.Transactions {
		if sender, ok := tx.GetSender(); ok {
			senders[i] = sender
		}
	}
	return senders
}

func (rb RawBody) EncodingSize() int {
	payloadSize, _, _, _, _ := rb.payloadSize()
	return payloadSize
}

func (rb RawBody) payloadSize() (payloadSize, txsLen, unclesLen, withdrawalsLen, requestsLen int) {
	// size of Transactions
	for _, tx := range rb.Transactions {
		txsLen += len(tx)
	}
	payloadSize += rlp2.ListPrefixLen(txsLen) + txsLen

	// size of Uncles
	unclesLen += encodingSizeGeneric(rb.Uncles)
	payloadSize += rlp2.ListPrefixLen(unclesLen) + unclesLen

	// size of Withdrawals
	if rb.Withdrawals != nil {
		withdrawalsLen += encodingSizeGeneric(rb.Withdrawals)
		payloadSize += rlp2.ListPrefixLen(withdrawalsLen) + withdrawalsLen
	}

	// size of requests
	if rb.Requests != nil {
		requestsLen += encodingSizeGeneric(rb.Requests)
		payloadSize += rlp2.ListPrefixLen(requestsLen) + requestsLen
	}

	return payloadSize, txsLen, unclesLen, withdrawalsLen, requestsLen
}

func (rb RawBody) EncodeRLP(w io.Writer) error {
	payloadSize, txsLen, unclesLen, withdrawalsLen, requestsLen := rb.payloadSize()
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
	if err := encodeRLPGeneric(rb.Uncles, unclesLen, w, b[:]); err != nil {
		return err
	}
	// encode Withdrawals
	if rb.Withdrawals != nil {
		if err := encodeRLPGeneric(rb.Withdrawals, withdrawalsLen, w, b[:]); err != nil {
			return err
		}
	}
	// encode Requests
	if rb.Requests != nil {
		if err := encodeRLPGeneric(rb.Requests, requestsLen, w, b[:]); err != nil {
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
		if tx == nil {
			return errors.New("RawBody.DecodeRLP tx nil")
		}
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
	if err := decodeUncles(&rb.Uncles, s); err != nil {
		return err
	}
	// decode Withdrawals
	rb.Withdrawals = []*Withdrawal{}
	if err := decodeWithdrawals(&rb.Withdrawals, s); err != nil {
		return err
	}
	// decode Requests
	rb.Requests = []*Request{}
	if err := decodeRequests(&rb.Requests, s); err != nil {
		return err
	}

	return s.ListEnd()
}

func (bfs BodyForStorage) payloadSize() (payloadSize, unclesLen, withdrawalsLen, requestsLen int) {
	baseTxIdLen := 1 + rlp.IntLenExcludingHead(bfs.BaseTxId)
	txAmountLen := 1 + rlp.IntLenExcludingHead(uint64(bfs.TxAmount))

	payloadSize += baseTxIdLen
	payloadSize += txAmountLen

	// size of Uncles
	unclesLen += encodingSizeGeneric(bfs.Uncles)
	payloadSize += rlp2.ListPrefixLen(unclesLen) + unclesLen

	// size of Withdrawals
	if bfs.Withdrawals != nil {
		withdrawalsLen += encodingSizeGeneric(bfs.Withdrawals)
		payloadSize += rlp2.ListPrefixLen(withdrawalsLen) + withdrawalsLen
	}

	// size of Requests
	if bfs.Requests != nil {
		requestsLen += encodingSizeGeneric(bfs.Requests)
		payloadSize += rlp2.ListPrefixLen(requestsLen) + requestsLen
	}

	return payloadSize, unclesLen, withdrawalsLen, requestsLen
}

func (bfs BodyForStorage) EncodeRLP(w io.Writer) error {
	payloadSize, unclesLen, withdrawalsLen, requestsLen := bfs.payloadSize()
	var b [33]byte

	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b[:]); err != nil {
		return err
	}

	// encode BaseTxId
	if err := rlp.Encode(w, bfs.BaseTxId); err != nil {
		return err
	}

	// encode TxAmount
	if err := rlp.Encode(w, bfs.TxAmount); err != nil {
		return err
	}

	// encode Uncles
	if err := encodeRLPGeneric(bfs.Uncles, unclesLen, w, b[:]); err != nil {
		return err
	}
	// encode Withdrawals
	// nil if pre-shanghai, empty slice if shanghai and no withdrawals in block, otherwise non-empty
	if bfs.Withdrawals != nil {
		if err := encodeRLPGeneric(bfs.Withdrawals, withdrawalsLen, w, b[:]); err != nil {
			return err
		}
	}
	// encode Requests
	if bfs.Requests != nil {
		if err := encodeRLPGeneric(bfs.Requests, requestsLen, w, b[:]); err != nil {
			return err
		}
	}
	return nil
}

func (bfs *BodyForStorage) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}

	// decode BaseTxId
	if err = s.Decode(&bfs.BaseTxId); err != nil {
		return err
	}
	// decode TxAmount
	if err = s.Decode(&bfs.TxAmount); err != nil {
		return err
	}
	// decode Uncles
	if err := decodeUncles(&bfs.Uncles, s); err != nil {
		return err
	}
	// decode Withdrawals
	bfs.Withdrawals = []*Withdrawal{}
	if err := decodeWithdrawals(&bfs.Withdrawals, s); err != nil {
		return err
	}
	// decode Requests
	bfs.Requests = []*Request{}
	if err := decodeRequests(&bfs.Requests, s); err != nil {
		return err
	}
	return s.ListEnd()
}

func (bb Body) EncodingSize() int {
	payloadSize, _, _, _, _ := bb.payloadSize()
	return payloadSize
}

func (bb Body) payloadSize() (payloadSize int, txsLen, unclesLen, withdrawalsLen, requestsLen int) {
	// size of Transactions
	txsLen += encodingSizeGeneric(bb.Transactions)
	payloadSize += rlp2.ListPrefixLen(txsLen) + txsLen

	// size of Uncles
	unclesLen += encodingSizeGeneric(bb.Uncles)
	payloadSize += rlp2.ListPrefixLen(unclesLen) + unclesLen

	// size of Withdrawals
	if bb.Withdrawals != nil {
		withdrawalsLen += encodingSizeGeneric(bb.Withdrawals)
		payloadSize += rlp2.ListPrefixLen(withdrawalsLen) + withdrawalsLen
	}

	// size of Requests
	if bb.Requests != nil {
		requestsLen += encodingSizeGeneric(bb.Requests)
		payloadSize += rlp2.ListPrefixLen(requestsLen) + requestsLen
	}

	return payloadSize, txsLen, unclesLen, withdrawalsLen, requestsLen
}

func (bb Body) EncodeRLP(w io.Writer) error {
	payloadSize, txsLen, unclesLen, withdrawalsLen, requestsLen := bb.payloadSize()
	var b [33]byte
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b[:]); err != nil {
		return err
	}
	// encode Transactions
	if err := encodeRLPGeneric(bb.Transactions, txsLen, w, b[:]); err != nil {
		return err
	}
	// encode Uncles
	if err := encodeRLPGeneric(bb.Uncles, unclesLen, w, b[:]); err != nil {
		return err
	}
	// encode Withdrawals
	if bb.Withdrawals != nil {
		if err := encodeRLPGeneric(bb.Withdrawals, withdrawalsLen, w, b[:]); err != nil {
			return err
		}
	}
	// encode Requests
	if bb.Requests != nil {
		if err := encodeRLPGeneric(bb.Requests, requestsLen, w, b[:]); err != nil {
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
	if err := decodeTxns(&bb.Transactions, s); err != nil {
		return err
	}
	// decode Uncles
	if err := decodeUncles(&bb.Uncles, s); err != nil {
		return err
	}
	// decode Withdrawals
	bb.Withdrawals = []*Withdrawal{}
	if err := decodeWithdrawals(&bb.Withdrawals, s); err != nil {
		return err
	}
	// decode Requests
	if err := decodeRequests(&bb.Requests, s); err != nil {
		return err
	}

	return s.ListEnd()
}

// NewBlock creates a new block. The input data is copied,
// changes to header and to the field values will not affect the block.
//
// The values of TxHash, UncleHash, ReceiptHash, Bloom, and WithdrawalHash
// in the header are ignored and set to the values derived from
// the given txs, uncles, receipts, and withdrawals.
func NewBlock(header *Header, txs []Transaction, uncles []*Header, receipts []*Receipt, withdrawals []*Withdrawal, requests []*Request) *Block {
	b := &Block{header: CopyHeader(header)}

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
		b.header.Bloom = Bloom{}
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

	if withdrawals == nil {
		b.header.WithdrawalsHash = nil
	} else if len(withdrawals) == 0 {
		b.header.WithdrawalsHash = &EmptyRootHash
		b.withdrawals = make(Withdrawals, len(withdrawals))
	} else {
		h := DeriveSha(Withdrawals(withdrawals))
		b.header.WithdrawalsHash = &h
		b.withdrawals = make(Withdrawals, len(withdrawals))
		for i, w := range withdrawals {
			wCopy := *w
			b.withdrawals[i] = &wCopy
		}
	}

	b.header.ParentBeaconBlockRoot = header.ParentBeaconBlockRoot

	if requests == nil {
		b.header.RequestsRoot = nil
	} else if len(requests) == 0 {
		b.header.RequestsRoot = &EmptyRootHash // TODO(racytech): is this correct?
		b.requests = make(Requests, len(requests))
	} else {
		h := DeriveSha(Requests(requests))
		b.header.RequestsRoot = &h
		b.requests = make(Requests, len(requests))
		for i, r := range requests {
			rCopy := *r
			b.requests[i] = &rCopy
		}
	}

	return b
}

// NewBlockFromStorage like NewBlock but used to create Block object when read it from DB
// in this case no reason to copy parts, or re-calculate headers fields - they are all stored in DB
func NewBlockFromStorage(hash libcommon.Hash, header *Header, txs []Transaction, uncles []*Header, withdrawals []*Withdrawal, requests []*Request) *Block {
	b := &Block{header: header, transactions: txs, uncles: uncles, withdrawals: withdrawals, requests: requests}
	b.hash.Store(hash)
	return b
}

// NewBlockWithHeader creates a block with the given header data. The
// header data is copied, changes to header and to the field values
// will not affect the block.
func NewBlockWithHeader(header *Header) *Block {
	return &Block{header: CopyHeader(header)}
}

// NewBlockFromNetwork like NewBlock but used to create Block object when assembled from devp2p network messages
// when there is no reason to copy parts, or re-calculate headers fields.
func NewBlockFromNetwork(header *Header, body *Body) *Block {
	return &Block{
		header:       header,
		transactions: body.Transactions,
		uncles:       body.Uncles,
		withdrawals:  body.Withdrawals,
		requests:     body.Requests,
	}
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
	if len(h.AuRaSeal) > 0 {
		cpy.AuRaSeal = make([]byte, len(h.AuRaSeal))
		copy(cpy.AuRaSeal, h.AuRaSeal)
	}
	if h.WithdrawalsHash != nil {
		cpy.WithdrawalsHash = new(libcommon.Hash)
		cpy.WithdrawalsHash.SetBytes(h.WithdrawalsHash.Bytes())
	}
	if h.BlobGasUsed != nil {
		blobGasUsed := *h.BlobGasUsed
		cpy.BlobGasUsed = &blobGasUsed
	}
	if h.ExcessBlobGas != nil {
		excessBlobGas := *h.ExcessBlobGas
		cpy.ExcessBlobGas = &excessBlobGas
	}
	if h.ParentBeaconBlockRoot != nil {
		cpy.ParentBeaconBlockRoot = new(libcommon.Hash)
		cpy.ParentBeaconBlockRoot.SetBytes(h.ParentBeaconBlockRoot.Bytes())
	}
	if h.RequestsRoot != nil {
		cpy.RequestsRoot = new(libcommon.Hash)
		cpy.RequestsRoot.SetBytes(h.RequestsRoot.Bytes())
	}
	return &cpy
}

// DecodeRLP decodes the Ethereum
func (bb *Block) DecodeRLP(s *rlp.Stream) error {
	size, err := s.List()
	if err != nil {
		return err
	}
	bb.size.Store(common.StorageSize(rlp.ListSize(size)))

	// decode header
	var h Header
	if err = h.DecodeRLP(s); err != nil {
		return err
	}
	bb.header = &h

	// decode Transactions
	if err := decodeTxns((*[]Transaction)(&bb.transactions), s); err != nil {
		return err
	}
	// decode Uncles
	if err := decodeUncles(&bb.uncles, s); err != nil {
		return err
	}
	// decode Withdrawals
	bb.withdrawals = []*Withdrawal{}
	if err := decodeWithdrawals(&bb.withdrawals, s); err != nil {
		return err
	}
	// decode Requests
	bb.requests = []*Request{}
	if err := decodeRequests(&bb.requests, s); err != nil {
		return err
	}

	return s.ListEnd()
}

func (bb Block) payloadSize() (payloadSize int, txsLen, unclesLen, withdrawalsLen, requestsLen int) {
	// size of Header
	headerLen := bb.header.EncodingSize()
	payloadSize += rlp2.ListPrefixLen(headerLen) + headerLen

	// size of Transactions
	txsLen += encodingSizeGeneric(bb.transactions)
	payloadSize += rlp2.ListPrefixLen(txsLen) + txsLen

	// size of Uncles
	unclesLen += encodingSizeGeneric(bb.uncles)
	payloadSize += rlp2.ListPrefixLen(unclesLen) + unclesLen

	// size of Withdrawals
	if bb.withdrawals != nil {
		withdrawalsLen += encodingSizeGeneric(bb.withdrawals)
		payloadSize += rlp2.ListPrefixLen(withdrawalsLen) + withdrawalsLen
	}

	// size of Requests
	if bb.requests != nil {
		requestsLen += encodingSizeGeneric(bb.requests)
		payloadSize += rlp2.ListPrefixLen(requestsLen) + requestsLen
	}

	return payloadSize, txsLen, unclesLen, withdrawalsLen, requestsLen
}

func (bb Block) EncodingSize() int {
	payloadSize, _, _, _, _ := bb.payloadSize()
	return payloadSize
}

// EncodeRLP serializes b into the Ethereum RLP block format.
func (bb Block) EncodeRLP(w io.Writer) error {
	payloadSize, txsLen, unclesLen, withdrawalsLen, requestsLen := bb.payloadSize()
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
	if err := encodeRLPGeneric(bb.transactions, txsLen, w, b[:]); err != nil {
		return err
	}
	// encode Uncles
	if err := encodeRLPGeneric(bb.uncles, unclesLen, w, b[:]); err != nil {
		return err
	}
	// encode Withdrawals
	if bb.withdrawals != nil {
		if err := encodeRLPGeneric(bb.withdrawals, withdrawalsLen, w, b[:]); err != nil {
			return err
		}
	}
	// encode Requests
	if bb.requests != nil {
		if err := encodeRLPGeneric(bb.requests, requestsLen, w, b[:]); err != nil {
			return err
		}
	}
	return nil
}

func (b *Block) Uncles() []*Header          { return b.uncles }
func (b *Block) Transactions() Transactions { return b.transactions }

func (b *Block) Transaction(hash libcommon.Hash) Transaction {
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

func (b *Block) NumberU64() uint64           { return b.header.Number.Uint64() }
func (b *Block) MixDigest() libcommon.Hash   { return b.header.MixDigest }
func (b *Block) Nonce() BlockNonce           { return b.header.Nonce }
func (b *Block) NonceU64() uint64            { return b.header.Nonce.Uint64() }
func (b *Block) Bloom() Bloom                { return b.header.Bloom }
func (b *Block) Coinbase() libcommon.Address { return b.header.Coinbase }
func (b *Block) Root() libcommon.Hash        { return b.header.Root }
func (b *Block) ParentHash() libcommon.Hash  { return b.header.ParentHash }
func (b *Block) TxHash() libcommon.Hash      { return b.header.TxHash }
func (b *Block) ReceiptHash() libcommon.Hash { return b.header.ReceiptHash }
func (b *Block) UncleHash() libcommon.Hash   { return b.header.UncleHash }
func (b *Block) Extra() []byte               { return libcommon.CopyBytes(b.header.Extra) }
func (b *Block) BaseFee() *big.Int {
	if b.header.BaseFee == nil {
		return nil
	}
	return new(big.Int).Set(b.header.BaseFee)
}
func (b *Block) WithdrawalsHash() *libcommon.Hash       { return b.header.WithdrawalsHash }
func (b *Block) Withdrawals() Withdrawals               { return b.withdrawals }
func (b *Block) ParentBeaconBlockRoot() *libcommon.Hash { return b.header.ParentBeaconBlockRoot }
func (b *Block) RequestsRoot() *libcommon.Hash          { return b.header.RequestsRoot }
func (b *Block) Requests() Requests                     { return b.requests }

// Header returns a deep-copy of the entire block header using CopyHeader()
func (b *Block) Header() *Header       { return CopyHeader(b.header) }
func (b *Block) HeaderNoCopy() *Header { return b.header }

// Body returns the non-header content of the block.
func (b *Block) Body() *Body {
	bd := &Body{Transactions: b.transactions, Uncles: b.uncles, Withdrawals: b.withdrawals, Requests: b.requests}
	bd.SendersFromTxs()
	return bd
}
func (b *Block) SendersToTxs(senders []libcommon.Address) {
	if len(senders) == 0 {
		return
	}
	for i, tx := range b.transactions {
		tx.SetSender(senders[i])
	}
}

// RawBody creates a RawBody based on the block. It is not very efficient, so
// will probably be removed in favour of RawBlock. Also it panics
func (b *Block) RawBody() *RawBody {
	br := &RawBody{Transactions: make([][]byte, len(b.transactions)), Uncles: b.uncles, Withdrawals: b.withdrawals}
	for i, tx := range b.transactions {
		var err error
		br.Transactions[i], err = rlp.EncodeToBytes(tx)
		if err != nil {
			panic(err)
		}
	}
	return br
}

// RawBody creates a RawBody based on the body.
func (b *Body) RawBody() *RawBody {
	br := &RawBody{Transactions: make([][]byte, len(b.Transactions)), Uncles: b.Uncles, Withdrawals: b.Withdrawals}
	for i, tx := range b.Transactions {
		var err error
		br.Transactions[i], err = rlp.EncodeToBytes(tx)
		if err != nil {
			panic(err)
		}
	}
	return br
}

// Size returns the true RLP encoded storage size of the block, either by encoding
// and returning it, or returning a previously cached value.
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

// HashCheck checks that transactions, receipts, uncles and withdrawals hashes are correct.
func (b *Block) HashCheck() error {
	if hash := DeriveSha(b.Transactions()); hash != b.TxHash() {
		return fmt.Errorf("block has invalid transaction hash: have %x, exp: %x", hash, b.TxHash())
	}

	if len(b.transactions) > 0 && b.ReceiptHash() == EmptyRootHash {
		return fmt.Errorf("block has empty receipt hash: %x but it includes %x transactions", b.ReceiptHash(), len(b.transactions))
	}

	if len(b.transactions) == 0 && b.ReceiptHash() != EmptyRootHash {
		return fmt.Errorf("block has non-empty receipt hash: %x but no transactions", b.ReceiptHash())
	}

	if hash := CalcUncleHash(b.Uncles()); hash != b.UncleHash() {
		return fmt.Errorf("block has invalid uncle hash: have %x, exp: %x", hash, b.UncleHash())
	}

	if b.WithdrawalsHash() == nil {
		if b.Withdrawals() != nil {
			return errors.New("header missing WithdrawalsHash")
		}
		return nil
	}
	if b.Withdrawals() == nil {
		return errors.New("body missing Withdrawals")
	}
	if hash := DeriveSha(b.Withdrawals()); hash != *b.WithdrawalsHash() {
		return fmt.Errorf("block has invalid withdrawals hash: have %x, exp: %x", hash, b.WithdrawalsHash())
	}
	return nil
}

type writeCounter common.StorageSize

func (c *writeCounter) Write(b []byte) (int, error) {
	*c += writeCounter(len(b))
	return len(b), nil
}

func CalcUncleHash(uncles []*Header) libcommon.Hash {
	if len(uncles) == 0 {
		return EmptyUncleHash
	}
	return rlpHash(uncles)
}

func CopyTxs(in Transactions) Transactions {
	transactionsData, err := MarshalTransactionsBinary(in)
	if err != nil {
		panic(fmt.Errorf("MarshalTransactionsBinary failed: %w", err))
	}
	out, err := DecodeTransactions(transactionsData)
	if err != nil {
		panic(fmt.Errorf("DecodeTransactions failed: %w", err))
	}
	for i, tx := range in {
		if txWrapper, ok := tx.(*BlobTxWrapper); ok {
			blobTx := out[i].(*BlobTx)
			out[i] = &BlobTxWrapper{
				Tx:          *blobTx,
				Commitments: txWrapper.Commitments.copy(),
				Blobs:       txWrapper.Blobs.copy(),
				Proofs:      txWrapper.Proofs.copy(),
			}
		}
	}
	return out
}

// Copy creates a deep copy of the Block.
func (b *Block) Copy() *Block {
	uncles := make([]*Header, 0, len(b.uncles))
	for _, uncle := range b.uncles {
		uncles = append(uncles, CopyHeader(uncle))
	}

	var withdrawals []*Withdrawal
	if b.withdrawals != nil {
		withdrawals = make([]*Withdrawal, 0, len(b.withdrawals))
		for _, withdrawal := range b.withdrawals {
			wCopy := *withdrawal
			withdrawals = append(withdrawals, &wCopy)
		}
	}

	var hashValue atomic.Value
	if value := b.hash.Load(); value != nil {
		hash := value.(libcommon.Hash)
		hashCopy := libcommon.BytesToHash(hash.Bytes())
		hashValue.Store(hashCopy)
	}

	var sizeValue atomic.Value
	if size := b.size.Load(); size != nil {
		sizeValue.Store(size)
	}

	return &Block{
		header:       CopyHeader(b.header),
		uncles:       uncles,
		transactions: CopyTxs(b.transactions),
		withdrawals:  withdrawals,
		hash:         hashValue,
		size:         sizeValue,
	}
}

// WithSeal returns a new block with the data from b but the header replaced with
// the sealed one.
func (b *Block) WithSeal(header *Header) *Block {
	cpy := *header

	return &Block{
		header:       &cpy,
		transactions: b.transactions,
		uncles:       b.uncles,
		withdrawals:  b.withdrawals,
	}
}

// Hash returns the keccak256 hash of b's header.
// The hash is computed on the first call and cached thereafter.
func (b *Block) Hash() libcommon.Hash {
	if hash := b.hash.Load(); hash != nil {
		return hash.(libcommon.Hash)
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

type BlockWithReceipts struct {
	Block    *Block
	Receipts Receipts
}

type rlpEncodable interface {
	EncodeRLP(w io.Writer) error
	EncodingSize() int
}

func encodingSizeGeneric[T rlpEncodable](arr []T) (_len int) {
	for _, item := range arr {
		size := item.EncodingSize()
		_len += rlp2.ListPrefixLen(size) + size
	}
	return
}

func encodeRLPGeneric[T rlpEncodable](arr []T, _len int, w io.Writer, b []byte) error {
	if err := EncodeStructSizePrefix(_len, w, b); err != nil {
		return err
	}
	for _, item := range arr {
		if err := item.EncodeRLP(w); err != nil {
			return err
		}
	}
	return nil
}

func decodeTxns(appendList *[]Transaction, s *rlp.Stream) error {
	var err error
	if _, err = s.List(); err != nil {
		return err
	}
	var tx Transaction
	blobTxnsAreWrappedWithBlobs := false
	for tx, err = DecodeRLPTransaction(s, blobTxnsAreWrappedWithBlobs); err == nil; tx, err = DecodeRLPTransaction(s, blobTxnsAreWrappedWithBlobs) {
		*appendList = append(*appendList, tx)
	}
	return checkErrListEnd(s, err)
}

func decodeUncles(appendList *[]*Header, s *rlp.Stream) error {
	var err error
	if _, err = s.List(); err != nil {
		return err
	}
	for err == nil {
		var u Header
		if err = u.DecodeRLP(s); err != nil {
			break
		}
		*appendList = append(*appendList, &u)
	}
	return checkErrListEnd(s, err)
}

func decodeWithdrawals(appendList *[]*Withdrawal, s *rlp.Stream) error {
	var err error
	if _, err = s.List(); err != nil {
		if errors.Is(err, rlp.EOL) {
			*appendList = nil
			return nil // EOL, check for ListEnd is in calling function
		}
		return fmt.Errorf("read Withdrawals: %w", err)
	}
	for err == nil {
		var w Withdrawal
		if err = w.DecodeRLP(s); err != nil {
			break
		}
		*appendList = append(*appendList, &w)
	}
	return checkErrListEnd(s, err)
}

func decodeRequests(appendList *[]*Request, s *rlp.Stream) error {
	var err error
	if _, err = s.List(); err != nil {
		if errors.Is(err, rlp.EOL) {
			*appendList = nil
			return nil
		}
		return fmt.Errorf("read requests: %v", err)
	}
	for err == nil {
		var r Request
		if err = r.DecodeRLP(s); err != nil {
			break
		}
		*appendList = append(*appendList, &r)
	}
	return checkErrListEnd(s, err)
}

func checkErrListEnd(s *rlp.Stream, err error) error {
	if !errors.Is(err, rlp.EOL) {
		return err
	}
	if err = s.ListEnd(); err != nil {
		return err
	}
	return nil
}
