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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"slices"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/execution/rlp"
)

//(go:generate gencodec -type Receipt -field-override receiptMarshaling -out gen_receipt_json.go)

var errShortTypedReceipt = errors.New("typed receipt too short")

var (
	receiptStatusFailedRLP     = []byte{}
	receiptStatusSuccessfulRLP = []byte{0x01}
)

const (
	// ReceiptStatusFailed is the status code of a transaction if execution failed.
	ReceiptStatusFailed = uint64(0)

	// ReceiptStatusSuccessful is the status code of a transaction if execution succeeded.
	ReceiptStatusSuccessful = uint64(1)
)

// Receipt represents the results of a transaction.
// DESCRIBED: docs/programmers_guide/guide.md#organising-ethereum-state-into-a-merkle-tree
type Receipt struct {
	// Consensus fields: These fields are defined by the Yellow Paper
	Type              uint8  `json:"type,omitempty"`
	PostState         []byte `json:"root" codec:"1"`
	Status            uint64 `json:"status" codec:"2"`
	CumulativeGasUsed uint64 `json:"cumulativeGasUsed" gencodec:"required"`
	Bloom             Bloom  `json:"logsBloom"         gencodec:"required"`
	Logs              Logs   `json:"logs"              gencodec:"required"`

	// Implementation fields: These fields are added by geth when processing a transaction.
	// They are stored in the chain database.
	TxHash          common.Hash    `json:"transactionHash" gencodec:"required"`
	ContractAddress common.Address `json:"contractAddress"`
	GasUsed         uint64         `json:"gasUsed" gencodec:"required"`

	// Inclusion information: These fields provide information about the inclusion of the
	// transaction corresponding to this receipt.
	BlockHash        common.Hash `json:"blockHash,omitempty"`
	BlockNumber      *big.Int    `json:"blockNumber,omitempty"`
	TransactionIndex uint        `json:"transactionIndex"`

	FirstLogIndexWithinBlock uint32 `json:"-"` // field which used to store in db and re-calc
}

type receiptMarshaling struct {
	Type              hexutil.Uint64
	PostState         hexutil.Bytes
	Status            hexutil.Uint64
	CumulativeGasUsed hexutil.Uint64
	GasUsed           hexutil.Uint64
	BlockNumber       *hexutil.Big
	TransactionIndex  hexutil.Uint
}

// receiptRLP is the consensus encoding of a receipt.
type receiptRLP struct {
	PostStateOrStatus []byte
	CumulativeGasUsed uint64
	Bloom             Bloom
	Logs              []*Log
}

// storedReceiptRLP is the storage encoding of a receipt.
type storedReceiptRLP struct {
	Type              uint8
	PostStateOrStatus []byte
	CumulativeGasUsed uint64
	FirstLogIndex     uint32 // Logs have their own incremental Index within block. To allow calc it without re-executing whole block - can store it in Receipt

	Logs []*LogForStorage

	TransactionIndex uint
	ContractAddress  common.Address
	GasUsed          uint64
}

// NewReceipt creates a barebone transaction receipt, copying the init fields.
// Deprecated: create receipts using a struct literal instead.
func NewReceipt(failed bool, cumulativeGasUsed uint64) *Receipt {
	r := &Receipt{
		Type:              LegacyTxType,
		CumulativeGasUsed: cumulativeGasUsed,
	}
	if failed {
		r.Status = ReceiptStatusFailed
	} else {
		r.Status = ReceiptStatusSuccessful
	}
	return r
}

// EncodeRLP implements rlp.Encoder, and flattens the consensus fields of a receipt
// into an RLP stream. If no post state is present, byzantium fork is assumed.
func (r Receipt) EncodeRLP(w io.Writer) error {
	data := &receiptRLP{r.statusEncoding(), r.CumulativeGasUsed, r.Bloom, r.Logs}
	if r.Type == LegacyTxType {
		return rlp.Encode(w, data)
	}
	buf := encodeBufferPool.Get().(*bytes.Buffer)
	defer encodeBufferPool.Put(buf)
	buf.Reset()
	if err := r.encodeTyped(data, buf); err != nil {
		return err
	}
	return rlp.Encode(w, buf.Bytes())
}

// encodeTyped writes the canonical encoding of a typed receipt to w.
func (r *Receipt) encodeTyped(data *receiptRLP, w *bytes.Buffer) error {
	w.WriteByte(r.Type)
	return rlp.Encode(w, data)
}

// MarshalBinary returns the consensus encoding of the receipt.
func (r *Receipt) MarshalBinary() ([]byte, error) {
	if r.Type == LegacyTxType {
		return rlp.EncodeToBytes(r)
	}
	data := &receiptRLP{r.statusEncoding(), r.CumulativeGasUsed, r.Bloom, r.Logs}
	var buf bytes.Buffer
	err := r.encodeTyped(data, &buf)
	return buf.Bytes(), err
}

// UnmarshalBinary decodes the consensus encoding of receipts.
// It supports legacy RLP receipts and EIP-2718 typed receipts.
func (r *Receipt) UnmarshalBinary(b []byte) error {
	if len(b) > 0 && b[0] > 0x7f {
		// It's a legacy receipt decode the RLP
		var data receiptRLP
		err := rlp.DecodeBytes(b, &data)
		if err != nil {
			return err
		}
		r.Type = LegacyTxType
		return r.setFromRLP(data)
	}
	// It's an EIP2718 typed transaction envelope.
	return r.decodeTyped(b)
}

func (r *Receipt) setFromRLP(data receiptRLP) error {
	r.CumulativeGasUsed, r.Bloom, r.Logs = data.CumulativeGasUsed, data.Bloom, data.Logs
	return r.setStatus(data.PostStateOrStatus)
}

// decodeTyped decodes a typed receipt from the canonical format.
func (r *Receipt) decodeTyped(b []byte) error {
	if len(b) <= 1 {
		return errShortTypedReceipt
	}
	switch b[0] {
	case DynamicFeeTxType, AccessListTxType, BlobTxType:
		var data receiptRLP
		err := rlp.DecodeBytes(b[1:], &data)
		if err != nil {
			return err
		}
		r.Type = b[0]
		return r.setFromRLP(data)
	default:
		return ErrTxTypeNotSupported
	}
}

func (r *Receipt) decodePayload(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	var b []byte
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read PostStateOrStatus: %w", err)
	}
	r.setStatus(b)
	if r.CumulativeGasUsed, err = s.Uint(); err != nil {
		return fmt.Errorf("read CumulativeGasUsed: %w", err)
	}
	if err = s.ReadBytes(r.Bloom[:]); err != nil {
		return fmt.Errorf("read Bloom: %w", err)
	}
	// decode logs
	if _, err = s.List(); err != nil {
		return fmt.Errorf("open Logs: %w", err)
	}
	if r.Logs != nil && len(r.Logs) > 0 {
		r.Logs = r.Logs[:0]
	}
	for _, err = s.List(); err == nil; _, err = s.List() {
		r.Logs = append(r.Logs, &Log{})
		log := r.Logs[len(r.Logs)-1]
		if err = s.ReadBytes(log.Address[:]); err != nil {
			return fmt.Errorf("read Address: %w", err)
		}
		if _, err = s.List(); err != nil {
			return fmt.Errorf("open Topics: %w", err)
		}
		for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
			log.Topics = append(log.Topics, common.Hash{})
			if len(b) != 32 {
				return fmt.Errorf("wrong size for Topic: %d", len(b))
			}
			copy(log.Topics[len(log.Topics)-1][:], b)
		}
		if !errors.Is(err, rlp.EOL) {
			return fmt.Errorf("read Topic: %w", err)
		}
		// end of Topics list
		if err = s.ListEnd(); err != nil {
			return fmt.Errorf("close Topics: %w", err)
		}
		if log.Data, err = s.Bytes(); err != nil {
			return fmt.Errorf("read Data: %w", err)
		}
		// end of Log
		if err = s.ListEnd(); err != nil {
			return fmt.Errorf("close Log: %w", err)
		}
	}
	if !errors.Is(err, rlp.EOL) {
		return fmt.Errorf("open Log: %w", err)
	}
	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close Logs: %w", err)
	}
	if err := s.ListEnd(); err != nil {
		return fmt.Errorf("close receipt payload: %w", err)
	}
	return nil
}

// DecodeRLP implements rlp.Decoder, and loads the consensus fields of a receipt
// from an RLP stream.
func (r *Receipt) DecodeRLP(s *rlp.Stream) error {
	kind, size, err := s.Kind()
	if err != nil {
		return err
	}
	switch kind {
	case rlp.List:
		// It's a legacy receipt.
		if err := r.decodePayload(s); err != nil {
			return err
		}
		r.Type = LegacyTxType
	case rlp.String:
		// It's an EIP-2718 typed txn receipt.
		s.NewList(size) // Hack - convert String (envelope) into List
		var b []byte
		if b, err = s.Bytes(); err != nil {
			return fmt.Errorf("read TxType: %w", err)
		}
		if len(b) != 1 {
			return fmt.Errorf("%w, got %d bytes", rlp.ErrWrongTxTypePrefix, len(b))
		}
		r.Type = b[0]
		switch r.Type {
		case AccessListTxType, DynamicFeeTxType, BlobTxType, SetCodeTxType:
			if err := r.decodePayload(s); err != nil {
				return err
			}
		default:
			return ErrTxTypeNotSupported
		}
		if err = s.ListEnd(); err != nil {
			return err
		}
	default:
		return rlp.ErrExpectedList
	}
	return nil
}

func (r *Receipt) setStatus(postStateOrStatus []byte) error {
	switch {
	case bytes.Equal(postStateOrStatus, receiptStatusSuccessfulRLP):
		r.Status = ReceiptStatusSuccessful
	case bytes.Equal(postStateOrStatus, receiptStatusFailedRLP):
		r.Status = ReceiptStatusFailed
	case len(postStateOrStatus) == len(common.Hash{}):
		r.PostState = postStateOrStatus
	default:
		return fmt.Errorf("invalid receipt status %x", postStateOrStatus)
	}
	return nil
}

func (r *Receipt) statusEncoding() []byte {
	if len(r.PostState) == 0 {
		if r.Status == ReceiptStatusFailed {
			return receiptStatusFailedRLP
		}
		return receiptStatusSuccessfulRLP
	}
	return r.PostState
}

// Copy creates a deep copy of the Receipt.
func (r *Receipt) Copy() *Receipt {
	if r == nil {
		return nil
	}
	return &Receipt{
		Type:              r.Type,
		PostState:         slices.Clone(r.PostState),
		Status:            r.Status,
		CumulativeGasUsed: r.CumulativeGasUsed,
		Bloom:             r.Bloom,
		Logs:              r.Logs.Copy(),
		TxHash:            r.TxHash,
		ContractAddress:   r.ContractAddress,
		GasUsed:           r.GasUsed,
		BlockHash:         r.BlockHash,
		BlockNumber:       big.NewInt(0).Set(r.BlockNumber),
		TransactionIndex:  r.TransactionIndex,

		FirstLogIndexWithinBlock: r.FirstLogIndexWithinBlock,
	}
}

type ReceiptsForStorage []*ReceiptForStorage

// ReceiptForStorage is a wrapper around a Receipt with RLP serialization
// that omits the Bloom field and deserialization that re-computes it.
type ReceiptForStorage Receipt

// EncodeRLP implements rlp.Encoder, and flattens all content fields of a receipt
// into an RLP stream.
func (r *ReceiptForStorage) EncodeRLP(w io.Writer) error {
	if r.FirstLogIndexWithinBlock == 0 && len(r.Logs) > 0 {
		r.FirstLogIndexWithinBlock = uint32(r.Logs[0].Index)
	}

	logsForStorage := make([]*LogForStorage, len(r.Logs))
	for i, l := range r.Logs {
		logsForStorage[i] = (*LogForStorage)(l)
	}
	return rlp.Encode(w, &storedReceiptRLP{
		Type:              r.Type,
		PostStateOrStatus: (*Receipt)(r).statusEncoding(),
		CumulativeGasUsed: r.CumulativeGasUsed,
		FirstLogIndex:     r.FirstLogIndexWithinBlock,

		Logs:             logsForStorage,
		GasUsed:          r.GasUsed,
		ContractAddress:  r.ContractAddress,
		TransactionIndex: r.TransactionIndex,
	})
}

// DecodeRLP implements rlp.Decoder, and loads both consensus and implementation
// fields of a receipt from an RLP stream.
func (r *ReceiptForStorage) DecodeRLP(s *rlp.Stream) error {
	var stored storedReceiptRLP
	if err := s.Decode(&stored); err != nil {
		return err
	}
	if err := (*Receipt)(r).setStatus(stored.PostStateOrStatus); err != nil {
		return err
	}
	r.Type = stored.Type
	r.CumulativeGasUsed = stored.CumulativeGasUsed
	r.FirstLogIndexWithinBlock = stored.FirstLogIndex

	r.Logs = make([]*Log, len(stored.Logs))
	for i, log := range stored.Logs {
		r.Logs[i] = (*Log)(log)
	}
	//r.TxHash = stored.TxHash
	r.ContractAddress = stored.ContractAddress
	r.GasUsed = stored.GasUsed
	r.TransactionIndex = stored.TransactionIndex
	//r.Bloom = CreateBloom(Receipts{(*Receipt)(r)})

	return nil
}

// Receipts implements DerivableList for receipts.
type Receipts []*Receipt

// Len returns the number of receipts in this list.
func (rs Receipts) Len() int { return len(rs) }

func (rs Receipts) Copy() Receipts {
	if rs == nil {
		return nil
	}
	rsCopy := make(Receipts, rs.Len())
	for i, r := range rs {
		rsCopy[i] = r.Copy()
	}
	return rsCopy
}

// EncodeIndex encodes the i'th receipt to w.
func (rs Receipts) EncodeIndex(i int, w *bytes.Buffer) {
	r := rs[i]
	data := &receiptRLP{r.statusEncoding(), r.CumulativeGasUsed, r.Bloom, r.Logs}
	switch r.Type {
	case LegacyTxType:
		if err := rlp.Encode(w, data); err != nil {
			panic(err)
		}
	case AccessListTxType:
		//nolint:errcheck
		w.WriteByte(AccessListTxType)
		if err := rlp.Encode(w, data); err != nil {
			panic(err)
		}
	case DynamicFeeTxType:
		w.WriteByte(DynamicFeeTxType)
		if err := rlp.Encode(w, data); err != nil {
			panic(err)
		}
	case BlobTxType:
		w.WriteByte(BlobTxType)
		if err := rlp.Encode(w, data); err != nil {
			panic(err)
		}
	case SetCodeTxType:
		w.WriteByte(SetCodeTxType)
		if err := rlp.Encode(w, data); err != nil {
			panic(err)
		}
	default:
		// For unsupported types, write nothing. Since this is for
		// DeriveSha, the error will be caught matching the derived hash
		// to the block.
	}
}

func (rs Receipts) AssertLogIndex(blockNum uint64) {
	if !dbg.AssertEnabled {
		return
	}
	logIndex := 0
	seen := make(map[uint]struct{}, 16)
	for _, r := range rs {
		// ensure valid field
		if logIndex != int(r.FirstLogIndexWithinBlock) {
			panic(fmt.Sprintf("assert: bn=%d, len(t.BlockReceipts)=%d, lastReceipt.FirstLogIndexWithinBlock=%d, logs=%d", blockNum, len(rs), r.FirstLogIndexWithinBlock, logIndex))
		}
		logIndex += len(r.Logs)

		//no duplicates
		if len(r.Logs) <= 1 {
			continue
		}

		for i := 0; i < len(r.Logs); i++ {
			if _, ok := seen[r.Logs[i].Index]; ok {
				panic(fmt.Sprintf("assert: duplicated log_index %d,  bn=%d", r.Logs[i].Index, blockNum))
			}
			seen[r.Logs[i].Index] = struct{}{}
		}
	}
}

// DeriveFields fills the receipts with their computed fields based on consensus
// data and contextual infos like containing block and transactions.
func (r Receipts) DeriveFields(hash common.Hash, number uint64, txs Transactions, senders []common.Address) error {
	logIndex := uint(0) // logIdx is unique within the block and starts from 0
	if len(txs) != len(r) {
		return fmt.Errorf("transaction and receipt count mismatch, txn count = %d, receipts count = %d", len(txs), len(r))
	}
	if len(senders) != len(txs) {
		return fmt.Errorf("transaction and senders count mismatch, txn count = %d, senders count = %d", len(txs), len(senders))
	}

	blockNumber := new(big.Int).SetUint64(number)
	for i := 0; i < len(r); i++ {
		// The transaction type and hash can be retrieved from the transaction itself
		r[i].Type = txs[i].Type()
		r[i].TxHash = txs[i].Hash()

		// block location fields
		r[i].BlockHash = hash
		r[i].BlockNumber = blockNumber
		r[i].TransactionIndex = uint(i)

		// The contract address can be derived from the transaction itself
		if txs[i].GetTo() == nil {
			// If one wants to deploy a contract, one needs to send a transaction that does not have `To` field
			// and then the address of the contract one is creating this way will depend on the `tx.From`
			// and the nonce of the creating account (which is `tx.From`).
			r[i].ContractAddress = CreateAddress(senders[i], txs[i].GetNonce())
		}
		// The used gas can be calculated based on previous r
		if i == 0 {
			r[i].GasUsed = r[i].CumulativeGasUsed
		} else {
			r[i].GasUsed = r[i].CumulativeGasUsed - r[i-1].CumulativeGasUsed
		}
		// The derived log fields can simply be set from the block and transaction
		for j := 0; j < len(r[i].Logs); j++ {
			r[i].Logs[j].BlockNumber = number
			r[i].Logs[j].BlockHash = hash
			r[i].Logs[j].TxHash = r[i].TxHash
			r[i].Logs[j].TxIndex = uint(i)
			r[i].Logs[j].Index = logIndex
			logIndex++
		}
	}
	return nil
}

// DeriveFieldsV3ForSingleReceipt fills the receipts with their computed fields based on consensus
// data and contextual infos like containing block and transactions.
func (r *Receipt) DeriveFieldsV3ForSingleReceipt(txnIdx int, blockHash common.Hash, blockNum uint64, txn Transaction, prevCumulativeGasUsed uint64) error {
	logIndex := r.FirstLogIndexWithinBlock // logIdx is unique within the block and starts from 0

	sender, ok := txn.cachedSender()
	if !ok {
		return errors.New("tx must have cached sender")
	}

	blockNumber := new(big.Int).SetUint64(blockNum)
	// The transaction type and hash can be retrieved from the transaction itself
	r.Type = txn.Type()
	r.TxHash = txn.Hash()

	// block location fields
	r.BlockHash = blockHash
	r.BlockNumber = blockNumber
	r.TransactionIndex = uint(txnIdx)

	// The contract address can be derived from the transaction itself
	if txn.GetTo() == nil {
		// If one wants to deploy a contract, one needs to send a transaction that does not have `To` field
		// and then the address of the contract one is creating this way will depend on the `tx.From`
		// and the nonce of the creating account (which is `tx.From`).
		r.ContractAddress = CreateAddress(sender, txn.GetNonce())
	}
	// The used gas can be calculated based on previous r
	if txnIdx == 0 {
		r.GasUsed = r.CumulativeGasUsed
	} else {
		r.GasUsed = r.CumulativeGasUsed - prevCumulativeGasUsed
	}

	// The derived log fields can simply be set from the block and transaction
	for j := 0; j < len(r.Logs); j++ {
		r.Logs[j].BlockNumber = blockNum
		r.Logs[j].BlockHash = blockHash
		r.Logs[j].TxHash = r.TxHash
		r.Logs[j].TxIndex = uint(txnIdx)
		r.Logs[j].Index = uint(logIndex)
		logIndex++
	}
	return nil
}

// DeriveFieldsV4ForCachedReceipt fills the receipts with their computed fields based on consensus
// data and contextual infos like containing block and transactions.
func (r *Receipt) DeriveFieldsV4ForCachedReceipt(blockHash common.Hash, blockNum uint64, txnHash common.Hash, calcBloom bool) {
	logIndex := r.FirstLogIndexWithinBlock // logIdx is unique within the block and starts from 0

	r.BlockHash = blockHash
	r.BlockNumber = big.NewInt(int64(blockNum))
	r.TxHash = txnHash

	// The derived log fields can simply be set from the block and transaction
	for j := 0; j < len(r.Logs); j++ {
		r.Logs[j].BlockNumber = blockNum
		r.Logs[j].BlockHash = r.BlockHash
		r.Logs[j].TxHash = r.TxHash
		r.Logs[j].TxIndex = r.TransactionIndex
		r.Logs[j].Index = uint(logIndex)
		logIndex++
	}
	if calcBloom {
		r.Bloom = CreateBloom(Receipts{r})
	}
}

// TODO: maybe make it more prettier (only for debug purposes)
func (r *Receipt) String() string {
	j, err := json.Marshal(r)
	if err != nil {
		return fmt.Sprintf("Error during JSON marshalling, receipt: %+v, error: %s", *r, err)
	}
	return string(j)
}
