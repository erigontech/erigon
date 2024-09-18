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

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/rlp"
)

// go:generate gencodec -type Receipt -field-override receiptMarshaling -out gen_receipt_json.go

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
	TxHash          libcommon.Hash    `json:"transactionHash" gencodec:"required"`
	ContractAddress libcommon.Address `json:"contractAddress"`
	GasUsed         uint64            `json:"gasUsed" gencodec:"required"`

	// Inclusion information: These fields provide information about the inclusion of the
	// transaction corresponding to this receipt.
	BlockHash        libcommon.Hash `json:"blockHash,omitempty"`
	BlockNumber      *big.Int       `json:"blockNumber,omitempty"`
	TransactionIndex uint           `json:"transactionIndex"`

	FirstLogIndexWithinBlock uint32 `json:"-"` // field which used to store in db and re-calc
}

type receiptMarshaling struct {
	Type              hexutil.Uint64
	PostState         hexutility.Bytes
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
	PostStateOrStatus []byte
	CumulativeGasUsed uint64
	FirstLogIndex     uint32 // Logs have their own incremental Index within block. To allow calc it without re-executing whole block - can store it in Receipt
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
	buf := new(bytes.Buffer)
	buf.WriteByte(r.Type)
	if err := rlp.Encode(buf, data); err != nil {
		return err
	}
	return rlp.Encode(w, buf.Bytes())
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
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Bloom: %w", err)
	}
	if len(b) != 256 {
		return fmt.Errorf("wrong size for Bloom: %d", len(b))
	}
	copy(r.Bloom[:], b)
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
		if b, err = s.Bytes(); err != nil {
			return fmt.Errorf("read Address: %w", err)
		}
		if len(b) != 20 {
			return fmt.Errorf("wrong size for Log address: %d", len(b))
		}
		copy(log.Address[:], b)
		if _, err = s.List(); err != nil {
			return fmt.Errorf("open Topics: %w", err)
		}
		for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
			log.Topics = append(log.Topics, libcommon.Hash{})
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
	case len(postStateOrStatus) == len(libcommon.Hash{}):
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
	postState := make([]byte, len(r.PostState))
	copy(postState, r.PostState)

	bloom := BytesToBloom(r.Bloom.Bytes())

	logs := make(Logs, 0, len(r.Logs))
	for _, log := range r.Logs {
		logs = append(logs, log.Copy())
	}

	txHash := libcommon.BytesToHash(r.TxHash.Bytes())
	contractAddress := libcommon.BytesToAddress(r.ContractAddress.Bytes())
	blockHash := libcommon.BytesToHash(r.BlockHash.Bytes())
	blockNumber := big.NewInt(0).Set(r.BlockNumber)

	return &Receipt{
		Type:              r.Type,
		PostState:         postState,
		Status:            r.Status,
		CumulativeGasUsed: r.CumulativeGasUsed,
		Bloom:             bloom,
		Logs:              logs,
		TxHash:            txHash,
		ContractAddress:   contractAddress,
		GasUsed:           r.GasUsed,
		BlockHash:         blockHash,
		BlockNumber:       blockNumber,
		TransactionIndex:  r.TransactionIndex,
	}
}

type ReceiptsForStorage []*ReceiptForStorage

// ReceiptForStorage is a wrapper around a Receipt with RLP serialization
// that omits the Bloom field and deserialization that re-computes it.
type ReceiptForStorage Receipt

// EncodeRLP implements rlp.Encoder, and flattens all content fields of a receipt
// into an RLP stream.
func (r *ReceiptForStorage) EncodeRLP(w io.Writer) error {
	var firstLogIndex uint32
	if len(r.Logs) > 0 {
		firstLogIndex = uint32(r.Logs[0].Index)
	}
	return rlp.Encode(w, &storedReceiptRLP{
		PostStateOrStatus: (*Receipt)(r).statusEncoding(),
		CumulativeGasUsed: r.CumulativeGasUsed,
		FirstLogIndex:     firstLogIndex,
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
	r.CumulativeGasUsed = stored.CumulativeGasUsed
	r.FirstLogIndexWithinBlock = stored.FirstLogIndex

	//r.Logs = make([]*Log, len(stored.Logs))
	//for i, log := range stored.Logs {
	//	r.Logs[i] = (*Log)(log)
	//}
	//r.Bloom = CreateBloom(Receipts{(*Receipt)(r)})

	return nil

}

// Receipts implements DerivableList for receipts.
type Receipts []*Receipt

// Len returns the number of receipts in this list.
func (rs Receipts) Len() int { return len(rs) }

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

// DeriveFields fills the receipts with their computed fields based on consensus
// data and contextual infos like containing block and transactions.
func (r Receipts) DeriveFields(hash libcommon.Hash, number uint64, txs Transactions, senders []libcommon.Address) error {
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
			r[i].ContractAddress = crypto.CreateAddress(senders[i], txs[i].GetNonce())
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

// DeriveFields fills the receipts with their computed fields based on consensus
// data and contextual infos like containing block and transactions.
//func (rl Receipts) DeriveFieldsV3ForSingleReceipt(i int, blockHash libcommon.Hash, blockNum uint64, txn Transaction) (*Receipt, error) {
//	r := rl[i]
//	var prevReceipt *Receipt
//	if i > 0 {
//		prevReceipt = rl[i-1]
//	}
//	err := r.DeriveFieldsV3ForSingleReceipt(i, blockHash, blockNum, txn, prevReceipt)
//	if err != nil {
//		return nil, err
//	}
//	return r, nil
//}

func (r *Receipt) DeriveFieldsV3ForSingleReceipt(txnIdx int, blockHash libcommon.Hash, blockNum uint64, txn Transaction, prevCumulativeGasUsed uint64) error {
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
		r.ContractAddress = crypto.CreateAddress(sender, txn.GetNonce())
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

// TODO: maybe make it more prettier (only for debug purposes)
func (r *Receipt) String() string {
	str := fmt.Sprintf("Receipt of tx %+v", *r)
	return str
}
