package types

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
	"github.com/prysmaticlabs/go-bitfield"
)

// SignedTransaction contains transaction payload and signature as defined in EIP-6493a
// TODO: maybe this belongs in the cl package?
// TODO: json tags are not final

type SignedTransaction struct {
	Payload   TransactionPayload   `json:"payload"`
	Signature TransactionSignature `json:"signature"`
}

func (tx *SignedTransaction) Clone() clonable.Clonable {
	clone := *tx
	return &clone
}

func (tx *SignedTransaction) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, tx.Payload, tx.Signature)
}

func (tx *SignedTransaction) Static() bool {
	return false
}

func (tx *SignedTransaction) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, tx.Payload, tx.Signature)
}

func (tx *SignedTransaction) EncodingSizeSSZ() int {
	return tx.Payload.EncodingSizeSSZ() + tx.Signature.EncodingSizeSSZ()
}

func (tx *SignedTransaction) sigHash() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(tx.Payload)
}

func (tx *SignedTransaction) txHash() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(tx.Payload, tx.Signature)
}

type TransactionPayload struct {
	Type                byte               `json:"type"`
	ChainID             *uint256.Int       `json:"chainId,omitempty"`
	Nonce               uint64             `json:"nonce"`
	GasPrice            *uint256.Int       `json:"gasPrice,omitempty"`
	Gas                 uint64             `json:"gas"`
	To                  *common.Address    `json:"to"`
	Value               *uint256.Int       `json:"value"`
	Input               []byte             `json:"input"`
	Accesses            *types2.AccessList `json:"accessList,omitempty"`
	Tip                 *uint256.Int       `json:"maxPriorityFeePerGas,omitempty"`
	MaxFeePerBlobGas    *uint256.Int       `json:"maxFeePerBlobGas,omitempty"`
	BlobVersionedHashes *[]common.Hash     `json:"blobVersionedHashes,omitempty"`
}

func (tx *TransactionPayload) Clone() clonable.Clonable {
	clone := *tx
	return &clone
}

func (tx *TransactionPayload) EncodeSSZ(dst []byte) ([]byte, error) {
	active, container, _ := tx.getPayloadContainer()
	return ssz2.MarshalSSZ(dst, container, active)
}

func (tx *TransactionPayload) Static() bool {
	return false
}

func (tx *TransactionPayload) DecodeSSZ(buf []byte, version int) error {
	var container payloadContainer
	var active bitfield.Bitvector32
	return ssz2.UnmarshalSSZ(buf, version, &container, active)
}

func (tx *TransactionPayload) EncodingSizeSSZ() int {
	active, _, containerLen := tx.getPayloadContainer()
	return int(active.Len()) + containerLen
}

func (tx *TransactionPayload) getPayloadContainer() (bitfield.Bitvector32, *payloadContainer, int) {
	active := bitfield.Bitvector32{}
	fields := []interface{}{}
	containerLen := 0

	active.SetBitAt(0, true)
	containerLen += 1
	fields = append(fields, tx.Type) //type_: TransactionType
	active.SetBitAt(1, true)
	containerLen += 8
	fields = append(fields, tx.ChainID.Uint64()) //chain_id: ChainId
	active.SetBitAt(2, true)
	fields = append(fields, tx.Nonce) //nonce: uint64
	containerLen += 8
	active.SetBitAt(3, true)
	containerLen += 32
	fields = append(fields, tx.GasPrice) //max_fee_per_gas: FeePerGas
	active.SetBitAt(4, true)
	containerLen += 8
	fields = append(fields, tx.Gas) //gas: uint64
	if tx.To != nil {
		active.SetBitAt(5, true)
		containerLen += length.Addr
		fields = append(fields, tx.To.Bytes()) //to: Optional[ExecutionAddress]
	}
	active.SetBitAt(6, true)
	containerLen += 32
	fields = append(fields, tx.Value.Bytes()) //value: uint256
	active.SetBitAt(7, true)
	containerLen += len(tx.Input)
	fields = append(fields, tx.Input) //input_: ByteList[MAX_CALLDATA_SIZE]

	switch tx.Type {
	case LegacyTxType:

	case AccessListTxType:
		active.SetBitAt(8, true)
		containerLen += length.Addr + tx.Accesses.StorageKeys()*length.Hash
		fields = append(fields, tx.Accesses) //access_list: List[AccessTuple, MAX_ACCESS_LIST_SIZE]

	case DynamicFeeTxType:
		active.SetBitAt(8, true)
		containerLen += length.Addr + tx.Accesses.StorageKeys()*length.Hash
		fields = append(fields, tx.Accesses) //access_list: List[AccessTuple, MAX_ACCESS_LIST_SIZE]
		active.SetBitAt(9, true)
		containerLen += 32
		fields = append(fields, tx.Tip.Bytes()) //max_priority_fee_per_gas: FeePerGas

	case BlobTxType:
		active.SetBitAt(8, true)
		containerLen += length.Addr + tx.Accesses.StorageKeys()*length.Hash
		fields = append(fields, tx.Accesses) //access_list: List[AccessTuple, MAX_ACCESS_LIST_SIZE]
		active.SetBitAt(9, true)
		containerLen += 32
		fields = append(fields, tx.Tip.Bytes()) //max_priority_fee_per_gas: FeePerGas
		active.SetBitAt(10, true)
		containerLen += 32
		fields = append(fields, tx.MaxFeePerBlobGas.Bytes()) //max_fee_per_blob_gas: FeePerGas
		active.SetBitAt(11, true)
		containerLen += len(*tx.BlobVersionedHashes) * 32
		fields = append(fields, tx.BlobVersionedHashes) //blob_versioned_hashes: List[VersionedHash, MAX_BLOB_COMMITMENTS_PER_BLOCK]
	}

	return active, &payloadContainer{fields}, containerLen
}

type TransactionSignature struct {
	From      common.Address `json:"from"`
	Signature [65]byte       `json:"signature"` // TODO: this needs to be of particular size (see EIP)
}

func (tx *TransactionSignature) Clone() clonable.Clonable {
	clone := *tx
	return &clone
}

func (tx *TransactionSignature) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, tx.From, tx.Signature)
}

func (tx *TransactionSignature) Static() bool {
	return false
}

func (tx *TransactionSignature) DecodeSSZ(buf []byte, version int) error {
	active := uint16(3)
	return ssz2.UnmarshalSSZ(buf, version, active, tx.From.Bytes, tx.Signature[:])
}

func (tx *TransactionSignature) EncodingSizeSSZ() int {
	return length.Addr + len(tx.Signature)
}

type TxVariant uint

const (
	LegacyTxnType TxVariant = iota
	DynamicFeeTxnType
	BlobTxnType
	AccessListTxnType
	SSZTxnType
	BasicTxnType
	ReplayableTxnType
)

func (t *SignedTransaction) GetVariant() TxVariant {
	switch int(t.Payload.Type) {
	case SSZTxType:
		if t.Payload.BlobVersionedHashes != nil {
			return BlobTxnType
		}
		return BasicTxnType

	case BlobTxType:
		return BlobTxnType

	case DynamicFeeTxType:
		return DynamicFeeTxnType

	case AccessListTxType:
		return AccessListTxnType

	default:
		if t.Payload.ChainID != nil {
			return LegacyTxnType
		}
		return ReplayableTxnType
	}
}

func UnmarshalTransctionFromJson(signer Signer, data []byte, blobTxnsAreWrappedWithBlobs bool) (Transaction, error) {
	tx := &SignedTransaction{}
	err := json.Unmarshal(data, tx)
	if err != nil {
		return nil, err
	}

	legacyTx := LegacyTx{
		CommonTx: CommonTx{
			TransactionMisc: TransactionMisc{},
			Nonce:           tx.Payload.Nonce,
			Gas:             tx.Payload.Gas,
			To:              tx.Payload.To,
			Value:           tx.Payload.Value,
			Data:            tx.Payload.Input,
		},
		GasPrice: tx.Payload.GasPrice,
	}

	var txi Transaction
	variant := tx.GetVariant()
	switch variant {
	case SSZTxType:
		blobTx := &BlobTx{
			DynamicFeeTransaction: DynamicFeeTransaction{
				CommonTx:   legacyTx.CommonTx,
				ChainID:    tx.Payload.ChainID,
				Tip:        tx.Payload.Tip,
				FeeCap:     tx.Payload.GasPrice,
				AccessList: *tx.Payload.Accesses,
			},
			BlobVersionedHashes: *tx.Payload.BlobVersionedHashes,
		}
		txi = &SSZTransaction{
			BlobTx: *blobTx,
		}

	case BasicTxnType:
		blobTx := &BlobTx{
			DynamicFeeTransaction: DynamicFeeTransaction{
				CommonTx:   legacyTx.CommonTx,
				ChainID:    tx.Payload.ChainID,
				Tip:        tx.Payload.Tip,
				FeeCap:     tx.Payload.GasPrice,
				AccessList: *tx.Payload.Accesses,
			},
		}
		txi = &SSZTransaction{
			BlobTx: *blobTx,
		}

	case BlobTxnType:
		txi = &BlobTx{
			DynamicFeeTransaction: DynamicFeeTransaction{
				CommonTx:   legacyTx.CommonTx,
				ChainID:    tx.Payload.ChainID,
				Tip:        tx.Payload.Tip,
				FeeCap:     tx.Payload.GasPrice,
				AccessList: *tx.Payload.Accesses,
			},
			BlobVersionedHashes: *tx.Payload.BlobVersionedHashes,
		}

	case DynamicFeeTxnType:
		txi = &DynamicFeeTransaction{
			CommonTx:   legacyTx.CommonTx,
			ChainID:    tx.Payload.ChainID,
			Tip:        tx.Payload.Tip,
			FeeCap:     tx.Payload.GasPrice,
			AccessList: *tx.Payload.Accesses,
		}

	case AccessListTxnType:
		txi = &AccessListTx{
			LegacyTx:   legacyTx,
			ChainID:    tx.Payload.ChainID,
			AccessList: *tx.Payload.Accesses,
		}

	case LegacyTxnType, ReplayableTxnType:
		txi = &legacyTx

	default:
		log.Fatalf("unknown transaction type: %d", variant)
		return nil, nil
	}

	txi, err = txi.WithSignature(signer, tx.Signature.Signature[:])
	if err != nil {
		return nil, err
	}

	// validate transaction

	v, r, s := txi.RawSignatureValues()
	maybeProtected := txi.Type() == LegacyTxType
	if err = sanityCheckSignature(v, r, s, maybeProtected); err != nil {
		return nil, err
	}

	// sender check

	txiSender, err := txi.Sender(signer)
	if err != nil {
		return nil, fmt.Errorf("failed at sender recovery")
	}

	if tx.Signature.From != txiSender {
		return nil, fmt.Errorf("sender mismatch: expected %v, got %v", tx.Signature.From, txiSender)
	}

	txi.SetSender(tx.Signature.From)

	return txi, nil
}

func DecodeTransactionsJson(signer Signer, txs [][]byte) ([]Transaction, error) {
	result := make([]Transaction, len(txs))
	var err error
	for i := range txs {
		result[i], err = UnmarshalTransctionFromJson(signer, txs[i], false /* blobTxnsAreWrappedWithBlobs*/)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

type payloadContainer struct {
	fields []interface{}
}

func (tx *payloadContainer) Clone() clonable.Clonable {
	clone := *tx
	return &clone
}

func (tx *payloadContainer) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, tx.fields...)
}

func (tx *payloadContainer) Static() bool {
	return false
}

func (tx *payloadContainer) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, tx.fields...)
}

func (tx *payloadContainer) EncodingSizeSSZ() int {
	return int(bitfield.Bitvector32{}.Len()) + 64
}
