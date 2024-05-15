package types

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
	"github.com/prysmaticlabs/go-bitfield"
)

type SSZTransaction struct {
	BlobTx
}

func (tx *SSZTransaction) Hash() libcommon.Hash {
	hash, _ := tx.AsSignedTransation().txHash()
	return libcommon.Hash(hash)
}

func (tx *SSZTransaction) Type() byte {
	return SSZTxType
}

func (tx *SSZTransaction) Unwrap() Transaction {
	return tx
}

func (tx *SSZTransaction) copy() *SSZTransaction {
	return &SSZTransaction{
		BlobTx: *tx.BlobTx.copy(),
	}
}

func (tx *SSZTransaction) AsSignedTransation() *SignedTransaction {
	return &SignedTransaction{*tx.AsTransationPayload(), TransactionSignature{}}
}

func (tx *SSZTransaction) AsTransationPayload() *TransactionPayload {
	return &TransactionPayload{
		Type:                sszType(tx),
		ChainID:             tx.ChainID,
		Nonce:               tx.Nonce,
		GasPrice:            gasPrice(tx),
		Gas:                 tx.Gas,
		To:                  tx.To,
		Value:               tx.Value,
		Input:               tx.Data,
		Accesses:            &tx.AccessList,
		Tip:                 tx.Tip,
		MaxFeePerBlobGas:    tx.MaxFeePerBlobGas,
		BlobVersionedHashes: &tx.BlobVersionedHashes,
	}
}

func sszType(tx Transaction) byte {
	//TODO: This needs to be refined
	return tx.Type()
}

func gasPrice(tx Transaction) *uint256.Int {
	switch tx.Unwrap().Type() {
	case LegacyTxType:
		return tx.GetPrice()
	default:
		return tx.GetFeeCap()
	}
}

// SSZ Support
var _ ssz2.ObjectSSZ = &SignedTransaction{}
var _ ssz2.ObjectSSZ = &TransactionPayload{}
var _ ssz2.ObjectSSZ = &TransactionSignature{}

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

type TransactionPayload struct {
	Type                byte               `json:"type"`                           //0
	ChainID             *uint256.Int       `json:"chainId,omitempty"`              //1
	Nonce               uint64             `json:"nonce"`                          //2
	GasPrice            *uint256.Int       `json:"gasPrice,omitempty"`             //3
	Gas                 uint64             `json:"gas"`                            //4
	To                  *common.Address    `json:"to"`                             //5
	Value               *uint256.Int       `json:"value"`                          //6
	Input               []byte             `json:"input"`                          //7
	Accesses            *types2.AccessList `json:"accessList,omitempty"`           //8
	Tip                 *uint256.Int       `json:"maxPriorityFeePerGas,omitempty"` //9
	MaxFeePerBlobGas    *uint256.Int       `json:"maxFeePerBlobGas,omitempty"`     //10
	BlobVersionedHashes *[]common.Hash     `json:"blobVersionedHashes,omitempty"`  //11
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
	Signature [64]byte       `json:"signature"` // TODO: this needs to be of particular size (see EIP)
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
