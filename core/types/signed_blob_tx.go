package types

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/protolambda/ztyp/codec"
	"github.com/protolambda/ztyp/conv"
	. "github.com/protolambda/ztyp/view"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
)

type ECDSASignature struct {
	V Uint8View
	R Uint256View
	S Uint256View
}

func (sig *ECDSASignature) GetV() *uint256.Int {
	bytes := uint256.Int([4]uint64{uint64(sig.V), 0, 0, 0})
	return &bytes
}

func (sig *ECDSASignature) GetR() *uint256.Int {
	r := uint256.Int(sig.R)
	return &r
}

func (sig *ECDSASignature) GetS() *uint256.Int {
	s := uint256.Int(sig.S)
	return &s
}

func (sig *ECDSASignature) Deserialize(dr *codec.DecodingReader) error {
	return dr.FixedLenContainer(&sig.V, &sig.R, &sig.S)
}

func (sig *ECDSASignature) Serialize(w *codec.EncodingWriter) error {
	return w.FixedLenContainer(&sig.V, &sig.R, &sig.S)
}

func (*ECDSASignature) ByteLength() uint64 {
	return 1 + 32 + 32
}

func (*ECDSASignature) FixedLength() uint64 {
	return 1 + 32 + 32
}

type AddressSSZ libcommon.Address

func (addr *AddressSSZ) Deserialize(dr *codec.DecodingReader) error {
	if addr == nil {
		return errors.New("cannot deserialize into nil Address")
	}
	_, err := dr.Read(addr[:])
	return err
}

func (addr *AddressSSZ) Serialize(w *codec.EncodingWriter) error {
	return w.Write(addr[:])
}

func (*AddressSSZ) ByteLength() uint64 {
	return 20
}

func (*AddressSSZ) FixedLength() uint64 {
	return 20
}

// AddressOptionalSSZ implements Union[None, Address]
type AddressOptionalSSZ struct {
	Address *AddressSSZ
}

func (ao *AddressOptionalSSZ) Deserialize(dr *codec.DecodingReader) error {
	if ao == nil {
		return errors.New("cannot deserialize into nil Address")
	}
	if v, err := dr.ReadByte(); err != nil {
		return err
	} else if v == 0 {
		ao.Address = nil
		return nil
	} else if v == 1 {
		ao.Address = new(AddressSSZ)
		if err := ao.Address.Deserialize(dr); err != nil {
			return err
		}
		return nil
	} else {
		return fmt.Errorf("invalid union selector for Union[None, Address]: %d", v)
	}
}

func (ao *AddressOptionalSSZ) Serialize(w *codec.EncodingWriter) error {
	if ao.Address == nil {
		return w.WriteByte(0)
	} else {
		if err := w.WriteByte(1); err != nil {
			return err
		}
		return ao.Address.Serialize(w)
	}
}

func (ao *AddressOptionalSSZ) ByteLength() uint64 {
	if ao.Address == nil {
		return 1
	} else {
		return 1 + 20
	}
}

func (*AddressOptionalSSZ) FixedLength() uint64 {
	return 0
}

type TxDataView []byte

func (tdv *TxDataView) Deserialize(dr *codec.DecodingReader) error {
	return dr.ByteList((*[]byte)(tdv), MAX_CALLDATA_SIZE)
}

func (tdv TxDataView) Serialize(w *codec.EncodingWriter) error {
	return w.Write(tdv)
}

func (tdv TxDataView) ByteLength() (out uint64) {
	return uint64(len(tdv))
}

func (tdv *TxDataView) FixedLength() uint64 {
	return 0
}

func (tdv TxDataView) MarshalText() ([]byte, error) {
	return conv.BytesMarshalText(tdv[:])
}

func (tdv TxDataView) String() string {
	return "0x" + hex.EncodeToString(tdv[:])
}

func (tdv *TxDataView) UnmarshalText(text []byte) error {
	if tdv == nil {
		return errors.New("cannot decode into nil blob data")
	}
	return conv.DynamicBytesUnmarshalText((*[]byte)(tdv), text)
}

func ReadHashes(dr *codec.DecodingReader, hashes *[]libcommon.Hash, length uint64) error {
	if uint64(len(*hashes)) != length {
		// re-use space if available (for recycling old state objects)
		if uint64(cap(*hashes)) >= length {
			*hashes = (*hashes)[:length]
		} else {
			*hashes = make([]libcommon.Hash, length)
		}
	} else if *hashes == nil {
		// make sure the output is never nil
		*hashes = []libcommon.Hash{}
	}
	dst := *hashes
	for i := uint64(0); i < length; i++ {
		if _, err := dr.Read(dst[i][:]); err != nil {
			return err
		}
	}
	return nil
}

func ReadHashesLimited(dr *codec.DecodingReader, hashes *[]libcommon.Hash, limit uint64) error {
	scope := dr.Scope()
	if scope%32 != 0 {
		return fmt.Errorf("bad deserialization scope, cannot decode hashes list")
	}
	length := scope / 32
	if length > limit {
		return fmt.Errorf("too many hashes: %d > %d", length, limit)
	}
	return ReadHashes(dr, hashes, length)
}

func WriteHashes(ew *codec.EncodingWriter, hashes []libcommon.Hash) error {
	for i := range hashes {
		if err := ew.Write(hashes[i][:]); err != nil {
			return err
		}
	}
	return nil
}

type VersionedHashesView []libcommon.Hash

func (vhv *VersionedHashesView) Deserialize(dr *codec.DecodingReader) error {
	return ReadHashesLimited(dr, (*[]libcommon.Hash)(vhv), MAX_VERSIONED_HASHES_LIST_SIZE)
}

func (vhv VersionedHashesView) Serialize(w *codec.EncodingWriter) error {
	return WriteHashes(w, vhv)
}

func (vhv VersionedHashesView) ByteLength() (out uint64) {
	return uint64(len(vhv)) * 32
}

func (vhv *VersionedHashesView) FixedLength() uint64 {
	return 0 // it's a list, no fixed length
}

type StorageKeysView []libcommon.Hash

func (skv *StorageKeysView) Deserialize(dr *codec.DecodingReader) error {
	return ReadHashesLimited(dr, (*[]libcommon.Hash)(skv), MAX_ACCESS_LIST_STORAGE_KEYS)
}

func (skv StorageKeysView) Serialize(w *codec.EncodingWriter) error {
	return WriteHashes(w, skv)
}

func (skv StorageKeysView) ByteLength() (out uint64) {
	return uint64(len(skv)) * 32
}

func (skv *StorageKeysView) FixedLength() uint64 {
	return 0 // it's a list, no fixed length
}

type AccessTupleView types2.AccessTuple

func (atv *AccessTupleView) Deserialize(dr *codec.DecodingReader) error {
	return dr.Container((*AddressSSZ)(&atv.Address), (*StorageKeysView)(&atv.StorageKeys))
}

func (atv *AccessTupleView) Serialize(w *codec.EncodingWriter) error {
	return w.Container((*AddressSSZ)(&atv.Address), (*StorageKeysView)(&atv.StorageKeys))
}

func (atv *AccessTupleView) ByteLength() uint64 {
	return codec.ContainerLength((*AddressSSZ)(&atv.Address), (*StorageKeysView)(&atv.StorageKeys))
}

func (atv *AccessTupleView) FixedLength() uint64 {
	return 0
}

type AccessListView types2.AccessList

func (alv *AccessListView) Deserialize(dr *codec.DecodingReader) error {
	*alv = AccessListView([]types2.AccessTuple{})
	return dr.List(func() codec.Deserializable {
		i := len(*alv)
		*alv = append(*alv, types2.AccessTuple{})
		return (*AccessTupleView)(&((*alv)[i]))
	}, 0, MAX_ACCESS_LIST_SIZE)
}

func (alv AccessListView) Serialize(w *codec.EncodingWriter) error {
	return w.List(func(i uint64) codec.Serializable {
		return (*AccessTupleView)(&alv[i])
	}, 0, uint64(len(alv)))
}

func (alv AccessListView) ByteLength() (out uint64) {
	for idx := range alv {
		out += (*AccessTupleView)(&alv[idx]).ByteLength() + codec.OFFSET_SIZE
	}
	return
}

func (alv *AccessListView) FixedLength() uint64 {
	return 0
}

type BlobTxMessage struct {
	ChainID          Uint256View
	Nonce            Uint64View
	GasTipCap        Uint256View // a.k.a. maxPriorityFeePerGas
	GasFeeCap        Uint256View // a.k.a. maxFeePerGas
	Gas              Uint64View
	To               AddressOptionalSSZ // nil means contract creation
	Value            Uint256View
	Data             TxDataView
	AccessList       AccessListView
	MaxFeePerDataGas Uint256View

	BlobVersionedHashes VersionedHashesView
}

func (tx *BlobTxMessage) Deserialize(dr *codec.DecodingReader) error {
	err := dr.Container(&tx.ChainID, &tx.Nonce, &tx.GasTipCap, &tx.GasFeeCap, &tx.Gas, &tx.To, &tx.Value, &tx.Data, &tx.AccessList, &tx.MaxFeePerDataGas, &tx.BlobVersionedHashes)
	return err
}

func (tx *BlobTxMessage) Serialize(w *codec.EncodingWriter) error {
	return w.Container(&tx.ChainID, &tx.Nonce, &tx.GasTipCap, &tx.GasFeeCap, &tx.Gas, &tx.To, &tx.Value, &tx.Data, &tx.AccessList, &tx.MaxFeePerDataGas, &tx.BlobVersionedHashes)
}

func (tx *BlobTxMessage) ByteLength() uint64 {
	return codec.ContainerLength(&tx.ChainID, &tx.Nonce, &tx.GasTipCap, &tx.GasFeeCap, &tx.Gas, &tx.To, &tx.Value, &tx.Data, &tx.AccessList, &tx.MaxFeePerDataGas, &tx.BlobVersionedHashes)
}

func (tx *BlobTxMessage) FixedLength() uint64 {
	return 0
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (tx *BlobTxMessage) copy() *BlobTxMessage {
	cpy := &BlobTxMessage{
		ChainID:             tx.ChainID,
		Nonce:               tx.Nonce,
		GasTipCap:           tx.GasTipCap,
		GasFeeCap:           tx.GasFeeCap,
		Gas:                 tx.Gas,
		To:                  AddressOptionalSSZ{Address: (*AddressSSZ)(copyAddressPtr((*libcommon.Address)(tx.To.Address)))},
		Value:               tx.Value,
		Data:                common.CopyBytes(tx.Data),
		AccessList:          make([]types2.AccessTuple, len(tx.AccessList)),
		MaxFeePerDataGas:    tx.MaxFeePerDataGas,
		BlobVersionedHashes: make([]libcommon.Hash, len(tx.BlobVersionedHashes)),
	}
	copy(cpy.AccessList, tx.AccessList)
	copy(cpy.BlobVersionedHashes, tx.BlobVersionedHashes)

	return cpy
}

type SignedBlobTx struct {
	TransactionMisc

	Message   BlobTxMessage
	Signature ECDSASignature
}

var _ Transaction = &SignedBlobTx{}

const (
	MAX_CALLDATA_SIZE              = 1 << 24
	MAX_ACCESS_LIST_SIZE           = 1 << 24
	MAX_ACCESS_LIST_STORAGE_KEYS   = 1 << 24
	MAX_VERSIONED_HASHES_LIST_SIZE = 1 << 24
)

// copy creates a deep copy of the transaction data and initializes all fields.
func (stx SignedBlobTx) copy() *SignedBlobTx {
	cpy := &SignedBlobTx{
		Message:   *stx.Message.copy(),
		Signature: stx.Signature,
	}

	return cpy
}

func (stx SignedBlobTx) Type() byte { return BlobTxType }

func (stx SignedBlobTx) GetChainID() *uint256.Int {
	chainID := uint256.Int(stx.Message.ChainID)
	return &chainID
}

func (stx SignedBlobTx) GetNonce() uint64 { return uint64(stx.Message.Nonce) }
func (stx SignedBlobTx) GetGas() uint64   { return uint64(stx.Message.Gas) }
func (stx SignedBlobTx) GetDataGas() uint64 {
	return params.DataGasPerBlob * uint64(len(stx.Message.BlobVersionedHashes))
}
func (stx SignedBlobTx) GetTo() *libcommon.Address {
	return (*libcommon.Address)(stx.Message.To.Address)
}
func (stx SignedBlobTx) GetAmount() uint256.Int { return uint256.Int(stx.Message.Value) }
func (stx SignedBlobTx) GetData() []byte        { return stx.Message.Data }
func (stx SignedBlobTx) GetValue() *uint256.Int {
	value := uint256.Int(stx.Message.Value)
	return &value
}

func (stx SignedBlobTx) GetPrice() *uint256.Int {
	tip := (uint256.Int)(stx.Message.GasTipCap)
	return &tip
}
func (stx SignedBlobTx) GetFeeCap() *uint256.Int {
	feeCap := (uint256.Int)(stx.Message.GasFeeCap)
	return &feeCap
}

func (stx *SignedBlobTx) GetTip() *uint256.Int {
	tip := (uint256.Int)(stx.Message.GasTipCap)
	return &tip
}

func (stx *SignedBlobTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return stx.GetTip()
	}
	gasFeeCap := stx.GetFeeCap()
	// return 0 because effectiveFee cant be < 0
	if gasFeeCap.Lt(baseFee) {
		return uint256.NewInt(0)
	}
	effectiveFee := new(uint256.Int).Sub(gasFeeCap, baseFee)
	if stx.GetTip().Lt(effectiveFee) {
		return stx.GetTip()
	} else {
		return effectiveFee
	}
}

func (stx *SignedBlobTx) Cost() *uint256.Int {
	total := new(uint256.Int).SetUint64(uint64(stx.Message.Gas))
	tip := uint256.Int(stx.Message.GasTipCap)
	total.Mul(total, &tip)

	value := uint256.Int(stx.Message.Value)
	total.Add(total, &value)
	return total
}

func (stx *SignedBlobTx) GetMaxFeePerDataGas() *uint256.Int {
	fee := (uint256.Int)(stx.Message.MaxFeePerDataGas)
	return &fee
}

func (stx *SignedBlobTx) GetDataHashes() []libcommon.Hash {
	return []libcommon.Hash(stx.Message.BlobVersionedHashes)
}

// TODO
func (stx *SignedBlobTx) Sender(signer Signer) (libcommon.Address, error) {
	if sc := stx.from.Load(); sc != nil {
		return sc.(libcommon.Address), nil
	}

	addr, err := signer.Sender(stx)
	if err != nil {
		return libcommon.Address{}, err
	}

	stx.from.Store(addr)
	return addr, nil
}

func (stx *SignedBlobTx) GetSender() (libcommon.Address, bool) {
	if sc := stx.from.Load(); sc != nil {
		return sc.(libcommon.Address), true
	}
	return libcommon.Address{}, false
}

func (stx *SignedBlobTx) SetSender(addr libcommon.Address) {
	stx.from.Store(addr)
}

func (stx *SignedBlobTx) IsContractDeploy() bool {
	return stx.GetTo() == nil
}

func (stx *SignedBlobTx) Unwrap() Transaction {
	return stx
}

func (stx SignedBlobTx) Protected() bool {
	return true
}

func (stx *SignedBlobTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	cpy := stx.copy()
	r, s, v, err := signer.SignatureValues(stx, sig)
	if err != nil {
		return nil, err
	}

	cpy.Signature.R = Uint256View(*r)
	cpy.Signature.V = Uint8View([4]uint64(*v)[0])
	cpy.Signature.S = Uint256View(*s)
	cpy.Message.ChainID = Uint256View(*signer.ChainID())
	return cpy, nil
}

func (tx *SignedBlobTx) FakeSign(address libcommon.Address) (Transaction, error) {
	cpy := tx.copy()
	cpy.Signature.R = Uint256View(*u256.Num1)
	cpy.Signature.S = Uint256View(*u256.Num1)
	cpy.Signature.V = Uint8View([4]uint64(*u256.Num4)[0])

	cpy.from.Store(address)
	return cpy, nil
}

func (tx *SignedBlobTx) GetAccessList() types2.AccessList {
	return types2.AccessList(tx.Message.AccessList)
}

func (stx SignedBlobTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	msg := Message{
		nonce:            stx.GetNonce(),
		gasLimit:         stx.GetGas(),
		tip:              *stx.GetTip(),
		feeCap:           *stx.GetFeeCap(),
		to:               stx.GetTo(),
		amount:           stx.GetAmount(),
		data:             stx.Message.Data,
		accessList:       stx.GetAccessList(),
		maxFeePerDataGas: *stx.GetMaxFeePerDataGas(),
		dataHashes:       stx.GetDataHashes(),
		checkNonce:       true,
	}

	if baseFee != nil {
		overflow := msg.gasPrice.SetFromBig(baseFee)
		if overflow {
			return msg, fmt.Errorf("gasPrice higher than 2^256-1")
		}
	}
	msg.gasPrice.Add(&msg.gasPrice, stx.GetTip())
	if msg.gasPrice.Gt(stx.GetFeeCap()) {
		msg.gasPrice.Set(stx.GetFeeCap())
	}

	var err error
	msg.from, err = stx.Sender(s)
	return msg, err
}

// Hash computes the hash (but not for signatures!)
func (stx *SignedBlobTx) Hash() libcommon.Hash {
	return prefixedSSZHash(BlobTxType, stx)
}

// MarshalBinary returns the canonical encoding of the transaction.
// For legacy transactions, it returns the RLP encoding. For EIP-2718 typed
// transactions, it returns the type and payload.
func (tx SignedBlobTx) MarshalBinary(w io.Writer) error {
	var b [33]byte
	// encode TxType
	b[0] = BlobTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	wcodec := codec.NewEncodingWriter(w)
	return tx.Serialize(wcodec)
}

func (stx SignedBlobTx) EncodeRLP(w io.Writer) error {
	var buf bytes.Buffer
	if err := stx.MarshalBinary(&buf); err != nil {
		return err
	}
	return rlp.Encode(w, buf.Bytes())
}

func (tx SignedBlobTx) RawSignatureValues() (v *uint256.Int, r *uint256.Int, s *uint256.Int) {
	return tx.Signature.GetV(), tx.Signature.GetR(), tx.Signature.GetS()
}

func (stx SignedBlobTx) SigningHash(chainID *big.Int) libcommon.Hash {
	// stx is a shallow copy of the tx so we can modify ChainID without mutating the input
	stx.Message.ChainID.SetFromBig(chainID)
	return prefixedSSZHash(BlobTxType, &stx.Message)
}

func (tx SignedBlobTx) EncodingSize() int {
	envelopeSize := int(codec.ContainerLength(&tx.Message, &tx.Signature))
	// Add type byte
	envelopeSize++
	return envelopeSize
}

func (stx *SignedBlobTx) Deserialize(dr *codec.DecodingReader) error {
	err := dr.Container(&stx.Message, &stx.Signature)
	return err
}

func (stx *SignedBlobTx) Serialize(w *codec.EncodingWriter) error {
	return w.Container(&stx.Message, &stx.Signature)
}

func (stx *SignedBlobTx) ByteLength() uint64 {
	return codec.ContainerLength(&stx.Message, &stx.Signature)
}

func (stx *SignedBlobTx) FixedLength() uint64 {
	return 0
}
