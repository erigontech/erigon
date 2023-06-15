package types

import (
	"fmt"
	"io"
	"math/big"
	"math/bits"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
)

type SignedBlobTx struct {
	TransactionMisc

	ChainID              *uint256.Int
	Nonce                uint64 // nonce of sender account
	MaxPriorityFeePerGas *uint256.Int
	MaxFeePerGas         *uint256.Int
	Gas                  uint64
	To                   *libcommon.Address `rlp:"nil"` // nil means contract creation
	Value                *uint256.Int       // wei amount
	Data                 []byte             // contract invocation input data
	AccessList           types2.AccessList

	MaxFeePerDataGas    *uint256.Int
	BlobVersionedHashes []libcommon.Hash

	YParity bool
	R       uint256.Int
	S       uint256.Int
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (stx SignedBlobTx) copy() *SignedBlobTx {
	cpy := &SignedBlobTx{
		TransactionMisc: TransactionMisc{
			time: stx.time,
		},
		ChainID:              new(uint256.Int),
		Nonce:                stx.Nonce,
		MaxPriorityFeePerGas: new(uint256.Int),
		MaxFeePerGas:         new(uint256.Int),
		Gas:                  stx.Gas,
		To:                   new(libcommon.Address),
		Value:                stx.Value,
		Data:                 common.CopyBytes(stx.Data),
		AccessList:           make([]types2.AccessTuple, len(stx.AccessList)),
		MaxFeePerDataGas:     stx.MaxFeePerDataGas,
		BlobVersionedHashes:  make([]libcommon.Hash, len(stx.BlobVersionedHashes)),
	}
	if stx.ChainID != nil {
		cpy.ChainID.Set(stx.ChainID)
	}
	if stx.MaxPriorityFeePerGas != nil {
		cpy.MaxPriorityFeePerGas.Set(stx.MaxPriorityFeePerGas)
	}
	if stx.MaxFeePerGas != nil {
		cpy.MaxFeePerGas.Set(stx.MaxFeePerGas)
	}
	copy(cpy.To[:], stx.To[:]) // TODO: make sure this is the right way
	copy(cpy.AccessList, stx.AccessList)
	copy(cpy.BlobVersionedHashes, stx.BlobVersionedHashes)
	return cpy
}

func (stx SignedBlobTx) Type() byte               { return BlobTxType }
func (stx SignedBlobTx) GetChainID() *uint256.Int { return stx.ChainID }
func (stx SignedBlobTx) GetNonce() uint64         { return stx.Nonce }
func (stx SignedBlobTx) GetPrice() *uint256.Int   { return stx.MaxPriorityFeePerGas }
func (stx SignedBlobTx) GetTip() *uint256.Int     { return stx.MaxPriorityFeePerGas }
func (stx SignedBlobTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
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
func (stx SignedBlobTx) GetFeeCap() *uint256.Int { return stx.MaxFeePerGas }
func (stx SignedBlobTx) Cost() *uint256.Int {
	// total := new(uint256.Int).SetUint64(uint64(stx.Message.Gas))
	// tip := uint256.Int(stx.Message.GasTipCap)
	// total.Mul(total, &tip)

	// value := uint256.Int(stx.Message.Value)
	// total.Add(total, &value)
	// return total

	total := new(uint256.Int).SetUint64(stx.Gas)
	total.Mul(total, stx.MaxPriorityFeePerGas)
	total.Add(total, stx.Value)
	return total
}

func (stx SignedBlobTx) GetDataHashes() []libcommon.Hash {
	return stx.BlobVersionedHashes
}
func (stx SignedBlobTx) GetGas() uint64 { return stx.Gas }
func (stx SignedBlobTx) GetDataGas() uint64 {
	return params.DataGasPerBlob * uint64(len(stx.BlobVersionedHashes))
}
func (stx SignedBlobTx) GetValue() *uint256.Int { return stx.Value }

func (stx SignedBlobTx) GetTo() *libcommon.Address { return stx.To }

func (stx SignedBlobTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	msg := Message{
		nonce:            stx.Nonce,
		gasLimit:         stx.Gas,
		gasPrice:         *stx.MaxPriorityFeePerGas,
		tip:              *stx.MaxFeePerGas,
		feeCap:           *stx.MaxPriorityFeePerGas,
		to:               stx.To,
		amount:           *stx.Value,
		data:             stx.Data,
		accessList:       stx.AccessList,
		checkNonce:       true,
		maxFeePerDataGas: *stx.MaxFeePerDataGas,
		dataHashes:       stx.BlobVersionedHashes,
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
	// msg.from, err = stx.Sender(s)
	return msg, err
}

func (stx SignedBlobTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	// TODO
	cpy := stx.copy()
	return cpy, nil
}

func (stx SignedBlobTx) FakeSign(address libcommon.Address) (Transaction, error) {
	// TODO
	cpy := stx.copy()
	return cpy, nil
}

func (stx SignedBlobTx) Hash() libcommon.Hash {
	if hash := stx.hash.Load(); hash != nil {
		return *hash.(*libcommon.Hash)
	}
	hash := prefixedRlpHash(BlobTxType, []interface{}{
		stx.ChainID,
		stx.Nonce,
		stx.MaxPriorityFeePerGas,
		stx.MaxFeePerGas,
		stx.Gas,
		stx.To,
		stx.Value,
		stx.Data,
		stx.AccessList,
		stx.MaxFeePerDataGas,
		stx.BlobVersionedHashes,
		stx.YParity,
		stx.R,
		stx.S,
	})
	stx.hash.Store(&hash)
	return hash
}

func (stx SignedBlobTx) SigningHash(chainID *big.Int) libcommon.Hash {
	return prefixedRlpHash(
		BlobTxType,
		[]interface{}{
			chainID,
			stx.Nonce,
			stx.MaxPriorityFeePerGas,
			stx.MaxFeePerGas,
			stx.Gas,
			stx.To,
			stx.Value,
			stx.Data,
			stx.AccessList,
			stx.MaxFeePerDataGas,
			stx.BlobVersionedHashes,
		})
}

func (stx SignedBlobTx) GetData() []byte                  { return stx.Data }
func (stx SignedBlobTx) GetAccessList() types2.AccessList { return stx.AccessList }
func (stx SignedBlobTx) Protected() bool                  { return true }

func (stx SignedBlobTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	// TODO
	return nil, nil, nil
}

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

func (stx SignedBlobTx) GetSender() (libcommon.Address, bool) {
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

func (stx SignedBlobTx) EncodingSize() int {
	payloadSize, _, _, _, _ := stx.payloadSize()
	envelopeSize := payloadSize
	// Add envelope size and type size
	if payloadSize >= 56 {
		envelopeSize += libcommon.BitLenToByteLen(bits.Len(uint(payloadSize)))
	}
	envelopeSize += 2
	return envelopeSize
}
func (stx SignedBlobTx) payloadSize() (payloadSize int, nonceLen, gasLen, accessListLen, blobHashesLen int) {
	// size of ChainID
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(stx.ChainID)
	// size of Nonce
	payloadSize++
	nonceLen = rlp.IntLenExcludingHead(stx.Nonce)
	payloadSize += nonceLen
	// size of MaxPriorityFeePerGas
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(stx.MaxPriorityFeePerGas)
	// size of MaxFeePerGas
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(stx.MaxFeePerGas)
	// size of Gas
	payloadSize++
	gasLen = rlp.IntLenExcludingHead(stx.Gas)
	payloadSize += gasLen
	// size of To
	payloadSize++
	if stx.To != nil {
		payloadSize += 20
	}
	// size of Value
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(stx.Value)
	// size of Data
	payloadSize++
	switch len(stx.Data) {
	case 0:
	case 1:
		if stx.Data[0] >= 128 {
			payloadSize++
		}
	default:
		if len(stx.Data) >= 56 {
			payloadSize += libcommon.BitLenToByteLen(bits.Len(uint(len(stx.Data))))
		}
		payloadSize += len(stx.Data)
	}
	// size of AccessList
	payloadSize++
	accessListLen = accessListSize(stx.AccessList)
	if accessListLen >= 56 {
		payloadSize += libcommon.BitLenToByteLen(bits.Len(uint(accessListLen)))
	}
	payloadSize += accessListLen
	// size of MaxFeePerDataGas
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(stx.MaxFeePerDataGas)
	// size of BlobVersionedHashes
	payloadSize++
	blobHashesLen = blobVersionedHashesSize(stx.BlobVersionedHashes)
	if blobHashesLen >= 56 {
		payloadSize += libcommon.BitLenToByteLen(bits.Len(uint(blobHashesLen)))
	}
	payloadSize += blobHashesLen
	// size of y_parity
	payloadSize++ // y_parity takes 1 byte?
	// size of R
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&stx.R)
	// size of S
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&stx.S)
	return payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen
}

func blobVersionedHashesSize(hashes []libcommon.Hash) int {
	return 33 * len(hashes)
}

func encodeBloblVersionedHashes(hashes []libcommon.Hash, w io.Writer, b []byte) error {
	b[0] = 128 + 32
	for _, h := range hashes {
		// if _, err := w.Write(b[:1]); err != nil {
		// 	return err
		// }
		// if _, err := w.Write(h.Bytes()); err != nil {
		// 	return err
		// }
		if err := rlp.EncodeString(h[:], w, b); err != nil {
			return err
		}
	}
	return nil
}

func (stx SignedBlobTx) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen int) error {
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}
	// encode ChainID
	if err := stx.ChainID.EncodeRLP(w); err != nil {
		return err
	}
	// encode Nonce
	if err := rlp.EncodeInt(stx.Nonce, w, b); err != nil {
		return err
	}
	// encode MaxPriorityFeePerGas
	if err := stx.MaxPriorityFeePerGas.EncodeRLP(w); err != nil {
		return err
	}
	// encode MaxFeePerGas
	if err := stx.MaxFeePerGas.EncodeRLP(w); err != nil {
		return err
	}
	// encode Gas
	if err := rlp.EncodeInt(stx.Gas, w, b); err != nil {
		return err
	}
	// encode To
	if stx.To == nil {
		b[0] = 128
	} else {
		b[0] = 128 + 20
	}
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if stx.To != nil {
		if _, err := w.Write(stx.To.Bytes()); err != nil {
			return err
		}
	}
	// encode Value
	if err := stx.Value.EncodeRLP(w); err != nil {
		return err
	}
	// encode Data
	if err := rlp.EncodeString(stx.Data, w, b); err != nil {
		return err
	}
	// prefix
	if err := EncodeStructSizePrefix(accessListLen, w, b); err != nil {
		return err
	}
	// encode AccessList
	if err := encodeAccessList(stx.AccessList, w, b); err != nil {
		return err
	}
	// encode MaxFeePerDataGas
	if err := stx.MaxFeePerDataGas.EncodeRLP(w); err != nil {
		return err
	}
	// prefix
	if err := EncodeStructSizePrefix(blobHashesLen, w, b); err != nil {
		return err
	}
	// encode BlobVersionedHashes
	if err := encodeBloblVersionedHashes(stx.BlobVersionedHashes, w, b); err != nil {
		return err
	}
	// encode y_parity
	if stx.YParity {
		b[0] = 1
	} else {
		b[0] = 0
	}
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	// encode R
	if err := stx.R.EncodeRLP(w); err != nil {
		return err
	}
	// encode S
	if err := stx.S.EncodeRLP(w); err != nil {
		return err
	}
	return nil
}

func (stx SignedBlobTx) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen := stx.payloadSize()
	envelopeSize := payloadSize
	if payloadSize >= 56 {
		envelopeSize += libcommon.BitLenToByteLen(bits.Len(uint(payloadSize)))
	}
	// size of struct prefix and TxType
	envelopeSize += 2
	var b [33]byte
	// envelope
	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}
	// encode TxType
	b[0] = BlobTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := stx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen); err != nil {
		return err
	}
	return nil
}

func (stx SignedBlobTx) MarshalBinary(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen := stx.payloadSize()
	var b [33]byte
	// encode TxType
	b[0] = BlobTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := stx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen); err != nil {
		return err
	}
	return nil
}

func (stx *SignedBlobTx) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	var b []byte
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.ChainID = new(uint256.Int).SetBytes(b)

	if stx.Nonce, err = s.Uint(); err != nil {
		return err
	}

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.MaxPriorityFeePerGas = new(uint256.Int).SetBytes(b)

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.MaxFeePerGas = new(uint256.Int).SetBytes(b)

	if stx.Gas, err = s.Uint(); err != nil {
		return err
	}

	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 0 && len(b) != 20 {
		return fmt.Errorf("wrong size for To: %d", len(b))
	}
	if len(b) > 0 {
		stx.To = &libcommon.Address{}
		copy((*stx.To)[:], b)
	}

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.Value = new(uint256.Int).SetBytes(b)

	if stx.Data, err = s.Bytes(); err != nil {
		return err
	}
	// decode AccessList
	stx.AccessList = types2.AccessList{}
	if err = decodeAccessList(&stx.AccessList, s); err != nil {
		return err
	}

	// decode MaxFeePerDataGas
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.MaxFeePerDataGas = new(uint256.Int).SetBytes(b)

	// decode BlobVersionedHashes
	stx.BlobVersionedHashes = []libcommon.Hash{}
	if err = decodeBlobVersionedHashes(stx.BlobVersionedHashes, s); err != nil {
		return err
	}

	// decode y_parity
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 1 {
		return fmt.Errorf("wrong size for y_parity: %d", len(b))
	}
	if b[0] == 1 {
		stx.YParity = true
	} else if b[0] == 0 {
		stx.YParity = false
	} else {
		return fmt.Errorf("wrong value for y_parity: %d", b)
	}

	// decode R
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.R.SetBytes(b)

	// decode S
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.S.SetBytes(b)
	return s.ListEnd()
}

func decodeBlobVersionedHashes(hashes []libcommon.Hash, s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open BlobVersionedHashes: %w", err)
	}
	var b []byte
	_hash := libcommon.Hash{}

	for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
		if len(b) == 32 {
			copy((_hash)[:], b)
			hashes = append(hashes, _hash)
		} else {
			return fmt.Errorf("wrong size for blobVersionedHashes: %d, %v", len(b), b[0])
		}
	}

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close BlobVersionedHashes: %w", err)
	}

	return nil
}
