package types

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/bits"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
)

type StarknetTransaction struct {
	CommonTx

	Salt       []byte // cairo contract address salt
	Tip        *uint256.Int
	FeeCap     *uint256.Int
	AccessList AccessList
}

func (tx StarknetTransaction) Type() byte {
	return StarknetType
}

func (tx StarknetTransaction) GetSalt() []byte {
	return tx.Salt
}

func (tx StarknetTransaction) IsStarkNet() bool {
	return true
}

func (tx *StarknetTransaction) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	var b []byte
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.ChainID = new(uint256.Int).SetBytes(b)
	if tx.Nonce, err = s.Uint(); err != nil {
		return err
	}
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.Tip = new(uint256.Int).SetBytes(b)
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.FeeCap = new(uint256.Int).SetBytes(b)
	if tx.Gas, err = s.Uint(); err != nil {
		return err
	}
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 0 && len(b) != 20 {
		return fmt.Errorf("wrong size for To: %d", len(b))
	}
	if len(b) > 0 {
		tx.To = &common.Address{}
		copy((*tx.To)[:], b)
	}
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.Value = new(uint256.Int).SetBytes(b)
	if tx.Data, err = s.Bytes(); err != nil {
		return err
	}
	if tx.Salt, err = s.Bytes(); err != nil {
		return err
	}
	// decode AccessList
	tx.AccessList = AccessList{}
	if err = decodeAccessList(&tx.AccessList, s); err != nil {
		return err
	}
	// decode V
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.V.SetBytes(b)
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.R.SetBytes(b)
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.S.SetBytes(b)
	return s.ListEnd()
}

func (tx StarknetTransaction) GetPrice() *uint256.Int {
	panic("implement me")
}

func (tx StarknetTransaction) GetTip() *uint256.Int {
	return tx.Tip
}

func (tx StarknetTransaction) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	panic("implement me")
}

func (tx StarknetTransaction) GetFeeCap() *uint256.Int {
	return tx.FeeCap
}

func (tx StarknetTransaction) Cost() *uint256.Int {
	panic("implement me")
}

func (tx StarknetTransaction) AsMessage(s Signer, baseFee *big.Int, rules *params.Rules) (Message, error) {
	msg := Message{
		nonce:      tx.Nonce,
		gasLimit:   tx.Gas,
		tip:        *tx.Tip,
		feeCap:     *tx.FeeCap,
		to:         tx.To,
		amount:     *tx.Value,
		data:       tx.Data,
		accessList: tx.AccessList,
		checkNonce: true,
	}

	if !rules.IsStarknet {
		return msg, errors.New("starknet tx not supported")
	}

	if baseFee != nil {
		overflow := msg.gasPrice.SetFromBig(baseFee)
		if overflow {
			return msg, fmt.Errorf("gasPrice higher than 2^256-1")
		}
	}
	msg.gasPrice.Add(&msg.gasPrice, tx.Tip)
	if msg.gasPrice.Gt(tx.FeeCap) {
		msg.gasPrice.Set(tx.FeeCap)
	}

	var err error
	msg.from, err = tx.Sender(s)
	return msg, err
}

func (tx *StarknetTransaction) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	cpy := tx.copy()
	r, s, v, err := signer.SignatureValues(tx, sig)
	if err != nil {
		return nil, err
	}
	cpy.R.Set(r)
	cpy.S.Set(s)
	cpy.V.Set(v)
	cpy.ChainID = signer.ChainID()
	return cpy, nil
}

func (tx StarknetTransaction) FakeSign(address common.Address) (Transaction, error) {
	panic("implement me")
}

func (tx StarknetTransaction) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash.(*common.Hash)
	}
	hash := prefixedRlpHash(DynamicFeeTxType, []interface{}{
		tx.ChainID,
		tx.Nonce,
		tx.Tip,
		tx.FeeCap,
		tx.Gas,
		tx.To,
		tx.Value,
		tx.Data,
		tx.AccessList,
		tx.V, tx.R, tx.S,
	})
	tx.hash.Store(&hash)
	return hash
}

func (tx StarknetTransaction) SigningHash(chainID *big.Int) common.Hash {
	return prefixedRlpHash(
		StarknetType,
		[]interface{}{
			chainID,
			tx.Nonce,
			tx.Tip,
			tx.FeeCap,
			tx.Gas,
			tx.To,
			tx.Value,
			tx.Data,
			tx.Salt,
			tx.AccessList,
		})
}

func (tx StarknetTransaction) GetAccessList() AccessList {
	panic("implement me")
}

func (tx StarknetTransaction) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return &tx.V, &tx.R, &tx.S
}

func (tx StarknetTransaction) MarshalBinary(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen := tx.payloadSize()
	var b [33]byte
	b[0] = StarknetType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen); err != nil {
		return err
	}
	return nil
}

func (tx *StarknetTransaction) Sender(signer Signer) (common.Address, error) {
	if sc := tx.from.Load(); sc != nil {
		return sc.(common.Address), nil
	}
	addr, err := signer.Sender(tx)
	if err != nil {
		return common.Address{}, err
	}
	tx.from.Store(addr)
	return addr, nil
}

func (tx StarknetTransaction) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen := tx.payloadSize()
	envelopeSize := payloadSize
	if payloadSize >= 56 {
		envelopeSize += (bits.Len(uint(payloadSize)) + 7) / 8
	}
	// size of struct prefix and TxType
	envelopeSize += 2
	var b [33]byte
	// envelope
	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}
	// encode TxType
	b[0] = StarknetType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen); err != nil {
		return err
	}
	return nil
}

func (tx StarknetTransaction) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen, accessListLen int) error {
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}
	// encode ChainID
	if err := tx.ChainID.EncodeRLP(w); err != nil {
		return err
	}
	// encode Nonce
	if err := rlp.EncodeInt(tx.Nonce, w, b); err != nil {
		return err
	}
	// encode MaxPriorityFeePerGas
	if err := tx.Tip.EncodeRLP(w); err != nil {
		return err
	}
	// encode MaxFeePerGas
	if err := tx.FeeCap.EncodeRLP(w); err != nil {
		return err
	}
	// encode Gas
	if err := rlp.EncodeInt(tx.Gas, w, b); err != nil {
		return err
	}
	// encode To
	if tx.To == nil {
		b[0] = 128
	} else {
		b[0] = 128 + 20
	}
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if tx.To != nil {
		if _, err := w.Write(tx.To.Bytes()); err != nil {
			return err
		}
	}
	// encode Value
	if err := tx.Value.EncodeRLP(w); err != nil {
		return err
	}
	// encode Data
	if err := rlp.EncodeString(tx.Data, w, b); err != nil {
		return err
	}
	// encode cairo contract address salt
	if err := rlp.EncodeString(tx.Salt, w, b); err != nil {
		return err
	}
	// prefix
	if err := EncodeStructSizePrefix(accessListLen, w, b); err != nil {
		return err
	}
	// encode AccessList
	if err := encodeAccessList(tx.AccessList, w, b); err != nil {
		return err
	}
	// encode V
	if err := tx.V.EncodeRLP(w); err != nil {
		return err
	}
	// encode R
	if err := tx.R.EncodeRLP(w); err != nil {
		return err
	}
	// encode S
	if err := tx.S.EncodeRLP(w); err != nil {
		return err
	}
	return nil
}

func (tx StarknetTransaction) payloadSize() (payloadSize int, nonceLen, gasLen, accessListLen int) {
	// size of ChainID
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.ChainID)
	// size of Nonce
	payloadSize++
	nonceLen = rlp.IntLenExcludingHead(tx.Nonce)
	payloadSize += nonceLen
	// size of MaxPriorityFeePerGas
	payloadSize++
	var tipLen int
	if tx.Tip.BitLen() >= 8 {
		tipLen = (tx.Tip.BitLen() + 7) / 8
	}
	payloadSize += tipLen
	// size of MaxFeePerGas
	payloadSize++
	var feeCapLen int
	if tx.FeeCap.BitLen() >= 8 {
		feeCapLen = (tx.FeeCap.BitLen() + 7) / 8
	}
	payloadSize += feeCapLen
	// size of Gas
	payloadSize++
	gasLen = rlp.IntLenExcludingHead(tx.Gas)
	payloadSize += gasLen
	// size of To
	payloadSize++
	if tx.To != nil {
		payloadSize += 20
	}
	// size of Value
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.Value)

	// size of Data
	payloadSize++
	switch len(tx.Data) {
	case 0:
	case 1:
		if tx.Data[0] >= 128 {
			payloadSize++
		}
	default:
		if len(tx.Data) >= 56 {
			payloadSize += (bits.Len(uint(len(tx.Data))) + 7) / 8
		}
		payloadSize += len(tx.Data)
	}

	// size of cairo contract address salt
	payloadSize++
	switch len(tx.Salt) {
	case 0:
	case 1:
		if tx.Salt[0] >= 128 {
			payloadSize++
		}
	default:
		if len(tx.Salt) >= 56 {
			payloadSize += (bits.Len(uint(len(tx.Salt))) + 7) / 8
		}
		payloadSize += len(tx.Salt)
	}

	// size of AccessList
	payloadSize++
	accessListLen = accessListSize(tx.AccessList)
	if accessListLen >= 56 {
		payloadSize += (bits.Len(uint(accessListLen)) + 7) / 8
	}
	payloadSize += accessListLen
	// size of V
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.V)
	// size of R
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.R)
	// size of S
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.S)
	return payloadSize, nonceLen, gasLen, accessListLen
}

func (tx StarknetTransaction) copy() *StarknetTransaction {
	cpy := &StarknetTransaction{
		CommonTx: CommonTx{
			TransactionMisc: TransactionMisc{
				time: tx.time,
			},
			ChainID: new(uint256.Int),
			Nonce:   tx.Nonce,
			To:      tx.To,
			Data:    common.CopyBytes(tx.Data),
			Gas:     tx.Gas,
			Value:   new(uint256.Int),
		},
		Salt:       common.CopyBytes(tx.Salt),
		AccessList: make(AccessList, len(tx.AccessList)),
		Tip:        new(uint256.Int),
		FeeCap:     new(uint256.Int),
	}
	copy(cpy.AccessList, tx.AccessList)
	if tx.Value != nil {
		cpy.Value.Set(tx.Value)
	}
	if tx.ChainID != nil {
		cpy.ChainID.Set(tx.ChainID)
	}
	if tx.Tip != nil {
		cpy.Tip.Set(tx.Tip)
	}
	if tx.FeeCap != nil {
		cpy.FeeCap.Set(tx.FeeCap)
	}
	cpy.V.Set(&tx.V)
	cpy.R.Set(&tx.R)
	cpy.S.Set(&tx.S)
	return cpy
}

func (tx StarknetTransaction) EncodingSize() int {
	payloadSize, _, _, _ := tx.payloadSize()
	envelopeSize := payloadSize
	// Add envelope size and type size
	if payloadSize >= 56 {
		envelopeSize += (bits.Len(uint(payloadSize)) + 7) / 8
	}
	envelopeSize += 2
	return envelopeSize
}
