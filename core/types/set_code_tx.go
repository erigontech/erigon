package types

import (
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	rlp2 "github.com/ledgerwatch/erigon-lib/rlp"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/rlp"
)

type SetCodeTransaction struct {
	DynamicFeeTransaction
	Authorizations []Authorization
}

func (tx *SetCodeTransaction) Unwrap() Transaction {
	return tx
}

func (tx *SetCodeTransaction) Type() byte {
	return SetCodeTxType
}

func (tx *SetCodeTransaction) GetBlobHashes() []libcommon.Hash {
	return []libcommon.Hash{}
}

func (tx *SetCodeTransaction) GetAuthorizations() []Authorization {
	return tx.Authorizations
}

func (tx *SetCodeTransaction) copy() *SetCodeTransaction {
	cpy := &SetCodeTransaction{}
	cpy.DynamicFeeTransaction = *tx.DynamicFeeTransaction.copy()

	cpy.Authorizations = make([]Authorization, len(tx.Authorizations))

	for i, ath := range tx.Authorizations {
		cpy.Authorizations[i] = *ath.copy()
	}

	return cpy
}

func (tx *SetCodeTransaction) EncodingSize() int {
	payloadSize, _, _, _, _ := tx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp2.ListPrefixLen(payloadSize) + payloadSize
}

func (tx *SetCodeTransaction) payloadSize() (payloadSize, nonceLen, gasLen, accessListLen, authorizationsLen int) {
	payloadSize, nonceLen, gasLen, accessListLen = tx.DynamicFeeTransaction.payloadSize()
	// size of Authorizations
	authorizationsLen = authorizationsSize(tx.Authorizations)
	payloadSize += rlp2.ListPrefixLen(authorizationsLen) + authorizationsLen

	return
}

func (tx *SetCodeTransaction) WithSignature(signer Signer, sig []byte) (Transaction, error) {
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

func (tx *SetCodeTransaction) FakeSign(address libcommon.Address) Transaction {
	cpy := tx.copy()
	cpy.R.Set(u256.Num1)
	cpy.S.Set(u256.Num1)
	cpy.V.Set(u256.Num4)
	cpy.from.Store(address)
	return cpy
}

func (tx *SetCodeTransaction) MarshalBinary(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen, authorizationsLen := tx.payloadSize()
	var b [33]byte
	// encode TxType
	b[0] = SetCodeTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen, authorizationsLen); err != nil {
		return err
	}
	return nil
}

func (tx *SetCodeTransaction) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	msg, err := tx.DynamicFeeTransaction.AsMessage(s, baseFee, rules)
	if err != nil {
		return msg, err
	}
	msg.authorizations = tx.Authorizations
	if !rules.IsPrague {
		return msg, errors.New("SetCodeTransaction is only supported in Prague")
	}

	return msg, nil
}

func (tx *SetCodeTransaction) Hash() libcommon.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash.(*libcommon.Hash)
	}
	hash := prefixedRlpHash(SetCodeTxType, []interface{}{
		tx.ChainID,
		tx.Nonce,
		tx.Tip,
		tx.FeeCap,
		tx.Gas,
		tx.To,
		tx.Value,
		tx.Data,
		tx.AccessList,
		tx.Authorizations,
		tx.V, tx.R, tx.S,
	})
	tx.hash.Store(&hash)
	return hash
}

func (tx *SetCodeTransaction) SigningHash(chainID *big.Int) libcommon.Hash {
	return prefixedRlpHash(
		SetCodeTxType,
		[]interface{}{
			chainID,
			tx.Nonce,
			tx.Tip,
			tx.FeeCap,
			tx.Gas,
			tx.To,
			tx.Value,
			tx.Data,
			tx.AccessList,
			tx.Authorizations,
		})
}

func (tx *SetCodeTransaction) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen, authorizationsLen := tx.payloadSize()
	envelopSize := 1 + rlp2.ListPrefixLen(payloadSize) + payloadSize
	var b [33]byte
	// encode envelope size
	if err := rlp.EncodeStringSizePrefix(envelopSize, w, b[:]); err != nil {
		return err
	}
	// encode TxType
	b[0] = SetCodeTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}

	return tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen, authorizationsLen)
}

func (tx *SetCodeTransaction) DecodeRLP(s *rlp.Stream) error {
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
		tx.To = &libcommon.Address{}
		copy((*tx.To)[:], b)
	}
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.Value = new(uint256.Int).SetBytes(b)
	if tx.Data, err = s.Bytes(); err != nil {
		return err
	}
	// decode AccessList
	tx.AccessList = types2.AccessList{}
	if err = decodeAccessList(&tx.AccessList, s); err != nil {
		return err
	}

	// decode authorizations
	tx.Authorizations = make([]Authorization, 0)
	if err = decodeAuthorizations(&tx.Authorizations, s); err != nil {
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

func (tx *SetCodeTransaction) encodePayload(w io.Writer, b []byte, payloadSize, _, _, accessListLen, authorizationsLen int) error {
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
	if err := rlp.EncodeOptionalAddress(tx.To, w, b); err != nil {
		return err
	}
	// encode Value
	if err := tx.Value.EncodeRLP(w); err != nil {
		return err
	}
	// encode Data
	if err := rlp.EncodeString(tx.Data, w, b); err != nil {
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
	// prefix
	if err := EncodeStructSizePrefix(authorizationsLen, w, b); err != nil {
		return err
	}
	// encode Authorizations
	if err := encodeAuthorizations(tx.Authorizations, w, b); err != nil {
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
