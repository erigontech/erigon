package types

import (
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	rlp2 "github.com/ledgerwatch/erigon-lib/rlp"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/rlp"
)

type SetCodeTransaction struct {
	DynamicFeeTransaction
	Authorizations []Authorization
}

type Authorization struct {
	ChainID uint64
	Address libcommon.Address
	Nonce   *uint256.Int
	V, R, S uint256.Int // signature values
}

func (ath *Authorization) copy() *Authorization {
	cpy := &Authorization{
		ChainID: ath.ChainID,
		Address: ath.Address,
		V:       *ath.V.Clone(),
		R:       *ath.R.Clone(),
		S:       *ath.S.Clone(),
	}

	if ath.Nonce != nil {
		cpy.Nonce = ath.Nonce.Clone()
	}

	return cpy
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

func authorizationsSize(authorizations []Authorization) int {
	size := 0
	// ChainID uint64
	// Address common.Address
	// Nonce   *uint256.Int
	// V, R, S uint256.Int // signature values
	for _, auth := range authorizations {
		size += rlp2.U64Len(auth.ChainID) // chainId
		size += 1 + length.Addr           // address

		size++
		if auth.Nonce != nil {
			size += rlp.Uint256LenExcludingHead(auth.Nonce)
		}

		size++
		size += rlp.Uint256LenExcludingHead(&auth.V)

		size++
		size += rlp.Uint256LenExcludingHead(&auth.R)

		size++
		size += rlp.Uint256LenExcludingHead(&auth.S)
	}
	return size
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

func (tx *SetCodeTransaction) FakeSign(address libcommon.Address) (Transaction, error) {
	cpy := tx.copy()
	cpy.R.Set(u256.Num1)
	cpy.S.Set(u256.Num1)
	cpy.V.Set(u256.Num4)
	cpy.from.Store(address)
	return cpy, nil
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

func decodeAuthorizations(auths *[]Authorization, s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open authorizations: %w", err)
	}
	var b []byte
	i := 0
	for _, err := s.List(); err == nil; _, err = s.List() {
		auth := Authorization{}
		if auth.ChainID, err = s.Uint(); err != nil {
			return err
		}

		if b, err = s.Bytes(); err != nil {
			return err
		}

		if len(b) != 20 {
			return fmt.Errorf("wrong size for Address: %d", len(b))
		}
		auth.Address = libcommon.BytesToAddress(b)

		if b, err = s.Uint256Bytes(); err != nil {
			return err
		}
		if len(b) > 0 {
			// TODO: check this: if nonce == nil, encode then decode really gives nil
			auth.Nonce = new(uint256.Int).SetBytes(b)
		}

		if b, err = s.Uint256Bytes(); err != nil {
			return err
		}
		auth.V.SetBytes(b)

		if b, err = s.Uint256Bytes(); err != nil {
			return err
		}
		auth.R.SetBytes(b)

		if b, err = s.Uint256Bytes(); err != nil {
			return err
		}
		auth.S.SetBytes(b)

		*auths = append(*auths, auth)
		// end of authorization
		if err = s.ListEnd(); err != nil {
			return fmt.Errorf("close Authorization: %w", err)
		}
		i++
	}

	if !errors.Is(err, rlp.EOL) {
		return fmt.Errorf("open authorizations: %d %w", i, err)
	}
	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close authorizations: %w", err)
	}
	return nil
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
	if err := rlp.EncodeAddress(tx.To, w, b); err != nil {
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

func encodeAuthorizations(authorizations []Authorization, w io.Writer, b []byte) error {
	// Authorization:
	// 	ChainID uint64
	// 	Address common.Address
	// 	Nonce   *uint256.Int
	// 	V, R, S uint256.Int
	for _, auth := range authorizations {
		// 0. encode length of individual Authorization
		authLen := rlp2.U64Len(auth.ChainID)
		authLen += (1 + length.Addr)
		authLen += 1 // nonce prefix
		if auth.Nonce != nil {
			authLen += rlp.Uint256LenExcludingHead(auth.Nonce)
		}

		authLen += (1 + rlp.Uint256LenExcludingHead(&auth.V)) + (1 + rlp.Uint256LenExcludingHead(&auth.R)) + (1 + rlp.Uint256LenExcludingHead(&auth.S))

		if err := EncodeStructSizePrefix(authLen, w, b); err != nil {
			return err
		}

		// 1. encode ChainId
		if err := rlp.EncodeInt(auth.ChainID, w, b); err != nil {
			return err
		}
		// 2. encode Address
		if err := rlp.EncodeAddress(&auth.Address, w, b); err != nil {
			return err
		}
		// 3. encode Nonce
		if err := auth.Nonce.EncodeRLP(w); err != nil {
			return err
		}
		// 4. encode V, R, S
		if err := auth.V.EncodeRLP(w); err != nil {
			return err
		}
		if err := auth.R.EncodeRLP(w); err != nil {
			return err
		}
		if err := auth.S.EncodeRLP(w); err != nil {
			return err
		}
	}

	return nil
}
