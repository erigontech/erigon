package types

import (
	"fmt"
	"io"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/holiman/uint256"
)

type TxRLP struct { // fields are written in rlp encoding order

	ChainID *uint256.Int // 1, 2, 3, 4 tx types

	Nonce uint64 // all tx types

	GasPrice *uint256.Int `rlp:"nil"` // 0, 1

	Tip    *uint256.Int `rlp:"nil"` // 2, 3, 4
	FeeCap *uint256.Int `rlp:"nil"` // 2, 3, 4

	Gas   uint64             // all
	To    *libcommon.Address `rlp:"nil"` // all
	Value *uint256.Int       // all
	Data  []byte             // all

	AccessList AccessList // 1, 2, 3, 4

	MaxFeePerBlobGas    *uint256.Int     // 3
	BlobVersionedHashes []libcommon.Hash // 3

	Authorizations []Authorization // 4

	V, R, S uint256.Int // all

	typ byte
}

func TxnToTxRLP(txn Transaction) TxRLP {
	v, r, s := txn.RawSignatureValues()
	base := TxRLP{
		Nonce: txn.GetNonce(),
		Gas:   txn.GetGas(),
		To:    txn.GetTo(),
		Value: txn.GetValue(),
		Data:  txn.GetData(),
		V:     *v,
		R:     *r,
		S:     *s,
	}

	base.typ = txn.Type()

	if txn.Type() > LegacyTxType { // 1, 2, 3, 4 types
		base.ChainID = txn.GetChainID()
		base.AccessList = txn.GetAccessList()
	}

	if txn.Type() < DynamicFeeTxType {
		base.GasPrice = txn.GetPrice()
	}

	if txn.Type() >= DynamicFeeTxType {
		base.Tip = txn.GetTip()
		base.FeeCap = txn.GetFeeCap()
	}

	if txn.Type() == BlobTxType {
		base.MaxFeePerBlobGas = txn.(*BlobTx).MaxFeePerBlobGas
		base.BlobVersionedHashes = txn.(*BlobTx).BlobVersionedHashes
	}

	if txn.Type() == SetCodeTxType {
		base.Authorizations = txn.(*SetCodeTransaction).Authorizations
	}

	return base
}

func (tx *TxRLP) EncodingSize() (size, accessListLen, blobHashesLen, authorizationsLen int) {
	fmt.Println("tx.typ: ", tx.typ)
	if tx.typ > LegacyTxType {
		size += rlp.Uint256LenExcludingHead(tx.ChainID) + 1
	}
	size += rlp.IntLenExcludingHead(tx.Nonce) + 1

	if tx.typ < DynamicFeeTxType {
		size += rlp.Uint256LenExcludingHead(tx.GasPrice) + 1
	} else {
		size += rlp.Uint256LenExcludingHead(tx.Tip) + 1
		size += rlp.Uint256LenExcludingHead(tx.FeeCap) + 1
	}

	size += rlp.IntLenExcludingHead(tx.Gas) + 1
	size++
	if tx.To != nil {
		size += 20
	}

	size += rlp.Uint256LenExcludingHead(tx.Value) + 1
	size += rlp.StringLen(tx.Data)

	if tx.typ > LegacyTxType {
		accessListLen = accessListSize(tx.AccessList)
		size += rlp.ListPrefixLen(accessListLen) + accessListLen
	}

	if tx.typ == BlobTxType {
		size += rlp.Uint256LenExcludingHead(tx.MaxFeePerBlobGas) + 1
		blobHashesLen = blobVersionedHashesSize(tx.BlobVersionedHashes)
		size += rlp.ListPrefixLen(blobHashesLen) + blobHashesLen
	}

	if tx.typ == SetCodeTxType {
		authorizationsLen = authorizationsSize(tx.Authorizations)
		size += rlp.ListPrefixLen(authorizationsLen) + authorizationsLen
	}

	size += rlp.Uint256LenExcludingHead(&tx.V) +
		rlp.Uint256LenExcludingHead(&tx.R) +
		rlp.Uint256LenExcludingHead(&tx.S) + 3

	return
}

func (tx *TxRLP) EncodeRLP(w io.Writer) error {
	var b [32]byte
	encodingSize, accessListLen, blobHashesLen, authorizationsLen := tx.EncodingSize()
	fmt.Println("encodingSize: ", encodingSize)
	if tx.typ > LegacyTxType {
		envelopeSize := 1 + rlp.ListPrefixLen(encodingSize) + encodingSize
		if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
			return err
		}
		b[0] = tx.typ
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	}

	if err := rlp.EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	if tx.typ > LegacyTxType {
		if err := rlp.EncodeUint256(tx.ChainID, w, b[:]); err != nil {
			return err
		}
	}
	if err := rlp.EncodeInt(tx.Nonce, w, b[:]); err != nil {
		return err
	}
	if tx.typ < DynamicFeeTxType {
		if err := rlp.EncodeUint256(tx.GasPrice, w, b[:]); err != nil {
			return err
		}
	} else {
		if err := rlp.EncodeUint256(tx.Tip, w, b[:]); err != nil {
			return err
		}
		if err := rlp.EncodeUint256(tx.FeeCap, w, b[:]); err != nil {
			return err
		}
	}
	if err := rlp.EncodeInt(tx.Gas, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeOptionalAddress(tx.To, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(tx.Value, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeString(tx.Data, w, b[:]); err != nil {
		return err
	}

	if tx.typ > LegacyTxType {
		if err := rlp.EncodeStructSizePrefix(accessListLen, w, b[:]); err != nil {
			return err
		}
		if err := encodeAccessList(tx.AccessList, w, b[:]); err != nil {
			return err
		}
	}

	if tx.typ == BlobTxType {
		if err := rlp.EncodeUint256(tx.MaxFeePerBlobGas, w, b[:]); err != nil {
			return err
		}
		if err := rlp.EncodeStructSizePrefix(blobHashesLen, w, b[:]); err != nil {
			return err
		}
		if err := rlp.EncodeHashSlice(tx.BlobVersionedHashes, w, b[:]); err != nil {
			return err
		}
	}

	if tx.typ == SetCodeTxType {
		if err := rlp.EncodeStructSizePrefix(authorizationsLen, w, b[:]); err != nil {
			return err
		}
		if err := encodeAuthorizations(tx.Authorizations, w, b[:]); err != nil {
			return err
		}
	}

	if err := rlp.EncodeUint256(&tx.V, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(&tx.R, w, b[:]); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(&tx.S, w, b[:]); err != nil {
		return err
	}

	return nil
}
